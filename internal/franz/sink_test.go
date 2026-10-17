package franz

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/commitmeta"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type producerMock struct {
	ProducerClient
	records   []*kgo.Record
	contexts  []context.Context
	callbacks []func(*kgo.Record, error)
	closed    bool
}

type delayedUnbufferingProducer struct {
	ProducerClient
	maxRecords, maxBytes int64
	records, bytes       atomic.Int64
	promises             []func(*kgo.Record, error)
}

func (p *delayedUnbufferingProducer) TryProduce(_ context.Context, r *kgo.Record, callback func(*kgo.Record, error)) {
	n := int64(len(r.Key) + len(r.Value))
	if p.records.Load() >= p.maxRecords || p.bytes.Load()+n > p.maxBytes {
		callback(r, kgo.ErrMaxBuffered)
		return
	}
	p.records.Add(1)
	p.bytes.Add(n)
	p.promises = append(p.promises, callback)
}
func TestProducerHeadroomCoversPromiseBeforeClientUnbuffering(t *testing.T) {
	// Given the pinned client's ordering: callback first, internal buffer accounting second.
	s, _ := sinkFixture(t)
	s.settings.queueBytes, s.settings.messageBytes = 600, 1000
	s.settings.request = 30 * time.Second
	s.settings.compression = "none"
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	optionsClient, err := kgo.NewClient(append(s.settings.producerOptions(), kgo.WithContext(ctx))...)
	if err != nil {
		t.Fatal(err)
	}
	defer optionsClient.Close()
	p := &delayedUnbufferingProducer{maxRecords: optionsClient.OptValue(kgo.MaxBufferedRecords).(int64), maxBytes: optionsClient.OptValue(kgo.MaxBufferedBytes).(int64)}
	s.client = p
	r := core.Record{Value: make([]byte, 512)}
	if err := s.Submit(r, core.Token{}); err != nil {
		t.Fatal(err)
	}
	callback := p.promises[0]
	promiseReturned, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		callback(nil, nil)
		close(promiseReturned)
		<-release
		p.records.Add(-1)
		p.bytes.Add(-512)
		close(done)
	}()
	<-promiseReturned
	// When the core submits in that transition window, then reserved headroom prevents ErrMaxBuffered.
	if err := s.Submit(r, core.Token{}); err != nil {
		close(release)
		t.Fatal(err)
	}
	close(release)
	<-done
	if len(p.promises) != 2 {
		t.Fatal("client rejected correctly admitted record")
	}
	p.promises[1](nil, nil)
	for range 2 {
		if report := <-s.Deliveries(); report.Err != nil {
			t.Fatal("buffer race failed a write", report.Err)
		}
	}
}

func (p *producerMock) TryProduce(ctx context.Context, r *kgo.Record, callback func(*kgo.Record, error)) {
	p.contexts = append(p.contexts, ctx)
	p.records = append(p.records, r)
	p.callbacks = append(p.callbacks, callback)
}
func (p *producerMock) Flush(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }
func (p *producerMock) Close() {
	p.closed = true
	for n, ctx := range p.contexts {
		select {
		case <-ctx.Done():
			p.callbacks[n](p.records[n], ctx.Err())
		default:
		}
	}
}
func sinkFixture(t *testing.T) (*Sink, *producerMock) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	client := &producerMock{}
	s := &Sink{client: client, topic: "output", settings: settings{queueRecords: 1, queueBytes: 1000000, messageBytes: 1000000, delivery: 40 * time.Millisecond}, clock: core.RealClock{}, log: quietLogger(), reports: make(chan core.Delivery, 2), ctx: ctx, cancel: cancel, lastMetadata: time.Now()}
	return s, client
}
func TestSinkGatesWorkAndUsesSubmissionTimeForDelivery(t *testing.T) {
	// Given a one-record producer and an old timestamp with raw nil/empty/header distinctions.
	s, client := sinkFixture(t)
	stamp := int64(0)
	r := core.Record{Key: []byte{}, Value: nil, TimestampMS: &stamp, Headers: []core.Header{{Name: "a\x00b", Value: nil}, {Name: string([]byte{255}), Value: []byte{}}}}
	token := core.Token{Partition: 2}
	before := time.Now()
	if err := s.Submit(r, token); err != nil {
		t.Fatal(err)
	}
	after := time.Now()
	// When another record arrives before the first delivery, then it returns Busy without calling the client.
	var busy *core.Busy
	if err := s.Submit(r, token); !errors.As(err, &busy) || len(client.records) != 1 {
		t.Fatal("full producer admitted work", err)
	}
	deadline, ok := client.contexts[0].Deadline()
	if !ok || deadline.Before(before.Add(s.settings.delivery)) || deadline.After(after.Add(s.settings.delivery)) {
		t.Fatal("delivery deadline used the record's timestamp", deadline)
	}
	got := client.records[0]
	if got.Key == nil || got.Value != nil || got.Timestamp.UnixMilli() != 0 || !reflect.DeepEqual(got.Headers, []kgo.RecordHeader{{Key: "a\x00b", Value: nil}, {Key: string([]byte{255}), Value: []byte{}}}) {
		t.Fatal("producer changed logical record", got)
	}
	// When delivery fails, then it settles that token without a global Fatal report or retained capacity.
	failure := context.DeadlineExceeded
	client.callbacks[0](got, failure)
	report := <-s.Deliveries()
	if report.Token != token || !errors.Is(report.Err, failure) || report.Fatal || s.records.Load() != 0 || s.bytes.Load() != 0 {
		t.Fatal("write failure did not settle its work", report)
	}
	if err := s.Submit(r, token); err != nil {
		t.Fatal("completed work retained producer capacity", err)
	}
}
func TestSinkRejectsOnlyProvenUnsentRecords(t *testing.T) {
	// Given a producer with a small size bound and known timestamp acceptance limits.
	s, client := sinkFixture(t)
	s.settings.messageBytes = 1000
	if err := s.Submit(core.Record{Value: make([]byte, 1001)}, core.Token{}); err == nil || len(client.records) != 0 {
		t.Fatal("oversized record sent", err)
	}
	s.configuration = map[string]string{"message.timestamp.type": "CreateTime", "message.timestamp.before.max.ms": "1000"}
	stamp := time.Now().Add(-time.Hour).UnixMilli()
	// When a timestamp is proven too old locally, then the record error permits the explicit SKIP policy.
	err := s.Submit(core.Record{TimestampMS: &stamp}, core.Token{})
	var recordError *core.RecordError
	if !errors.As(err, &recordError) || recordError.Class != "rejected_timestamp" || len(client.records) != 0 {
		t.Fatal("unsafe local timestamp classification", err)
	}
}
func TestSinkCloseDoesNotWaitForUndrainedDeliveryReports(t *testing.T) {
	// Given a pending write, a coalesced flush, and a delivery channel the core has not read.
	s, client := sinkFixture(t)
	if err := s.Submit(core.Record{}, core.Token{}); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	// When closing, then cancellation releases the flush and callback without needing the core to drain reports.
	done := make(chan struct{})
	go func() { s.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("producer close waited for reports")
	}
	if !client.closed {
		t.Fatal("producer not closed")
	}
}

type sinkClock struct{ now time.Time }

func (c *sinkClock) Now() time.Time        { return c.now }
func (c *sinkClock) Sleep(d time.Duration) { c.now = c.now.Add(d) }

func TestSinkRefreshesRoutingOnIntervalWithoutBlockingOnMetadataFailure(t *testing.T) {
	// Given a destination whose partition count changes and whose metadata can become unavailable.
	s, producer := sinkFixture(t)
	start := time.Unix(1000000, 0)
	clock := &sinkClock{now: start}
	s.clock, s.lastMetadata, s.partitionCount = clock, start, 1
	var logs bytes.Buffer
	s.log = slog.New(slog.NewJSONHandler(&logs, nil))
	count, calls := 1, 0
	var failure error
	client := &metadataMock{request: func(_ context.Context, r kmsg.Request) (kmsg.Response, error) {
		if *r.(*kmsg.MetadataRequest).Topics[0].Topic != "output" {
			t.Fatal("refreshed the wrong topic")
		}
		calls++
		if failure != nil {
			return nil, failure
		}
		name, cluster := "output", "cluster"
		partitions := make([]kmsg.MetadataResponseTopicPartition, count)
		for i := range partitions {
			partitions[i].Partition = int32(i)
		}
		return &kmsg.MetadataResponse{ClusterID: &cluster, Topics: []kmsg.MetadataResponseTopic{{Topic: &name, TopicID: [16]byte{1}, Partitions: partitions}}}, nil
	}}
	s.inspector = &Inspector{codec: commitmeta.NewCodec(), client: client, topic: "output", log: s.log}
	for _, step := range []struct {
		seconds, partitions, calls, remembered int
		failure                                error
	}{
		{0, 1, 0, 1, nil}, {29, 1, 0, 1, nil}, {30, 1, 1, 1, nil},
		{60, 3, 2, 3, nil}, {61, 3, 2, 3, nil},
		{90, 3, 3, 3, errors.New("metadata unavailable")}, {91, 3, 3, 3, nil}, {120, 3, 4, 3, nil},
	} {
		clock.now = start.Add(time.Duration(step.seconds) * time.Second)
		count, failure = step.partitions, step.failure
		// When records arrive before and after each refresh interval, including an outage.
		if err := s.Submit(core.Record{}, core.Token{}); err != nil {
			t.Fatal("optional metadata check prevented publishing", err)
		}
		n := len(producer.records) - 1
		producer.callbacks[n](producer.records[n], nil)
		// Then refreshes stay throttled and remember only successfully read partition counts.
		if calls != step.calls || s.partitionCount != step.remembered {
			t.Fatal("incorrect routing refresh or delivery", step.seconds, calls, s.partitionCount)
		}
		select {
		case report := <-s.Deliveries():
			if report.Err != nil {
				t.Fatal("delivery failed", report.Err)
			}
		default:
			t.Fatal("delivery outcome missing")
		}
	}
	if strings.Count(logs.String(), "destination partition count changed;") != 1 || strings.Count(logs.String(), "destination partition count check failed") != 1 {
		t.Fatal("routing change or metadata failure was not logged once", logs.String())
	}
}
