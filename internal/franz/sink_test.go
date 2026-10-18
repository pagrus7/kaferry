package franz

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/commitmeta"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, err)
	defer optionsClient.Close()
	p := &delayedUnbufferingProducer{maxRecords: optionsClient.OptValue(kgo.MaxBufferedRecords).(int64), maxBytes: optionsClient.OptValue(kgo.MaxBufferedBytes).(int64)}
	s.client = p
	r := core.Record{Value: make([]byte, 512)}
	require.NoError(t, s.Submit(r, core.Token{}))
	callback := p.promises[0]
	promiseReturned, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var released sync.Once
	releasePromise := func() { released.Do(func() { close(release) }) }
	t.Cleanup(releasePromise)
	go func() {
		callback(nil, nil)
		close(promiseReturned)
		<-release
		p.records.Add(-1)
		p.bytes.Add(-512)
		close(done)
	}()
	await(t, promiseReturned, "producer promise")
	// When the core submits in that transition window, then reserved headroom prevents ErrMaxBuffered.
	require.NoError(t, s.Submit(r, core.Token{}))
	releasePromise()
	await(t, done, "producer unbuffering")
	require.Len(t, p.promises, 2, "client rejected correctly admitted record")
	p.promises[1](nil, nil)
	for range 2 {
		if report := receiveDelivery(t, s); report.Err != nil {
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
	require.NoError(t, s.Submit(r, token))
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
	report := receiveDelivery(t, s)
	if report.Token != token || !errors.Is(report.Err, failure) || report.Fatal || s.records.Load() != 0 || s.bytes.Load() != 0 {
		t.Fatal("write failure did not settle its work", report)
	}
	require.NoError(t, s.Submit(r, token), "completed work retained producer capacity")
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
	require.NoError(t, s.Submit(core.Record{}, core.Token{}))
	require.NoError(t, s.Flush())
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
		require.Equal(t, "output", *r.(*kmsg.MetadataRequest).Topics[0].Topic, "refreshed the wrong topic")
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
		require.NoError(t, s.Submit(core.Record{}, core.Token{}), "optional metadata check prevented publishing")
		n := len(producer.records) - 1
		producer.callbacks[n](producer.records[n], nil)
		// Then refreshes stay throttled and remember only successfully read partition counts.
		if calls != step.calls || s.partitionCount != step.remembered {
			t.Fatal("incorrect routing refresh or delivery", step.seconds, calls, s.partitionCount)
		}
		select {
		case report := <-s.Deliveries():
			require.NoError(t, report.Err, "delivery failed")
		default:
			t.Fatal("delivery outcome missing")
		}
	}
	if strings.Count(logs.String(), "destination partition count changed;") != 1 || strings.Count(logs.String(), "destination partition count check failed") != 1 {
		t.Fatal("routing change or metadata failure was not logged once", logs.String())
	}
}

func TestSinkByteQueueCapReleasesAfterDelivery(t *testing.T) {
	// Given room for three records but only one 600-byte value within the byte queue.
	s, client := sinkFixture(t)
	s.settings.queueRecords, s.settings.queueBytes = 3, 1024
	r := core.Record{Value: make([]byte, 600)}
	require.NoError(t, s.Submit(r, core.Token{}))
	// When another value exceeds the byte allowance, then admission waits without touching the producer.
	var busy *core.Busy
	require.ErrorAs(t, s.Submit(r, core.Token{}), &busy)
	require.Len(t, client.records, 1)
	require.Equal(t, int64(1), s.records.Load())
	require.Equal(t, r.Size(), s.bytes.Load())
	// When delivery releases the first value, then retry admits the second value.
	client.callbacks[0](client.records[0], nil)
	require.NoError(t, receiveDelivery(t, s).Err)
	require.NoError(t, s.Submit(r, core.Token{}))
	require.Len(t, client.records, 2)
	client.callbacks[1](client.records[1], nil)
	require.NoError(t, receiveDelivery(t, s).Err)
	require.Zero(t, s.records.Load())
	require.Zero(t, s.bytes.Load())
}

func TestSinkRejectsRecordLargerThanQueueWithoutAdmittingWork(t *testing.T) {
	// Given a message ceiling above the queue allowance and no pending records.
	s, client := sinkFixture(t)
	s.settings.queueRecords, s.settings.queueBytes, s.settings.messageBytes = 3, 1024, 2000
	// When a record fits the message ceiling but cannot fit the empty queue.
	err := s.Submit(core.Record{Value: make([]byte, 1025)}, core.Token{})
	// Then the synchronous validation error leaves client and counters untouched.
	var rejected *core.RecordError
	require.ErrorAs(t, err, &rejected)
	require.Equal(t, "validation", rejected.Class)
	require.EqualError(t, rejected, "record exceeds producer queue byte limit")
	require.Empty(t, client.records)
	require.Zero(t, s.records.Load())
	require.Zero(t, s.bytes.Load())
}

func TestSinkUnknownTimestampTypeChecksClientSafeBoundaries(t *testing.T) {
	for _, tc := range []struct {
		stamp    int64
		accepted bool
	}{
		{-9223372036854, true}, {9223372036854, true},
		{-9223372036855, false}, {9223372036855, false},
	} {
		t.Run(fmt.Sprint(tc.stamp), func(t *testing.T) {
			// Given an unreadable destination timestamp type and a timestamp near the client's range.
			s, client := sinkFixture(t)
			// When submitting, then only the exact safe boundaries reach the producer.
			err := s.Submit(core.Record{TimestampMS: &tc.stamp}, core.Token{})
			if tc.accepted {
				require.NoError(t, err)
				require.Len(t, client.records, 1)
				require.Equal(t, tc.stamp, client.records[0].Timestamp.UnixMilli())
				client.callbacks[0](client.records[0], nil)
				require.NoError(t, receiveDelivery(t, s).Err)
			} else {
				var rejected *core.RecordError
				require.ErrorAs(t, err, &rejected)
				require.Equal(t, "rejected_timestamp", rejected.Class)
				require.Empty(t, client.records)
				require.Zero(t, s.records.Load())
				require.Zero(t, s.bytes.Load())
			}
		})
	}
}

func receiveDelivery(t *testing.T, sink *Sink) core.Delivery {
	t.Helper()
	select {
	case report := <-sink.Deliveries():
		return report
	case <-time.After(time.Second):
		t.Fatal("delivery report did not arrive")
		return core.Delivery{}
	}
}
