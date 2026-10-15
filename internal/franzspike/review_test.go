//go:build integration && franzspike

package franzspike

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func newProbe(t *testing.T, src, dst, group string) (*Source, *lifeProbe) {
	t.Helper()
	p := spikePlan(t, fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' OPTIONS(GROUP='%s',START=EARLIEST)", dst, src, group))
	_, id, err := EndpointIdentity(ctx(t), p.Source)
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewSource(p, "probe", id)
	if err != nil {
		t.Fatal(err)
	}
	life := &lifeProbe{src: s}
	if err := s.Start(life); err != nil {
		t.Fatal(err)
	}
	return s, life
}
func TestSpikeHeldPartitionDoesNotBlockAnotherPartition(t *testing.T) {
	// Given a fetched record for partition 0, paused before handoff to the core.
	src, dst := topic(t, 2), topic(t, 1)
	producer := client(t, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	publish := func(part int32) {
		t.Helper()
		if err := producer.ProduceSync(ctx(t), &kgo.Record{Topic: src, Partition: part, Value: []byte("one")}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	publish(0)
	s, life := newProbe(t, src, dst, src+"-held")
	defer s.Close()
	pollUntil(t, s, func() bool { return life.assignments == 2 })
	select {
	case result := <-s.fetched:
		s.pending = &result
	case <-time.After(5 * time.Second):
		t.Fatal("no first fetch")
	}
	if err := s.Pause(0, true); err != nil {
		t.Fatal(err)
	}
	if event, err := s.Poll(0); err != nil || event.Input != nil {
		t.Fatal("paused input escaped", event, err)
	}
	// When another partition receives a record while partition 0 remains paused.
	publish(1)
	deadline := time.Now().Add(5 * time.Second)
	found := false
	for !found {
		event, err := s.Poll(20 * time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		found = event.Input != nil && event.Input.Cursor.Partition == 1
		if time.Now().After(deadline) {
			t.Fatal("held record blocked another partition")
		}
	}
	// Then that record is delivered, and resuming partition 0 delivers its held record first.
	if err := s.Pause(0, false); err != nil {
		t.Fatal(err)
	}
	event, err := s.Poll(0)
	if err != nil || event.Input == nil || event.Input.Cursor.Partition != 0 {
		t.Fatal(event, err)
	}
}
func TestSpikePauseDoesNotSurviveReassignment(t *testing.T) {
	// Given a paused member that transfers one partition to a second member.
	src, dst := topic(t, 2), topic(t, 1)
	group := src + "-reassign"
	first, life := newProbe(t, src, dst, group)
	defer first.Close()
	pollUntil(t, first, func() bool { return life.assignments == 2 })
	for id := range first.positions {
		if err := first.Pause(id, true); err != nil {
			t.Fatal(err)
		}
	}
	second, other := newProbe(t, src, dst, group)
	closed := false
	defer func() {
		if !closed {
			second.Close()
		}
	}()
	deadline := time.Now().Add(10 * time.Second)
	for other.assignments == 0 || life.revoked == 0 {
		if _, err := first.Poll(10 * time.Millisecond); err != nil {
			t.Fatal(err)
		}
		if _, err := second.Poll(10 * time.Millisecond); err != nil {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("rebalance did not finish")
		}
	}
	var transferred int32
	for id := range second.positions {
		transferred = id
	}
	// When the second member leaves and that partition returns to the first.
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	pollUntil(t, first, func() bool { _, ok := first.positions[transferred]; return ok })
	producer := client(t, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err := producer.ProduceSync(ctx(t), &kgo.Record{Topic: src, Partition: transferred, Value: []byte("returned")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	// Then the client fetches it without an explicit unpause from the lifecycle owner.
	deadline = time.Now().Add(5 * time.Second)
	for {
		event, err := first.Poll(20 * time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		if event.Input != nil && event.Input.Cursor.Partition == transferred {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("client kept revoked partition paused")
		}
	}
}
func TestSpikeMissingOffsetResponseIsAnError(t *testing.T) {
	// Given metadata for a topic that does not exist.
	broker := kfake.MustCluster(kfake.NumBrokers(1))
	defer broker.Close()
	c, err := kgo.NewClient(kgo.SeedBrokers(broker.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	source := &Source{admin: kadm.NewClient(c), plan: plan.Plan{Source: plan.Endpoint{Kind: "TOPIC", Name: "missing"}}, ctx: context.Background(), positions: map[int32]int64{0: 0}, paused: map[int32]bool{}}
	// When probing EOF and physical end.
	event, err := source.Poll(0)
	// Then missing offsets never become a fabricated zero-offset EOF or high watermark.
	if err == nil || event.EOF != nil {
		t.Fatal("missing topic looked like EOF", event, err)
	}
	if _, err := source.HighWatermark(0); err == nil {
		t.Fatal("missing topic looked like high watermark zero")
	}
}
func TestSpikeSinkHeaderBudgetAndFlush(t *testing.T) {
	// Given a long linger, a narrow worker byte cap and one buffered header-heavy record.
	src, dst := topic(t, 1), topic(t, 1)
	p := spikePlan(t, fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='127.0.0.1:39092','linger.ms'='60000') INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' OPTIONS(GROUP='%s',START=EARLIEST)", dst, src, src+"-bytes"))
	limits := config.Defaults(t.TempDir()).Worker
	limits.ByteCap = 1000
	sink, err := NewSink(p, limits)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	record := core.Record{Value: []byte("one"), Headers: []core.Header{{Name: "header", Value: bytes.Repeat([]byte("h"), 600)}}}
	if err := sink.Submit(record, core.Token{}); err != nil {
		t.Fatal(err)
	}
	// When accepting another record would exceed the byte cap because of headers.
	var busy *core.Busy
	if err := sink.Submit(record, core.Token{}); !errors.As(err, &busy) {
		t.Fatalf("wanted Busy, got %v", err)
	}
	// Then submission stays nonblocking, and Flush bypasses the long linger.
	if err := sink.Flush(); err != nil {
		t.Fatal(err)
	}
	select {
	case report := <-sink.Deliveries():
		if report.Err != nil {
			t.Fatal(report.Err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("flush did not bypass linger")
	}
}
func TestSpikeRawTimestampDecodingAndRejection(t *testing.T) {
	// Given a raw wire batch whose timestamp bypasses franz-go's time.Time producer encoder.
	src, dst := topic(t, 1), topic(t, 1)
	stamp := int64(253402214400000) // 9999-12-31, beyond UnixNano's range.
	record := kmsg.Record{Value: []byte("sentinel")}
	record.Length = int32(len(record.AppendTo(nil)) - 1)
	batch := kmsg.RecordBatch{PartitionLeaderEpoch: -1, Magic: 2, FirstTimestamp: stamp, MaxTimestamp: stamp, ProducerID: -1, ProducerEpoch: -1, FirstSequence: -1, NumRecords: 1, Records: record.AppendTo(nil)}
	batch.Length = int32(len(batch.AppendTo(nil)) - 12)
	data := batch.AppendTo(nil)
	batch.CRC = int32(crc32.Checksum(data[21:], crc32.MakeTable(crc32.Castagnoli)))
	request := kmsg.NewPtrProduceRequest()
	request.Acks = -1
	request.TimeoutMillis = 5000
	request.Topics = []kmsg.ProduceRequestTopic{{Topic: src, Partitions: []kmsg.ProduceRequestTopicPartition{{Partition: 0, Records: batch.AppendTo(nil)}}}}
	producer := client(t)
	response, err := request.RequestWith(ctx(t), producer)
	if err != nil {
		t.Fatal(err)
	}
	if code := response.Topics[0].Partitions[0].ErrorCode; code != 0 {
		t.Fatal("raw produce failed", code)
	}
	reader := client(t, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{src: {0: kgo.NewOffset().AtStart()}}))
	// When reading the timestamp and copying through the real engine.
	if got := readN(t, reader, 1)[0].Timestamp.UnixMilli(); got != stamp {
		t.Fatalf("consumer wrapped raw timestamp: want %d got %d", stamp, got)
	}
	p := spikePlan(t, fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' OPTIONS(GROUP='%s',START=EARLIEST)", dst, src, src+"-raw-time"))
	err = Execute(ctx(t), p, config.Defaults(t.TempDir()).Worker, slog.New(slog.NewTextHandler(io.Discard, nil)))
	// Then the unsafe write is rejected before output or source progress advances.
	if err == nil {
		t.Fatal("unsafe timestamp was silently copied")
	}
	ends, err := kadm.NewClient(producer).ListEndOffsets(ctx(t), dst)
	if err != nil || ends[dst][0].Offset != 0 {
		t.Fatal("unsafe output", ends, err)
	}
	saved, err := kadm.NewClient(producer).FetchOffsets(ctx(t), p.Group)
	if err != nil || saved[src][0].At != 0 {
		t.Fatal("unsafe progress", saved, err)
	}
}
