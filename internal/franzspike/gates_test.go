//go:build integration && franzspike

package franzspike

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

var sequence atomic.Uint64

func ctx(t *testing.T) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	return c
}
func client(t *testing.T, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	brokers := os.Getenv("KAFKA_TEST_BROKERS")
	if brokers == "" {
		brokers = "127.0.0.1:39092"
	}
	base := []kgo.Opt{kgo.SeedBrokers(strings.Split(brokers, ",")...), kgo.RequestTimeoutOverhead(2 * time.Second), kgo.RetryTimeout(5 * time.Second)}
	c, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.CloseAllowingRebalance)
	return c
}
func topic(t *testing.T, parts int32) string {
	t.Helper()
	c := client(t)
	a := kadm.NewClient(c)
	name := fmt.Sprintf("franz-spike-%d-%d", os.Getpid(), sequence.Add(1))
	configs := map[string]*string{"message.timestamp.before.max.ms": kadm.StringPtr("9223372036854775807"), "message.timestamp.after.max.ms": kadm.StringPtr("9223372036854775807")}
	if _, err := a.CreateTopic(ctx(t), parts, 1, configs, name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = a.DeleteTopics(cleanup, name)
	})
	return name
}
func readN(t *testing.T, c *kgo.Client, n int) []*kgo.Record {
	t.Helper()
	deadline := ctx(t)
	var out []*kgo.Record
	for len(out) < n {
		fs := c.PollRecords(deadline, n-len(out))
		if err := fs.Err(); err != nil {
			t.Fatal(err)
		}
		out = append(out, fs.Records()...)
	}
	return out
}

func TestSpikeFidelity(t *testing.T) {
	// Given opaque keys/values and ordered duplicate, NUL and non-UTF8 headers.
	src := topic(t, 1)
	headers := []kgo.RecordHeader{{Key: "dup", Value: nil}, {Key: "dup", Value: []byte{}}, {Key: "nul\x00name", Value: []byte{0, 255}}, {Key: string([]byte{255, 128}), Value: []byte{1}}}
	records := []*kgo.Record{
		{Topic: src, Key: nil, Value: nil, Headers: headers, Timestamp: time.UnixMilli(0)},
		{Topic: src, Key: []byte{}, Value: []byte{}, Headers: headers, Timestamp: time.UnixMilli(1)},
		{Topic: src, Key: []byte{0, 255}, Value: []byte{255, 0}, Headers: headers, Timestamp: time.UnixMilli(-2)},
	}
	producer := client(t, kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)))
	for _, r := range records {
		if err := producer.ProduceSync(ctx(t), r).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	consumer := client(t, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{src: {0: kgo.NewOffset().AtStart()}}), kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	// When consuming the full logical records.
	got := readN(t, consumer, len(records))
	// Then null/empty, raw headers and ordinary negative/zero timestamps survive.
	for i, r := range got {
		want := records[i]
		if !reflect.DeepEqual(r.Key, want.Key) || !reflect.DeepEqual(r.Value, want.Value) || !reflect.DeepEqual(r.Headers, want.Headers) || r.Timestamp.UnixMilli() != want.Timestamp.UnixMilli() || r.Attrs.TimestampType() != 0 {
			t.Fatalf("record %d mismatch: got=%+v want=%+v", i, r, want)
		}
	}
}

func TestSpikeTimestampRangeHypothesis(t *testing.T) {
	// Given timestamp extremes outside the currently documented native range.
	src := topic(t, 1)
	p := client(t)
	stamps := []int64{-1, 0, 1, 9223372036855, 1 << 62, math.MaxInt64}
	for _, stamp := range stamps {
		if err := p.ProduceSync(ctx(t), &kgo.Record{Topic: src, Timestamp: time.UnixMilli(stamp), Value: []byte(fmt.Sprint(stamp))}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	c := client(t, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{src: {0: kgo.NewOffset().AtStart()}}))
	// When the broker returns the stored timestamps.
	got := readN(t, c, len(stamps))
	// Then report the actual capability rather than assuming all int64 values work.
	lossless := true
	for i, r := range got {
		same := r.Timestamp.UnixMilli() == stamps[i]
		t.Logf("timestamp input=%d output=%d exact=%t type=%d", stamps[i], r.Timestamp.UnixMilli(), same, r.Attrs.TimestampType())
		lossless = lossless && same
	}
	if !lossless {
		t.Log("GATE LIMIT: unbounded int64 timestamp preservation is false; keep checked rejection outside the client-safe range")
	}
}

func TestSpikeMemberCommitMetadataAndPositioning(t *testing.T) {
	// Given a group that must start at offset 2 before its first fetch.
	src := topic(t, 1)
	group := src + "-group"
	p := client(t)
	for i := range 4 {
		if err := p.ProduceSync(ctx(t), &kgo.Record{Topic: src, Value: []byte{byte(i)}}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	adjusted := atomic.Bool{}
	consumer := client(t, kgo.ConsumerGroup(group), kgo.ConsumeTopics(src), kgo.DisableAutoCommit(), kgo.ConsumeResetOffset(kgo.NoResetOffset()), kgo.BlockRebalanceOnPoll(), kgo.Balancers(kgo.CooperativeStickyBalancer()), kgo.AdjustFetchOffsetsFn(func(_ context.Context, offsets map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
		for part := range offsets[src] {
			offsets[src][part] = kgo.NewOffset().At(2)
		}
		adjusted.Store(true)
		return offsets, nil
	}))
	// When polling, then synchronously committing custom metadata as a group member.
	got := readN(t, consumer, 1)
	if !adjusted.Load() || got[0].Offset != 2 {
		t.Fatal("positioning hook did not precede the first fetch", got[0])
	}
	metadata := `kfwd2:1;{"fp":"spike","tid":"spike"}`
	commitCtx := kgo.PreCommitFnContext(ctx(t), func(req *kmsg.OffsetCommitRequest) error {
		if req.MemberID == "" || req.Generation < 0 {
			return fmt.Errorf("commit is not fenced by group membership")
		}
		for i := range req.Topics {
			for j := range req.Topics[i].Partitions {
				req.Topics[i].Partitions[j].Metadata = &metadata
			}
		}
		return nil
	})
	var failure error
	consumer.CommitOffsetsSync(commitCtx, map[string]map[int32]kgo.EpochOffset{src: {0: {Epoch: -1, Offset: 3}}}, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, res *kmsg.OffsetCommitResponse, err error) {
		failure = err
		if res != nil {
			for _, topic := range res.Topics {
				for _, part := range topic.Partitions {
					if e := kerr.ErrorForCode(part.ErrorCode); e != nil {
						failure = e
					}
				}
			}
		}
	})
	if failure != nil {
		t.Fatal(failure)
	}
	// Then Kafka stores both the desired position and unchanged metadata bytes.
	offsets, err := kadm.NewClient(p).FetchOffsets(ctx(t), group)
	if err != nil {
		t.Fatal(err)
	}
	saved := offsets[src][0]
	if saved.Err != nil || saved.At != 3 || saved.Metadata != metadata {
		t.Fatal(saved)
	}
	consumer.AllowRebalance()
}

func TestSpikeReadCommittedControlCursor(t *testing.T) {
	// Given an aborted transaction larger than fetch limits, followed by committed data.
	src := topic(t, 1)
	tx := client(t, kgo.TransactionalID(src+"-tx"), kgo.ProducerBatchMaxBytes(1024))
	if err := tx.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	for range 100 {
		if err := tx.ProduceSync(ctx(t), &kgo.Record{Topic: src, Value: bytes.Repeat([]byte("a"), 512)}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.EndTransaction(ctx(t), kgo.TryAbort); err != nil {
		t.Fatal(err)
	}
	p := client(t)
	tail := &kgo.Record{Topic: src, Value: []byte("committed")}
	if err := p.ProduceSync(ctx(t), tail).FirstErr(); err != nil {
		t.Fatal(err)
	}
	c := client(t, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{src: {0: kgo.NewOffset().AtStart()}}), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.FetchMaxBytes(1024), kgo.FetchMaxPartitionBytes(1024), kgo.KeepControlRecords())
	// When reading committed data with control records retained for cursor advancement.
	deadline := ctx(t)
	controls := 0
	found := false
	for !found {
		fs := c.PollRecords(deadline, 1)
		if err := fs.Err(); err != nil {
			t.Fatal(err)
		}
		fs.EachRecord(func(r *kgo.Record) {
			if r.Attrs.IsControl() {
				controls++
				return
			}
			if !bytes.Equal(r.Value, []byte("committed")) {
				t.Error("aborted data was delivered")
			}
			if r.Offset != tail.Offset {
				t.Error("committed cursor mismatch", r.Offset, tail.Offset)
			}
			found = true
		})
	}
	// Then aborted data stays hidden and the committed record is never skipped.
	if controls == 0 {
		t.Fatal("no control cursor was exposed")
	}
	t.Logf("aborted=100 control_records=%d committed_offset=%d next=%d", controls, tail.Offset, tail.Offset+1)
}
