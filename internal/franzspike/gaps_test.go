//go:build integration && franzspike

package franzspike

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
)

func compactedBatch(retained ...int32) []byte {
	batch := kmsg.RecordBatch{PartitionLeaderEpoch: -1, Magic: 2, LastOffsetDelta: 9, FirstTimestamp: time.Now().Add(-time.Hour).UnixMilli(), ProducerID: -1, ProducerEpoch: -1, FirstSequence: -1}
	batch.MaxTimestamp = batch.FirstTimestamp
	for _, offset := range retained {
		record := kmsg.Record{OffsetDelta: offset, Value: []byte(fmt.Sprintf("retained-%d", offset))}
		record.Length = int32(len(record.AppendTo(nil)) - 1)
		batch.Records = record.AppendTo(batch.Records)
		batch.NumRecords++
	}
	batch.Length = int32(len(batch.AppendTo(nil)) - 12)
	data := batch.AppendTo(nil)
	batch.CRC = int32(crc32.Checksum(data[21:], crc32.MakeTable(crc32.Castagnoli)))
	return batch.AppendTo(nil)
}

func TestSpikeCompactedOffsetsReachTheBoundedEnd(t *testing.T) {
	for _, scenario := range []struct {
		retained []int32
		version  int16
	}{{nil, 17}, {[]int32{0}, 17}, {[]int32{0, 5}, 17}, {nil, 12}} {
		retained := scenario.retained
		t.Run(fmt.Sprintf("retained=%v_version=%d", retained, scenario.version), func(t *testing.T) {
			// Given a legal compacted batch whose last offset survives without a record.
			versions := kversion.V3_9_0()
			versions.SetMaxKeyVersion(int16(kmsg.Fetch), scenario.version)
			cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "source", "output"), kfake.MaxVersions(versions))
			defer cluster.Close()
			producer, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
			if err != nil {
				t.Fatal(err)
			}
			defer producer.Close()
			for range 10 {
				if err := producer.ProduceSync(ctx(t), &kgo.Record{Topic: "source", Value: []byte("compacted")}).FirstErr(); err != nil {
					t.Fatal(err)
				}
			}
			batch := compactedBatch(retained...)
			metadata, err := kadm.NewClient(producer).Metadata(ctx(t), "source")
			if err != nil {
				t.Fatal(err)
			}
			id := metadata.Topics["source"].ID
			cluster.ControlKey(int16(kmsg.Fetch), func(request kmsg.Request) (kmsg.Response, error, bool) {
				cluster.KeepControl()
				req := request.(*kmsg.FetchRequest)
				res := req.ResponseKind().(*kmsg.FetchResponse)
				for _, topic := range req.Topics {
					if topic.Topic != "source" && topic.TopicID != id {
						continue
					}
					result := kmsg.FetchResponseTopic{Topic: topic.Topic, TopicID: topic.TopicID}
					for _, part := range topic.Partitions {
						p := kmsg.NewFetchResponseTopicPartition()
						p.Partition, p.HighWatermark, p.LastStableOffset, p.LogStartOffset = part.Partition, 10, 10, 0
						if part.FetchOffset < 10 {
							p.RecordBatches = batch
						}
						result.Partitions = append(result.Partitions, p)
					}
					res.Topics = append(res.Topics, result)
				}
				return res, nil, len(res.Topics) > 0
			})
			p := fakePlan(t, cluster, fmt.Sprintf("INSERT INTO TOPIC 'output' SELECT * FROM TOPIC 'source' WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' OPTIONS(GROUP='compacted',CUTOFF=INTERVAL '0' SECOND)", time.Now().Add(-30*time.Minute).UTC().Truncate(time.Millisecond).Format(time.RFC3339Nano)))
			deadline, cancel := context.WithTimeout(ctx(t), 4*time.Second)
			defer cancel()
			// When reading through the compacted tail with a bounded time range.
			err = Execute(deadline, p, config.Defaults(t.TempDir()).Worker, slog.New(slog.NewTextHandler(io.Discard, nil)))
			// Then the batch's consumed end is saved, and every retained record is copied.
			if err != nil {
				t.Fatalf("did not complete across compacted offsets: %v", err)
			}
			saved, err := kadm.NewClient(producer).FetchOffsets(ctx(t), p.Group)
			if err != nil || saved["source"][0].At != 10 {
				t.Fatalf("wrong consumed end: %v %v", saved, err)
			}
			want := int64(len(retained))
			if got := cluster.PartitionInfo("output", 0).HighWatermark; got != want {
				t.Fatalf("copied %d records, wanted %d", got, want)
			}
			// When the same query restarts at the verified end.
			if err := Execute(ctx(t), p, config.Defaults(t.TempDir()).Worker, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
				t.Fatal(err)
			}
			// Then completed compacted offsets do not produce another write.
			if got := cluster.PartitionInfo("output", 0).HighWatermark; got != want {
				t.Fatalf("restart duplicated retained data: %d", got)
			}
		})
	}
}

func TestSpikeGapProofPreservesRetainedRecords(t *testing.T) {
	// Given a batch retaining records at 0 and 5, with the first already consumed.
	p := kmsg.FetchResponseTopicPartition{Partition: 0, LastStableOffset: 10, RecordBatches: compactedBatch(0, 5)}
	// When proving the gap from position 1, and then from position 6.
	before, err := emptyBatchEnd(1, "source", &p)
	if err != nil || before != 1 {
		t.Fatal("proof skipped retained record 5", before, err)
	}
	after, err := emptyBatchEnd(6, "source", &p)
	// Then it advances only after the ordinary consumer has delivered record 5.
	if err != nil || after != 10 {
		t.Fatal("proof failed to cross compacted tail", after, err)
	}
}

func TestSpikeGapProofRejectsUnsafeResponses(t *testing.T) {
	for _, scenario := range []string{"broker error", "bad CRC", "truncated batch", "beyond stable end"} {
		t.Run(scenario, func(t *testing.T) {
			// Given an invalid or incomplete fetch response.
			p := kmsg.FetchResponseTopicPartition{Partition: 0, LastStableOffset: 10, RecordBatches: compactedBatch()}
			switch scenario {
			case "broker error":
				p.ErrorCode = 6 // NOT_LEADER_OR_FOLLOWER.
			case "bad CRC":
				p.RecordBatches[21] ^= 1
			case "truncated batch":
				p.RecordBatches = p.RecordBatches[:len(p.RecordBatches)-1]
			case "beyond stable end":
				p.LastStableOffset = 5
			}
			// When checking whether the response proves consumption.
			next, err := emptyBatchEnd(0, "source", &p)
			// Then the saved cursor cannot advance, even if a truncated batch yields no error.
			if next != 0 || (scenario != "truncated batch" && err == nil) {
				t.Fatal("unsafe response advanced cursor", next, err)
			}
		})
	}
}

func TestSpikeStaleFetchAfterGapProofIsDiscarded(t *testing.T) {
	// Given a verified gap through 10 and a stale record fetched before compaction.
	source := &Source{positions: map[int32]int64{0: 10}}
	// When that record arrives after the proof, or after its partition is revoked.
	for _, partition := range []int32{0, 1} {
		event, err := source.deliverRecord(&kgo.Record{Partition: partition, Offset: 3, Value: []byte("stale")})
		// Then it creates no input/EOF and leaves current progress unchanged.
		if err != nil || event.Input != nil || event.EOF != nil || source.positions[0] != 10 {
			t.Fatal("stale fetch changed progress", event, err)
		}
	}
}

type stubGapReader struct {
	next  int64
	err   error
	calls []int32
}

func (g *stubGapReader) Next(_ context.Context, id int32, position int64) (int64, error) {
	g.calls = append(g.calls, id)
	return max(position, g.next), g.err
}

func sourceWithGaps(t *testing.T, count int32) (*Source, *stubGapReader) {
	t.Helper()
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(count, "source"))
	t.Cleanup(cluster.Close)
	client, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	positions := make(map[int32]int64)
	for id := int32(0); id < count; id++ {
		positions[id] = 0
		for range 10 {
			if err := client.ProduceSync(ctx(t), &kgo.Record{Topic: "source", Partition: id, Value: []byte("retained")}).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
	}
	gap := &stubGapReader{}
	return &Source{admin: kadm.NewClient(client), gaps: gap, plan: plan.Plan{Source: plan.Endpoint{Name: "source"}}, positions: positions, ctx: context.Background()}, gap
}

func TestSpikeIntermediateGapDoesNotSignalTip(t *testing.T) {
	// Given a quiet partition with stable end 10 and a proof reaching only offset 5.
	source, proof := sourceWithGaps(t, 1)
	proof.next = 5
	// When a bounded gap is proven before the actual tip.
	if err := source.pollTips(ctx(t)); err != nil {
		t.Fatal(err)
	}
	// Then progress advances locally without telling the core it reached the tip.
	if source.positions[0] != 5 || len(source.readyEOF) != 0 {
		t.Fatal("intermediate gap looked like EOF", source.positions, source.readyEOF)
	}
	// When a later proof reaches the sampled stable end.
	proof.next = 10
	if err := source.pollTips(ctx(t)); err != nil {
		t.Fatal(err)
	}
	// Then the core receives exactly the actual consumed end.
	if len(source.readyEOF) != 1 || source.readyEOF[0].Position != 10 {
		t.Fatal("actual tip was not reported", source.readyEOF)
	}
}

func TestSpikeGapProbesRotateAndExcludePausedOrHeldPartitions(t *testing.T) {
	// Given three quiet partitions that still contain retained data.
	source, proof := sourceWithGaps(t, 3)
	// When each timer turn can probe only one partition.
	for range 3 {
		if err := source.pollTips(ctx(t)); err != nil {
			t.Fatal(err)
		}
	}
	seen := make(map[int32]bool)
	for _, id := range proof.calls {
		seen[id] = true
	}
	// Then every partition gets a turn, and none advances without a proof.
	if len(proof.calls) != 3 || len(seen) != 3 || len(source.readyEOF) != 0 {
		t.Fatal("probe starvation or false EOF", proof.calls, source.readyEOF)
	}
	// Given one paused partition and another with a held record.
	source.paused = map[int32]bool{1: true}
	source.held = map[int32]*kgo.Record{2: {Partition: 2, Offset: 0}}
	proof.calls = nil
	// When polling tips again.
	if err := source.pollTips(ctx(t)); err != nil {
		t.Fatal(err)
	}
	// Then neither partition is probed or advanced.
	if len(proof.calls) != 1 || proof.calls[0] != 0 || source.positions[1] != 0 || source.positions[2] != 0 {
		t.Fatal("paused or held record was bypassed", proof.calls, source.positions)
	}
}

func TestSpikeTransientGapFailureDoesNotStopOrdinaryConsumption(t *testing.T) {
	for _, failure := range []error{kerr.FencedLeaderEpoch, context.DeadlineExceeded, io.EOF, kerr.UnknownTopicOrPartition, errors.New("corrupt batch")} {
		t.Run(failure.Error(), func(t *testing.T) {
			// Given a gap reader that cannot prove any progress on this turn.
			source, proof := sourceWithGaps(t, 1)
			proof.err = failure
			// When its deadline expires or its leader changes during the probe.
			event, err := source.Poll(0)
			// Then transient errors leave ordinary consumption running; corruption still fails.
			wantFailure := failure.Error() == "corrupt batch" || errors.Is(failure, kerr.UnknownTopicOrPartition)
			if (err != nil) != wantFailure || event.Input != nil || event.EOF != nil || source.positions[0] != 0 || len(proof.calls) != 1 {
				t.Fatal("wrong probe error handling", event, err, source.positions, proof.calls)
			}
		})
	}
}
