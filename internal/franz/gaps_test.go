package franz

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"testing"
	"time"

	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
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

func TestGapProofPreservesRetainedRecords(t *testing.T) {
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

func TestGapProofRejectsUnsafeResponses(t *testing.T) {
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

func TestStaleFetchAfterGapProofIsDiscarded(t *testing.T) {
	// Given a verified gap through 10 and a stale record fetched before compaction.
	source := &Source{positions: map[int32]int64{0: 10}}
	// When that record arrives after the proof, or after its partition is revoked.
	for _, partition := range []int32{0, 1} {
		event, err := source.deliver(&kgo.Record{Partition: partition, Offset: 3, Value: []byte("stale")})
		// Then it creates no input/EOF and leaves current progress unchanged.
		if err != nil || event.Input != nil || event.EOF != nil || source.positions[0] != 10 {
			t.Fatal("stale fetch changed progress", event, err)
		}
	}
}

type stubGapReader struct {
	next         int64
	err          error
	calls        []int32
	perPartition map[int32]int64
}

func (g *stubGapReader) Next(_ context.Context, id int32, position int64) (int64, error) {
	g.calls = append(g.calls, id)
	if g.perPartition != nil {
		return max(position, g.perPartition[id]), g.err
	}
	return max(position, g.next), g.err
}

func sourceWithGaps(t *testing.T, count int32) (*Source, *stubGapReader) {
	t.Helper()
	source, _ := sourceFixture(t)
	source.lastTip = time.Time{}
	source.positions = make(map[int32]int64)
	stable := make(map[int32]int64)
	for id := int32(0); id < count; id++ {
		source.positions[id] = 0
		stable[id] = 10
	}
	source.inspector = &sourceMetadataMock{stable: stable}
	proof := &stubGapReader{}
	source.gaps = proof
	return source, proof
}
func TestIntermediateGapDoesNotSignalTip(t *testing.T) {
	// Given a quiet partition with stable end 10 and a proof reaching only offset 5.
	source, proof := sourceWithGaps(t, 1)
	proof.next = 5
	// When a bounded gap is proven before the actual tip.
	require.NoError(t, source.pollTips(t.Context()))
	// Then progress advances locally without telling the core it reached the tip.
	if source.positions[0] != 5 || len(source.readyEOF) != 0 {
		t.Fatal("intermediate gap looked like EOF", source.positions, source.readyEOF)
	}
	// When a later proof reaches the sampled stable end.
	proof.next = 10
	require.NoError(t, source.pollTips(t.Context()))
	// Then the core receives exactly the actual consumed end.
	if len(source.readyEOF) != 1 || source.readyEOF[0].Position != 10 {
		t.Fatal("actual tip was not reported", source.readyEOF)
	}
}

func TestGapProbesRotateAndExcludePausedOrHeldPartitions(t *testing.T) {
	// Given three quiet partitions that still contain retained data.
	source, proof := sourceWithGaps(t, 3)
	// When each timer turn can probe only one partition.
	for range 3 {
		require.NoError(t, source.pollTips(t.Context()))
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
	require.NoError(t, source.pollTips(t.Context()))
	// Then neither partition is probed or advanced.
	if len(proof.calls) != 1 || proof.calls[0] != 0 || source.positions[1] != 0 || source.positions[2] != 0 {
		t.Fatal("paused or held record was bypassed", proof.calls, source.positions)
	}
}

func TestHotPartitionCannotStarveAQuietPartitionsGapProof(t *testing.T) {
	// Given a ready record on a hot partition and a compacted tail on a quiet partition.
	s, proof := sourceWithGaps(t, 2)
	proof.perPartition = map[int32]int64{0: 0, 1: 10}
	feed(s, &kgo.Record{Partition: 0, Offset: 0, Value: []byte("hot")}, nil)
	// When polling without waiting for an idle timer, then the quiet partition still reaches its proven end.
	event, err := s.Poll(time.Second)
	if err != nil || event.EOF == nil || event.EOF.Partition != 1 || event.EOF.Position != 10 {
		t.Fatal("hot input starved the quiet tip", event, err, proof.calls)
	}
	// Then the hot record remains available on the following poll.
	event, err = s.Poll(time.Second)
	if err != nil || event.Input == nil || string(event.Input.Record.Value) != "hot" {
		t.Fatal("tip probe discarded hot input", event, err)
	}
}

func TestGapProbeErrorsRetryOnlyWhenRecoverable(t *testing.T) {
	for _, scenario := range []struct {
		failure error
		stops   bool
	}{
		{kerr.FencedLeaderEpoch, false}, {context.DeadlineExceeded, false}, {io.EOF, false},
		{kerr.UnknownTopicOrPartition, true}, {errors.New("corrupt batch"), true},
	} {
		t.Run(scenario.failure.Error(), func(t *testing.T) {
			// Given a gap reader that cannot prove any progress on this turn.
			source, proof := sourceWithGaps(t, 1)
			proof.err = scenario.failure
			// When polling after a probe failure.
			event, err := source.Poll(0)
			// Then only temporary failures permit another poll, and no failure advances progress.
			if (err != nil) != scenario.stops || event.Input != nil || event.EOF != nil || source.positions[0] != 0 || len(proof.calls) != 1 {
				t.Fatal("wrong probe error handling", event, err, source.positions, proof.calls)
			}
		})
	}
}

func TestThrottledGapReplyProvesNothingAndCanRetry(t *testing.T) {
	for _, throttled := range []bool{false, true} {
		// Given a verified source identity and a fetch response without partition data.
		id := [16]byte{1}
		mock := &metadataMock{request: func(_ context.Context, request kmsg.Request) (kmsg.Response, error) {
			if request.Key() == int16(kmsg.Metadata) {
				return metadataResponse(id), nil
			}
			response := &kmsg.FetchResponse{}
			if throttled {
				response.ThrottleMillis = 100
			}
			return response, nil
		}}
		proof := &fetchGap{client: mock, topic: "input", topicID: topicID(id)}
		// When the gap is probed, then no progress is inferred and a throttled reply can be retried.
		next, err := proof.Next(t.Context(), 0, 7)
		var retry *core.Recoverable
		if next != 7 || err == nil || errors.As(err, &retry) != throttled {
			t.Fatal("unsafe empty fetch handling", next, err)
		}
	}
}

func TestGapMetadataFailureCannotAdvanceCursorOrFetch(t *testing.T) {
	for _, tc := range []struct {
		name     string
		identity [16]byte
		leader   int32
		code     int16
		retry    bool
	}{
		{name: "recreated topic", identity: [16]byte{2}},
		{name: "leader election", identity: [16]byte{1}, leader: -1, retry: true},
		{name: "retryable partition", identity: [16]byte{1}, code: kerr.NotLeaderForPartition.Code, retry: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given a source cursor and metadata that no longer permits a safe gap fetch.
			fetches := 0
			client := &metadataMock{request: func(_ context.Context, request kmsg.Request) (kmsg.Response, error) {
				if _, ok := request.(*kmsg.MetadataRequest); !ok {
					fetches++
					return nil, errors.New("unsafe fetch")
				}
				response := metadataResponse(tc.identity)
				response.Topics[0].Partitions[0].Leader = tc.leader
				response.Topics[0].Partitions[0].ErrorCode = tc.code
				return response, nil
			}}
			proof := &fetchGap{client: client, topic: "input", topicID: topicID([16]byte{1})}
			// When probing for a compaction gap, then no cursor or fetch escapes failed metadata.
			next, err := proof.Next(t.Context(), 0, 7)
			require.Equal(t, int64(7), next)
			require.Error(t, err)
			var retry *core.Recoverable
			require.Equal(t, tc.retry, errors.As(err, &retry))
			require.Zero(t, fetches)
		})
	}
}
