package copy

import (
	"testing"

	"github.com/pagrus7/kaferry/src/internal/plan"
	"github.com/stretchr/testify/require"
)

//kaferry:guarantee completion.offset-end
func TestFixedOffsetEndCompletesWithoutTimestampOrClockEvidence(t *testing.T) {
	// Given a saved fixed end and deliberately misleading record timestamps.
	r, _, sink, store, _ := fixture(t)
	r.plan.Source.Kind = "TOPIC"
	r.plan.Offsets = &plan.OffsetRange{Latest: true}
	require.NoError(t, r.Assign([]Assignment{{Cursor: Cursor{End: new(int64(2))}}}))
	for position := int64(0); position < 3; position++ {
		input := incoming(0, position, int(position+1))
		input.Record.TimestampMS = new(int64(1 << 60))
		input.Record.Source = &Provenance{Partition: 0, Offset: position}
		require.NoError(t, r.input(t.Context(), input))
		if position < 2 {
			sink.reports <- Delivery{Token: sink.writes[len(sink.writes)-1]}
			require.NoError(t, r.deliveries())
		}
	}
	// When checking completion, then stop exactly at the fixed exclusive end without another lookup.
	require.NoError(t, r.advance(t.Context()))
	require.NoError(t, r.commit(r.ids()))
	require.Len(t, sink.writes, 2)
	require.Equal(t, &Stop{Reason: "tip", At: 2}, r.partitions[0].stop)
	require.EqualValues(t, 2, store.saves[len(store.saves)-1][0].Position)
}

func TestOffsetFilterUsesOriginalProvenance(t *testing.T) {
	// Given a file replay whose virtual cursor differs from the original Kafka offset.
	r, source, sink, _, _ := fixture(t)
	r.plan.Source.Kind = "ARCHIVE"
	r.plan.Offsets = &plan.OffsetRange{Lower: new(int64(100)), Upper: new(int64(102))}
	require.NoError(t, source.Start(t.Context(), r, nil))
	for i, offset := range []int64{99, 100, 101, 102} {
		input := incoming(0, int64(i), i+1)
		input.Record.Source = &Provenance{Partition: 7, Offset: offset}
		require.NoError(t, r.input(t.Context(), input))
		if offset == 100 || offset == 101 {
			sink.reports <- Delivery{Token: sink.writes[len(sink.writes)-1]}
			require.NoError(t, r.deliveries())
		}
	}
	require.Len(t, sink.writes, 2)
	require.EqualValues(t, 2, r.filtered)
	input := incoming(0, 4, 5)
	require.ErrorContains(t, r.input(t.Context(), input), "original source provenance")
}
