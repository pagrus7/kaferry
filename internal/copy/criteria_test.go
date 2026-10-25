package copy

import (
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
)

func TestIndependentDisorderAndWallClockRules(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		s, d                   int64
		timestampStop, capture bool
	}{
		{"data only", -1, 0, true, false},
		{"clock only", 0, -1, false, true},
		{"separate allowances", 600000, 0, true, false},
		{"both disabled", -1, -1, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given a range ending a minute ago and independently configured rules.
			r, source, sink, _, clock := fixture(t)
			r.plan.Source.Kind = "TOPIC"
			r.plan.MaxStalenessMS = tc.s
			r.plan.MaxOutOfOrderMS = &tc.d
			upper := clock.Now().Add(-time.Minute).UnixMilli()
			source.high = 10
			query := &watermarkSource{testSource: source}
			r.source = query
			require.NoError(t, r.Assign([]Assignment{{Window: plan.Window{Upper: &upper}}}))
			// When checking the clock and then a timestamp beyond the upper bound.
			require.NoError(t, r.tips())
			input := incoming(0, 0, 1)
			input.Record.TimestampMS = new(upper + 1)
			require.NoError(t, r.input(input))
			// Then each switch controls only its own stopping evidence.
			require.Equal(t, tc.capture, query.calls > 0)
			require.Equal(t, tc.timestampStop, r.partitions[0].stop != nil)
			require.Empty(t, sink.writes)
		})
	}
}

func TestFixedOffsetEndCompletesWithoutTimestampOrClockEvidence(t *testing.T) {
	// Given a saved fixed end and deliberately misleading record timestamps.
	r, source, sink, store, _ := fixture(t)
	r.plan.Source.Kind = "TOPIC"
	r.plan.Offsets = &plan.OffsetRange{Latest: true}
	query := &watermarkSource{testSource: source}
	r.source = query
	require.NoError(t, r.Assign([]Assignment{{Cursor: Cursor{End: new(int64(2))}}}))
	for position := int64(0); position < 3; position++ {
		input := incoming(0, position, int(position+1))
		input.Record.TimestampMS = new(int64(1 << 60))
		input.Record.Source = &Provenance{Partition: 0, Offset: position}
		require.NoError(t, r.input(input))
		if position < 2 {
			sink.reports <- Delivery{Token: sink.writes[len(sink.writes)-1]}
			require.NoError(t, r.deliveries())
		}
	}
	// When checking completion, then stop exactly at the fixed exclusive end without another lookup.
	require.NoError(t, r.tips())
	require.NoError(t, r.commit(r.ids()))
	require.Len(t, sink.writes, 2)
	require.Zero(t, query.calls)
	require.Equal(t, &Stop{Reason: "tip", At: 2}, r.partitions[0].stop)
	require.EqualValues(t, 2, store.saves[len(store.saves)-1][0].Position)
}

func TestOffsetFilterUsesOriginalProvenance(t *testing.T) {
	// Given a file replay whose virtual cursor differs from the original Kafka offset.
	r, source, sink, _, _ := fixture(t)
	r.plan.Source.Kind = "FILE"
	r.plan.Offsets = &plan.OffsetRange{Lower: new(int64(100)), Upper: new(int64(102))}
	require.NoError(t, source.Start(r, nil))
	for i, offset := range []int64{99, 100, 101, 102} {
		input := incoming(0, int64(i), i+1)
		input.Record.Source = &Provenance{Partition: 7, Offset: offset}
		require.NoError(t, r.input(input))
		if offset == 100 || offset == 101 {
			sink.reports <- Delivery{Token: sink.writes[len(sink.writes)-1]}
			require.NoError(t, r.deliveries())
		}
	}
	require.Len(t, sink.writes, 2)
	require.EqualValues(t, 2, r.filtered)
	input := incoming(0, 4, 5)
	require.ErrorContains(t, r.input(input), "original source provenance")
}
