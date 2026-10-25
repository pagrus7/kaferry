package kafkaport

import (
	"testing"

	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
)

func TestOffsetEndResolutionAndResume(t *testing.T) {
	for _, tc := range []struct {
		name, start        string
		bounds             plan.OffsetRange
		timestamp          bool
		wantStart, wantEnd int64
		calls              int
	}{
		{name: "latest", bounds: plan.OffsetRange{Latest: true}, wantStart: 100, wantEnd: 900, calls: 1},
		{name: "latest start uses same sample", start: "LATEST", bounds: plan.OffsetRange{Latest: true}, wantStart: 900, wantEnd: 900, calls: 1},
		{name: "literal", bounds: plan.OffsetRange{Lower: new(int64(200)), Upper: new(int64(800))}, wantStart: 200, wantEnd: 800},
		{name: "timestamp beyond end", bounds: plan.OffsetRange{Upper: new(int64(400))}, timestamp: true, wantStart: 400, wantEnd: 400},
		{name: "lower only", bounds: plan.OffsetRange{Lower: new(int64(200))}, wantStart: 200, wantEnd: -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given retained data and a possibly unstable tail.
			backend := &countingOffsets{low: 100, high: 1000, stable: 900}
			times := &countingTimes{}
			p := plan.Plan{Source: plan.Endpoint{Kind: "TOPIC"}, Start: tc.start, Offsets: &tc.bounds}
			if p.Start == "" {
				p.Start = "EARLIEST"
			}
			if tc.timestamp {
				p.Window.Lower = &plan.Expression{LiteralMS: 1}
			}
			positioner := NewPositionerFactory(&fixedClock{}, quietLogger(), NewOffsetLookupFactory()).New(backend, times, p, "fp", "id")
			// When initializing, then use exclusive ends and reuse a shared LATEST sample.
			located, err := positioner.Locate(t.Context(), []int32{1}, nil, nil)
			require.NoError(t, err)
			a := located.Assignments[0]
			require.Equal(t, tc.wantStart, a.Cursor.Position)
			if tc.wantEnd < 0 {
				require.Nil(t, a.Cursor.End)
			} else {
				require.Equal(t, tc.wantEnd, *a.Cursor.End)
			}
			require.Equal(t, tc.calls, readableCalls(backend.calls))
			if a.Cursor.End == nil {
				return
			}
			// When reassigned after growth, then reuse the saved boundary and completion.
			backend.high, backend.stable = 2000, 1900
			saved := Progress{Fingerprint: "fp", TopicID: "id", End: a.Cursor.End}
			located, err = positioner.Locate(t.Context(), []int32{1}, map[int32]int64{1: a.Cursor.Position}, map[int32]Progress{1: saved})
			require.NoError(t, err)
			require.Equal(t, tc.wantEnd, *located.Assignments[0].Cursor.End)
			require.Equal(t, tc.calls, readableCalls(backend.calls))
		})
	}
}

func TestRequestedOffsetRangesRejectUnavailablePositions(t *testing.T) {
	for _, r := range []plan.OffsetRange{{Lower: new(int64(99))}, {Lower: new(int64(1001))}, {Upper: new(int64(99))}, {Upper: new(int64(1001))}, {Lower: new(int64(950)), Latest: true}} {
		// Given an explicit bound outside the retained snapshot, when initializing, then fail instead of silently clamping it.
		b := &countingOffsets{low: 100, high: 1000, stable: 900}
		p := plan.Plan{Source: plan.Endpoint{Kind: "TOPIC"}, Start: "EARLIEST", Offsets: &r}
		_, err := NewPositionerFactory(&fixedClock{}, quietLogger(), NewOffsetLookupFactory()).New(b, &countingTimes{}, p, "fp", "id").Locate(t.Context(), []int32{1}, nil, nil)
		require.ErrorContains(t, err, "outside retained positions")
	}
}

func readableCalls(calls []offsetCall) int {
	n := 0
	for _, call := range calls {
		if call.committed {
			n++
		}
	}
	return n
}
