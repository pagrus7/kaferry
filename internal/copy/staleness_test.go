package copy

import (
	"errors"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
)

func TestStalenessCapturesWhileBehindAndWaitsOnlyAtReadableTip(t *testing.T) {
	// Given a reader with a backlog after the wall-clock threshold.
	r, source, sink, store, clock := fixture(t)
	r.plan.Source.Kind = "TOPIC"
	upper := clock.Now().Add(-time.Minute).UnixMilli()
	source.high = 100
	query := &watermarkSource{testSource: source}
	r.source = query
	require.NoError(t, r.Assign([]Assignment{{Cursor: Cursor{Position: 10}, Window: plan.Window{Upper: &upper}}}))

	// When evaluating completion without reaching the readable tip.
	require.NoError(t, r.tips())

	// Then save the end at unchanged confirmed progress, without writing records.
	require.Equal(t, 1, query.calls)
	require.Equal(t, [][]Cursor{{{Position: 10, End: new(int64(100))}}}, store.saves)
	require.Empty(t, sink.writes)
	require.True(t, r.partitions[0].waitStarted.IsZero())

	// When ordinary backlog processing takes longer than the transaction timeout.
	clock.Sleep(time.Hour)
	source.high = 200
	require.NoError(t, r.tips())
	require.Equal(t, 1, query.calls)
	require.Equal(t, int64(100), *r.partitions[0].high)

	// Then only reaching a blocked readable tip starts that timeout.
	require.NoError(t, r.eof(Cursor{Position: 10}))
	require.NoError(t, r.tips())
	clock.Sleep(16 * time.Minute)
	require.ErrorContains(t, r.tips(), "tip wait exceeded")
}

func TestStalenessReadsBeforeDeadlineAndKeepsTimestampShortcutAfterCapture(t *testing.T) {
	// Given a future deadline and an in-range record.
	r, source, sink, store, clock := fixture(t)
	r.plan.Source.Kind = "TOPIC"
	r.plan.MaxStalenessMS = 1000
	upper := clock.Now().Add(time.Minute).UnixMilli()
	source.high = 10
	query := &watermarkSource{testSource: source}
	r.source = query
	require.NoError(t, r.Assign([]Assignment{{Cursor: Cursor{}, Window: plan.Window{Upper: &upper}}}))
	record := incoming(0, 0, 1)
	record.Record.TimestampMS = new(upper - 1)

	// When consuming before the deadline, then write immediately without capture.
	require.NoError(t, r.tips())
	require.NoError(t, r.input(record))
	require.Len(t, sink.writes, 1)
	require.Zero(t, query.calls)
	sink.reports <- Delivery{Token: sink.writes[0]}
	require.NoError(t, r.deliveries())

	// When the deadline passes while newer source offsets remain.
	clock.Sleep(62 * time.Second)
	require.NoError(t, r.tips())
	record = incoming(0, 1, 2)
	record.Record.TimestampMS = new(upper + 1001)
	require.NoError(t, r.input(record))
	require.NoError(t, r.commit(r.ids()))

	// Then the timestamp trigger still stops before the saved end.
	require.Len(t, sink.writes, 1)
	require.Equal(t, &Stop{Reason: "timestamp", At: 1}, r.partitions[0].stop)
	require.Equal(t, int64(10), *store.saves[len(store.saves)-1][0].End)
	require.Equal(t, int64(1), store.saves[len(store.saves)-1][0].Position)
}

func TestStalenessResumeKeepsEndAndCompletedStops(t *testing.T) {
	for _, reason := range []string{"", "timestamp", "tip"} {
		t.Run(reason, func(t *testing.T) {
			// Given restored progress, a saved end, and a source that has grown.
			r, source, sink, store, clock := fixture(t)
			r.plan.Source.Kind = "TOPIC"
			upper := clock.Now().Add(-time.Minute).UnixMilli()
			source.high = 100
			query := &watermarkSource{testSource: source}
			r.source = query
			cursor := Cursor{Position: 2, End: new(int64(3))}
			if reason == "tip" {
				cursor.Position = 3
			}
			if reason != "" {
				cursor.Stop = &Stop{Reason: reason, At: cursor.Position}
			}
			require.NoError(t, r.Assign([]Assignment{{Cursor: cursor, Window: plan.Window{Upper: &upper}}}))

			// When evaluating after a backward clock adjustment, then reuse saved state.
			clock.now = clock.now.Add(-24 * time.Hour)
			require.NoError(t, r.tips())
			require.Zero(t, query.calls)
			if reason != "" {
				require.True(t, r.readyToExit())
				require.Empty(t, sink.writes)
				return
			}

			// When EOF proves a gap beyond the saved end, then save exactly that end.
			require.NoError(t, r.eof(Cursor{Position: 50}))
			require.NoError(t, r.tips())
			require.NoError(t, r.commit(r.ids()))
			require.Equal(t, [][]Cursor{{{Position: 3, End: new(int64(3)), Stop: &Stop{Reason: "tip", At: 3}}}}, store.saves)
		})
	}
}

func TestStalenessRejectsTruncatedEndAndFailedCaptureSave(t *testing.T) {
	for _, failSave := range []bool{false, true} {
		t.Run(map[bool]string{false: "truncation", true: "save failure"}[failSave], func(t *testing.T) {
			// Given a past deadline with either an invalid watermark or unavailable saves.
			r, source, sink, store, clock := fixture(t)
			r.plan.Source.Kind = "TOPIC"
			upper := clock.Now().Add(-time.Minute).UnixMilli()
			source.high = 0
			if failSave {
				source.high = 2
				store.fail = map[int32]error{0: errors.New("save unavailable")}
			}
			require.NoError(t, r.Assign([]Assignment{{Cursor: Cursor{Position: 1}, Window: plan.Window{Upper: &upper}}}))
			// When capturing, then fail before submitting any more output.
			err := r.tips()
			if failSave {
				require.ErrorContains(t, err, "save unavailable")
			} else {
				require.ErrorContains(t, err, "below consumed position")
			}
			require.Empty(t, sink.writes)
		})
	}
}
