package copy

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type recordingObserver struct {
	snapshots []Snapshot
	saves     []SaveResult
	clock     Clock
	at        []time.Time
}

func (o *recordingObserver) Publish(s Snapshot) {
	o.snapshots = append(o.snapshots, s)
	if o.clock != nil {
		o.at = append(o.at, o.clock.Now())
	}
}
func (o *recordingObserver) Saved(s SaveResult) { o.saves = append(o.saves, s) }

type saveFunc func(context.Context, []Cursor) (map[int32]error, error)

func (f saveFunc) Save(ctx context.Context, c []Cursor) (map[int32]error, error) { return f(ctx, c) }

func TestSaveObservationsPreserveStoreResults(t *testing.T) {
	failure := errors.New("save failed")
	for _, tc := range []struct {
		name    string
		cursors []Cursor
		results map[int32]error
		err     error
		outcome string
	}{
		{"initial or metadata-only", []Cursor{{Partition: 0, Stop: &Stop{Reason: "eof", At: 0}}}, map[int32]error{0: nil}, nil, "success"},
		{"partial", []Cursor{{Partition: 0}, {Partition: 1}}, map[int32]error{0: nil, 1: failure}, nil, "partial"},
		{"missing response", []Cursor{{Partition: 0}}, map[int32]error{}, nil, "failure"},
		{"all failed", []Cursor{{Partition: 0}}, map[int32]error{0: failure}, nil, "failure"},
		{"request failed", []Cursor{{Partition: 0}}, map[int32]error{0: nil}, failure, "failure"},
		{"empty request", nil, map[int32]error{}, nil, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given a store with authoritative per-partition results.
			o := &recordingObserver{}
			calls := 0
			store := saveFunc(func(_ context.Context, c []Cursor) (map[int32]error, error) {
				calls++
				require.Equal(t, tc.cursors, c)
				return tc.results, tc.err
			})
			// When saving through observation.
			got, err := SaveProgress(t.Context(), store, tc.cursors, o)
			// Then results and failures are unchanged, and only real nonempty saves are counted.
			require.Equal(t, 1, calls)
			require.Equal(t, tc.results, got)
			require.True(t, err == tc.err, "save must return the exact error: got %v, want %v", err, tc.err)
			if tc.outcome == "" {
				require.Empty(t, o.saves)
				return
			}
			require.Len(t, o.saves, 1)
			require.Equal(t, tc.outcome, o.saves[0].Outcome)
			require.False(t, o.saves[0].At.IsZero())
			require.GreaterOrEqual(t, o.saves[0].Duration, time.Duration(0))
		})
	}
}

func TestProgressLogsAndSnapshotsUseLoopTime(t *testing.T) {
	// Given a quiet following source and independently configured progress logs.
	r, _, _, _, clock := fixture(t)
	r.plan.Source.Kind = "TOPIC"
	r.progressInterval = 10 * time.Second
	o := &recordingObserver{clock: clock}
	r.observer = o
	var logs bytes.Buffer
	r.log = slog.New(slog.NewJSONHandler(&logs, nil))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	start := clock.now
	clock.tick = func() {
		if clock.now.Sub(start) >= 25*time.Second {
			cancel()
		}
	}
	// When the loop runs for 25 seconds and stops.
	require.NoError(t, r.Run(ctx))
	// Then metrics publish every second, independently of the ten-second log cadence.
	require.Equal(t, 2, strings.Count(logs.String(), `"msg":"progress"`), logs.String())
	require.Len(t, o.snapshots, 26)
	require.Equal(t, start.Add(20*time.Millisecond), o.at[0])
	for i := 1; i < len(o.at)-1; i++ {
		require.Equal(t, time.Second, o.at[i].Sub(o.at[i-1]), "publication %d", i)
		require.Greater(t, o.snapshots[i].Iterations, o.snapshots[i-1].Iterations)
	}
	// Then cancellation forces the latest state out before the next periodic publication.
	last := len(o.snapshots) - 1
	require.Equal(t, start.Add(25*time.Second), o.at[last])
	require.Less(t, o.at[last].Sub(o.at[last-1]), time.Second)
	require.Greater(t, o.snapshots[last].Iterations, o.snapshots[last-1].Iterations)
	require.Equal(t, r.snapshot(), o.snapshots[last])
}

func TestSnapshotDistinguishesTrackingFromRecordCounts(t *testing.T) {
	// Given a tracked cursor gap and one consumed record deferred before tracking.
	r, _, _, _, _ := fixture(t)
	require.NoError(t, r.Assign([]Assignment{{Cursor: Cursor{Partition: 0}}}))
	require.NoError(t, r.tracker.Advance(Cursor{Partition: 0, Position: 2}))
	in := incoming(0, 2, 10)
	r.deferred = &in
	r.partitions[0].stop = &Stop{Reason: "eof", At: 2}
	// When inspecting the loop-owned state.
	s := r.snapshot()
	// Then accounting includes the gap but excludes the deferred record; EOF is a stop reason.
	require.Equal(t, int64(1), s.PendingEntries)
	require.Zero(t, s.PendingEstimatedBytes)
	require.Equal(t, int64(1), s.Partitions)
	require.Equal(t, int64(1), s.EOFStops)
	// When ownership is lost, then gauges clear without resetting invocation counters.
	r.submitted = 3
	r.confirmed = 2
	require.NoError(t, r.Revoke([]int32{0}, true))
	s = r.snapshot()
	require.Zero(t, s.Partitions)
	require.Zero(t, s.EOFStops)
	require.Zero(t, s.PendingEntries)
	require.Equal(t, uint64(3), s.Submitted)
	require.Equal(t, uint64(2), s.Confirmed)
}

func BenchmarkObservationDisabled(b *testing.B) {
	r := &Runner{clock: RealClock{}}
	b.ReportAllocs()
	for b.Loop() {
		r.report(false)
	}
}
