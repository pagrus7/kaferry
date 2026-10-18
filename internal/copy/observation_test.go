package copy

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"
)

type recordingObserver struct {
	snapshots []Snapshot
	saves     []SaveResult
}

func (o *recordingObserver) Publish(s Snapshot) { o.snapshots = append(o.snapshots, s) }
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
				if !reflect.DeepEqual(c, tc.cursors) {
					t.Fatal("cursors changed")
				}
				return tc.results, tc.err
			})
			// When saving through observation.
			got, err := SaveProgress(t.Context(), store, tc.cursors, o)
			// Then results and failures are unchanged, and only real nonempty saves are counted.
			if calls != 1 || !reflect.DeepEqual(got, tc.results) || err != tc.err {
				t.Fatal(calls, got, err)
			}
			if tc.outcome == "" {
				if len(o.saves) != 0 {
					t.Fatal(o.saves)
				}
				return
			}
			if len(o.saves) != 1 || o.saves[0].Outcome != tc.outcome || o.saves[0].At.IsZero() || o.saves[0].Duration < 0 {
				t.Fatal(o.saves)
			}
		})
	}
}

func TestProgressLogsAndSnapshotsUseLoopTime(t *testing.T) {
	// Given a quiet following source and independently configured progress logs.
	r, _, _, _, clock := fixture(t)
	r.plan.Source.Kind = "TOPIC"
	r.progressInterval = 10 * time.Second
	o := &recordingObserver{}
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
	if err := r.Run(ctx); err != nil {
		t.Fatal(err)
	}
	// Then it emits two progress logs and a final snapshot, including quiet-loop activity.
	if strings.Count(logs.String(), `"msg":"progress"`) != 2 {
		t.Fatal(logs.String())
	}
	if len(o.snapshots) < 2 || o.snapshots[len(o.snapshots)-1].Iterations == 0 {
		t.Fatal(o.snapshots)
	}
	for i := 1; i < len(o.snapshots); i++ {
		if o.snapshots[i].Iterations < o.snapshots[i-1].Iterations {
			t.Fatal("activity reset")
		}
	}
}

func TestSnapshotDistinguishesTrackingFromRecordCounts(t *testing.T) {
	// Given a tracked cursor gap and one consumed record deferred before tracking.
	r, _, _, _, _ := fixture(t)
	if err := r.Assign([]Assignment{{Cursor: Cursor{Partition: 0}}}); err != nil {
		t.Fatal(err)
	}
	if err := r.tracker.Advance(Cursor{Partition: 0, Position: 2}); err != nil {
		t.Fatal(err)
	}
	in := incoming(0, 2, 10)
	r.deferred = &in
	r.partitions[0].stop = &Stop{Reason: "eof", At: 2}
	// When inspecting the loop-owned state.
	s := r.snapshot()
	// Then accounting includes the gap but excludes the deferred record; EOF is a stop reason.
	if s.PendingEntries != 1 || s.PendingEstimatedBytes != 0 || s.Partitions != 1 || s.EOFStops != 1 {
		t.Fatal(s)
	}
	// When ownership is lost, then gauges clear without resetting invocation counters.
	r.submitted = 3
	r.confirmed = 2
	if err := r.Revoke([]int32{0}, true); err != nil {
		t.Fatal(err)
	}
	s = r.snapshot()
	if s.Partitions != 0 || s.EOFStops != 0 || s.PendingEntries != 0 || s.Submitted != 3 || s.Confirmed != 2 {
		t.Fatal(s)
	}
}

func BenchmarkObservationDisabled(b *testing.B) {
	r := &Runner{clock: RealClock{}}
	b.ReportAllocs()
	for b.Loop() {
		r.report(false)
	}
}
