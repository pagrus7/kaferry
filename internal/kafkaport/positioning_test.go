package kafkaport

import (
	"context"
	"io"
	"log/slog"
	"reflect"
	"testing"
	"time"

	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
func TestSavedMixedWindowSurvivesClockAdvanceAndAdoption(t *testing.T) {
	for _, origin := range []string{"saved", "adopted", "fresh"} {
		t.Run(origin, func(t *testing.T) {
			// Given a mixed NOW/literal window whose fresh resolution is now empty.
			hi, lo := int64(7200000), int64(3600000)
			p := &positioner{plan: plan.Plan{Window: plan.WindowSpec{Lower: &plan.Expression{Now: true, AdjustmentMS: -3600000}, Upper: &plan.Expression{LiteralMS: hi}}}, log: quietLogger()}
			saved, metadata := map[int32]int64{}, map[int32]Progress{}
			var adopted *plan.Window
			window := plan.Window{Lower: &lo, Upper: &hi}
			if origin == "saved" {
				saved[0] = 42
				metadata[0] = Progress{Window: &window}
			}
			if origin == "adopted" {
				adopted = &window
			}

			// When positioning after both the saved upper bound and its staleness.
			got, _, position, err := p.window(0, saved, metadata, adopted, time.UnixMilli(14400000))

			// Then persisted windows remain authoritative; only fresh resolution fails.
			if origin == "fresh" {
				if err == nil {
					t.Fatal("empty fresh window accepted")
				}
				return
			}
			if err != nil || !reflect.DeepEqual(got, window) || origin == "saved" && position != 42 {
				t.Fatal("saved window was re-resolved", got, position, err)
			}
		})
	}
}

type offsetCall struct {
	kind      OffsetKind
	committed bool
}
type offsetsStub struct {
	ctx   context.Context
	calls []offsetCall
}

func (s *offsetsStub) Offsets(ctx context.Context, _ []int32, kind OffsetKind, committed bool) (map[int32]int64, error) {
	s.ctx = ctx
	s.calls = append(s.calls, offsetCall{kind, committed})
	if kind == EarliestOffset {
		return map[int32]int64{0: 3, 2: 1}, nil
	}
	if committed {
		return map[int32]int64{0: 90, 2: 80}, nil
	}
	return map[int32]int64{0: 100, 2: 100}, nil
}

type timesStub struct {
	ctx      context.Context
	requests map[int32]int64
}

func (s *timesStub) OffsetsForTimes(ctx context.Context, requests map[int32]int64) (map[int32]int64, error) {
	s.ctx, s.requests = ctx, requests
	return map[int32]int64{0: 55, 2: -1}, nil
}

func TestPositioningBatchesTimeLookupsAndUsesCommittedEndForNoMatch(t *testing.T) {
	// Given two assigned partitions, with a time match in one and an open transaction in the other.
	offsets, times := &offsetsStub{}, &timesStub{}
	lower := int64(10)
	p := plan.Plan{Window: plan.WindowSpec{Lower: &plan.Expression{LiteralMS: lower}}}
	positioner := NewPositionerFactory(core.RealClock{}, quietLogger(), NewOffsetLookupFactory()).New(offsets, times, p, "fp", "topic-id")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// When resolving both starts through client-neutral mocked ports.
	result, err := positioner.Locate(ctx, []int32{2, 0}, nil, nil)
	// Then the unmatched partition starts at its stable end, and all calls retain the caller context.
	require.NoError(t, err)
	require.Len(t, result.Assignments, 2)
	require.Equal(t, int32(2), result.Assignments[0].Cursor.Partition)
	require.Equal(t, int64(80), result.Assignments[0].Cursor.Position, "no time match uses committed end")
	require.Equal(t, int32(0), result.Assignments[1].Cursor.Partition)
	require.Equal(t, int64(55), result.Assignments[1].Cursor.Position)
	require.Same(t, ctx, offsets.ctx)
	require.Same(t, ctx, times.ctx)
	require.Equal(t, map[int32]int64{0: 10, 2: 10}, times.requests)
	require.Equal(t, []offsetCall{{EarliestOffset, false}, {LatestOffset, false}, {LatestOffset, true}}, offsets.calls)
}

func TestReusedPositionerDoesNotRepeatAnEarlierEndLookup(t *testing.T) {
	// Given a positioner that started one assignment at its stable end.
	offsets := &offsetsStub{}
	p := NewPositionerFactory(core.RealClock{}, quietLogger(), NewOffsetLookupFactory()).New(offsets, &timesStub{}, plan.Plan{Start: "LATEST"}, "fp", "topic-id")
	if _, err := p.Locate(t.Context(), []int32{0}, nil, nil); err != nil {
		t.Fatal(err)
	}
	// When the next assignment resumes a different partition from its saved offset.
	result, err := p.Locate(t.Context(), []int32{2}, map[int32]int64{2: 9}, nil)
	// Then the old end lookup cannot replace the new partition's saved position.
	if err != nil || len(result.Assignments) != 1 || result.Assignments[0].Cursor.Position != 9 || len(offsets.calls) != 5 {
		t.Fatal("previous assignment affected resumed position", result, offsets.calls, err)
	}
}

type offsetReaderFunc func(context.Context, []int32, OffsetKind, bool) (map[int32]int64, error)

func (f offsetReaderFunc) Offsets(ctx context.Context, ids []int32, kind OffsetKind, committed bool) (map[int32]int64, error) {
	return f(ctx, ids, kind, committed)
}

func TestPositioningRejectsIncompleteOrInvalidOffsetResults(t *testing.T) {
	for _, stage := range []string{"earliest", "latest", "stable"} {
		for _, value := range []int64{-1, -2} {
			t.Run(stage+map[int64]string{-1: "-missing", -2: "-negative"}[value], func(t *testing.T) {
				// Given a broker adapter that omits an offset or returns a sentinel as a real position.
				reader := offsetReaderFunc(func(_ context.Context, _ []int32, kind OffsetKind, committed bool) (map[int32]int64, error) {
					current := "latest"
					if kind == EarliestOffset {
						current = "earliest"
					}
					if committed {
						current = "stable"
					}
					if current == stage {
						if value == -1 {
							return nil, nil
						}
						return map[int32]int64{0: value}, nil
					}
					return map[int32]int64{0: 0}, nil
				})
				p := NewPositionerFactory(core.RealClock{}, quietLogger(), NewOffsetLookupFactory()).New(reader, &timesStub{}, plan.Plan{Start: "LATEST"}, "fp", "topic-id")
				// When resolving the assignment, then it fails instead of silently starting at offset zero.
				if _, err := p.Locate(t.Context(), []int32{0}, nil, nil); err == nil {
					t.Fatal("invalid offset result accepted")
				}
			})
		}
	}
}

func TestPositioningRestoresCapturedEndAndTerminalStops(t *testing.T) {
	for _, reason := range []string{"timestamp", "tip", "worker_record_cap"} {
		t.Run(reason, func(t *testing.T) {
			// Given saved progress with a captured end smaller than the current source end.
			p := plan.Plan{Window: plan.WindowSpec{Upper: &plan.Expression{LiteralMS: 1000}}}
			pos := NewPositionerFactory(core.RealClock{}, quietLogger(), NewOffsetLookupFactory()).New(&offsetsStub{}, &timesStub{}, p, "fp", "topic-id")
			position := int64(42)
			if reason == "tip" {
				position = 60
			}
			metadata := map[int32]Progress{0: {End: new(int64(60)), Stop: &core.Stop{Reason: reason, At: position}}}
			// When reassigned, then keep the boundary and only clear an invocation record stop.
			result, err := pos.Locate(t.Context(), []int32{0}, map[int32]int64{0: position}, metadata)
			require.NoError(t, err)
			require.Equal(t, int64(60), *result.Assignments[0].Cursor.End)
			if reason == "worker_record_cap" {
				require.Nil(t, result.Assignments[0].Cursor.Stop)
			} else {
				require.Equal(t, metadata[0].Stop, result.Assignments[0].Cursor.Stop)
			}
		})
	}
	for _, end := range []int64{2, 101} {
		// Given a captured end outside current retained positions, when restoring, then fail closed.
		p := plan.Plan{Window: plan.WindowSpec{Upper: &plan.Expression{LiteralMS: 1000}}}
		pos := NewPositionerFactory(core.RealClock{}, quietLogger(), NewOffsetLookupFactory()).New(&offsetsStub{}, &timesStub{}, p, "fp", "topic-id")
		_, err := pos.Locate(t.Context(), []int32{0}, map[int32]int64{0: 2}, map[int32]Progress{0: {End: &end}})
		require.ErrorContains(t, err, "captured end")
	}
}
