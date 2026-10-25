package kafkaport

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
)

type fixedClock struct{ now time.Time }

func (c *fixedClock) Now() time.Time        { return c.now }
func (c *fixedClock) Sleep(d time.Duration) { c.now = c.now.Add(d) }

type countingOffsets struct {
	low, high, stable int64
	calls             []offsetCall
}

func (o *countingOffsets) Offsets(_ context.Context, ids []int32, kind OffsetKind, committed bool) (map[int32]int64, error) {
	o.calls = append(o.calls, offsetCall{kind, committed})
	value := o.high
	if kind == EarliestOffset {
		value = o.low
	}
	if committed {
		value = o.stable
	}
	result := make(map[int32]int64)
	for _, id := range ids {
		result[id] = value
	}
	return result, nil
}

type countingTimes struct{ calls int }

func (t *countingTimes) OffsetsForTimes(_ context.Context, queries map[int32]int64) (map[int32]int64, error) {
	t.calls++
	result := make(map[int32]int64)
	for id := range queries {
		result[id] = 500
		if id%3 == 0 {
			result[id] = -1
		}
	}
	return result, nil
}
func TestEmptyAssignmentNeedsNoOffsetRequests(t *testing.T) {
	// Given a positioner with no assigned partitions.
	offsets := &countingOffsets{}
	p := NewPositionerFactory(&fixedClock{}, quietLogger(), NewOffsetLookupFactory()).New(offsets, &countingTimes{}, plan.Plan{}, "fp", "id")
	// When locating the empty assignment, then it needs no broker requests.
	got, err := p.Locate(t.Context(), nil, nil, nil)
	if err != nil || len(got.Assignments) != 0 || len(offsets.calls) != 0 {
		t.Fatal(got, err, offsets.calls)
	}
}
func TestPositioningStartRulesAndCommittedPositions(t *testing.T) {
	lower, upper := int64(1000), int64(2000)
	zero, negative := int64(0), int64(-1000)
	for _, scenario := range []struct {
		name   string
		start  string
		window plan.WindowSpec
		saved  map[int32]int64
		want   int64
		times  int
		fails  bool
	}{
		{name: "fresh earliest", start: "EARLIEST", want: 100},
		{name: "fresh latest uses stable end", start: "LATEST", want: 900},
		{name: "existing commit overrides latest", start: "LATEST", saved: map[int32]int64{1: 700}, want: 700},
		{name: "expired commit remaps to retained start", start: "LATEST", saved: map[int32]int64{1: 50}, want: 100},
		{name: "commit above log end fails", start: "EARLIEST", saved: map[int32]int64{1: 1001}, fails: true},
		{name: "new partition in existing group starts earliest", start: "LATEST", saved: map[int32]int64{0: 700}, want: 100},
		{name: "positive lower uses timestamp lookup", window: plan.WindowSpec{Lower: &plan.Expression{LiteralMS: lower}}, want: 500, times: 1},
		{name: "zero lower starts earliest", window: plan.WindowSpec{Lower: &plan.Expression{LiteralMS: zero}}, want: 100},
		{name: "negative lower starts earliest", window: plan.WindowSpec{Lower: &plan.Expression{LiteralMS: negative}}, want: 100},
		{name: "upper only starts earliest", window: plan.WindowSpec{Upper: &plan.Expression{LiteralMS: upper}}, want: 100},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			// Given retained offsets, an unstable tail, and optional committed progress.
			topic := "orders"
			offsets := &countingOffsets{low: 100, high: 1000, stable: 900}
			times := &countingTimes{}
			p := plan.Plan{Source: plan.Endpoint{Name: topic}, Start: scenario.start, Window: scenario.window}
			positioner := NewPositionerFactory(&fixedClock{now: time.Unix(100, 0)}, quietLogger(), NewOffsetLookupFactory()).New(offsets, times, p, "fp", "topic-id")

			// When a partition is assigned through the positioner interface.
			got, err := positioner.Locate(t.Context(), []int32{1}, scenario.saved, map[int32]Progress{})

			// Then the start rule is applied only in the absence of usable commits.
			if scenario.fails {
				var conflict *Conflict
				if !errors.As(err, &conflict) || len(got.Assignments) != 0 || times.calls != 0 || len(offsets.calls) != 2 {
					t.Fatalf("invalid commit fell back: result=%+v err=%v", got, err)
				}
				return
			}
			if err != nil || len(got.Assignments) != 1 || got.Assignments[0].Cursor.Position != scenario.want || times.calls != scenario.times {
				t.Fatalf("result=%+v lookups=%d err=%v", got, times.calls, err)
			}
		})
	}
}

func TestPositioningBatchesBrokerRequests(t *testing.T) {
	// Given
	topic := "orders"
	hour := int64(3600000)
	spec := plan.WindowSpec{Lower: &plan.Expression{Now: true, AdjustmentMS: -hour}}
	clock := &fixedClock{now: time.UnixMilli(10 * hour)}
	offsets := &countingOffsets{low: 100, high: 1000, stable: 900}
	times := &countingTimes{}
	positioner := NewPositionerFactory(clock, quietLogger(), NewOffsetLookupFactory()).New(offsets, times, plan.Plan{Source: plan.Endpoint{Name: topic}, Window: spec}, "fp", "topic-id")
	savedLower := 5 * hour
	saved := map[int32]int64{}
	metadata := map[int32]Progress{}
	parts := make([]int32, 200)
	for n := range parts {
		id := int32(n)
		parts[n] = id
		if n >= 150 {
			stored := int64(50) // Expired: below the log start.
			if n >= 175 {
				stored = 700
			}
			saved[id] = stored
			metadata[id] = Progress{Window: &plan.Window{Lower: &savedLower}}
		}
	}

	// When
	positioned, err := positioner.Locate(t.Context(), parts, saved, metadata)

	// Then
	require.NoError(t, err)
	if len(offsets.calls) != 3 || times.calls != 1 {
		t.Fatalf("broker requests: offsets=%v timestamp lookups=%d", offsets.calls, times.calls)
	}
	ids := make([]int32, len(positioned.Assignments))
	for i, a := range positioned.Assignments {
		ids[i] = a.Cursor.Partition
	}
	require.Equal(t, parts, ids, "every requested partition is returned once, in order")
	for _, a := range positioned.Assignments {
		id := a.Cursor.Partition
		want := int64(500)
		switch {
		case id >= 175:
			want = 700
		case id >= 150:
			want = 100
		case id%3 == 0:
			want = 900
		}
		if a.Cursor.Position != want {
			t.Fatalf("partition %d positioned at %d, want %d", id, a.Cursor.Position, want)
		}
		if *a.Window.Lower != savedLower {
			t.Fatalf("partition %d window %d did not adopt the lowest saved window", id, *a.Window.Lower)
		}
	}
}

type failedTimes struct {
	err    error
	result map[int32]int64
}

func (f failedTimes) OffsetsForTimes(context.Context, map[int32]int64) (map[int32]int64, error) {
	return f.result, f.err
}

type failedStableOffsets struct {
	countingOffsets
	err error
}

func (f *failedStableOffsets) Offsets(ctx context.Context, ids []int32, kind OffsetKind, stable bool) (map[int32]int64, error) {
	if stable {
		return nil, f.err
	}
	return f.countingOffsets.Offsets(ctx, ids, kind, stable)
}
func TestPositioningDoesNotFallbackAfterLookupFailure(t *testing.T) {
	for _, scenario := range []string{"timestamp RPC", "missing timestamp result", "stable end RPC"} {
		t.Run(scenario, func(t *testing.T) {
			// Given retained offsets and a failed or incomplete start-position lookup.
			failure := errors.New("lookup failed")
			offsets := &failedStableOffsets{countingOffsets: countingOffsets{low: 100, high: 1000}, err: failure}
			times := failedTimes{}
			p := plan.Plan{Window: plan.WindowSpec{Lower: &plan.Expression{LiteralMS: 1000}}}
			if scenario == "timestamp RPC" {
				times.err = failure
			}
			if scenario == "stable end RPC" {
				p.Window = plan.WindowSpec{}
				p.Start = "LATEST"
			}
			positioner := NewPositionerFactory(&fixedClock{}, quietLogger(), NewOffsetLookupFactory()).New(offsets, times, p, "fp", "id")
			// When locating the assignment.
			got, err := positioner.Locate(t.Context(), []int32{1}, nil, nil)
			// Then no usable position or initial progress escapes via a fallback.
			if err == nil || got.Assignments != nil || got.Progress != nil {
				t.Fatal(got, err)
			}
			if scenario != "missing timestamp result" && !errors.Is(err, failure) {
				t.Fatal("lookup cause lost", err)
			}
		})
	}
}
