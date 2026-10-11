package kafka

import (
	"errors"
	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pagrus7/kaferry/internal/plan"
	"reflect"
	"testing"
	"time"
)

func TestSavedMixedWindowSurvivesClockAdvanceAndAdoption(t *testing.T) {
	for _, origin := range []string{"saved", "adopted", "fresh"} {
		t.Run(origin, func(t *testing.T) {
			// Given a mixed NOW/literal window whose fresh resolution is now empty.
			hi, lo := int64(7200000), int64(3600000)
			p := &positioner{plan: plan.Plan{Window: plan.WindowSpec{Lower: &plan.Expression{Now: true, AdjustmentMS: -3600000}, Upper: &plan.Expression{LiteralMS: hi}}}, log: quietLogger()}
			saved, metadata := map[int32]k.TopicPartition{}, map[int32]Progress{}
			var adopted *plan.Window
			window := plan.Window{Lower: &lo, Upper: &hi}
			if origin == "saved" {
				saved[0] = k.TopicPartition{Offset: 42}
				metadata[0] = Progress{Window: &window}
			}
			if origin == "adopted" {
				adopted = &window
			}

			// When positioning after both the saved upper bound and its cutoff.
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

func TestPositioningStartRulesAndCommittedPositions(t *testing.T) {
	lower, upper := int64(1000), int64(2000)
	zero, negative := int64(0), int64(-1000)
	for _, scenario := range []struct {
		name   string
		start  string
		window plan.WindowSpec
		saved  map[int32]k.TopicPartition
		want   int64
		times  int
		fails  bool
	}{
		{name: "fresh earliest", start: "EARLIEST", want: 100},
		{name: "fresh latest uses stable end", start: "LATEST", want: 900},
		{name: "existing commit overrides latest", start: "LATEST", saved: map[int32]k.TopicPartition{1: {Offset: 700}}, want: 700},
		{name: "expired commit remaps to retained start", start: "LATEST", saved: map[int32]k.TopicPartition{1: {Offset: 50}}, want: 100},
		{name: "commit above log end fails", start: "EARLIEST", saved: map[int32]k.TopicPartition{1: {Offset: 1001}}, fails: true},
		{name: "new partition in existing group starts earliest", start: "LATEST", saved: map[int32]k.TopicPartition{0: {Offset: 700}}, want: 100},
		{name: "positive lower uses timestamp lookup", window: plan.WindowSpec{Lower: &plan.Expression{LiteralMS: lower}}, want: 500, times: 1},
		{name: "zero lower starts earliest", window: plan.WindowSpec{Lower: &plan.Expression{LiteralMS: zero}}, want: 100},
		{name: "negative lower starts earliest", window: plan.WindowSpec{Lower: &plan.Expression{LiteralMS: negative}}, want: 100},
		{name: "upper only starts earliest", window: plan.WindowSpec{Upper: &plan.Expression{LiteralMS: upper}}, want: 100},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			// Given retained offsets, an unstable tail, and optional committed progress.
			topic := "orders"
			offsets := &countingOffsets{low: 100, high: 1000, stable: 900}
			times := &countingTimes{topic: topic}
			p := plan.Plan{Source: plan.Endpoint{Name: topic}, Start: scenario.start, Window: scenario.window}
			positioner := NewPositionerFactory(&fixedClock{now: time.Unix(100, 0)}, quietLogger()).New(offsets, times, p, "fp", "topic-id")

			// When a partition is assigned through the positioner interface.
			got, err := positioner.Locate([]k.TopicPartition{{Topic: &topic, Partition: 1}}, scenario.saved, map[int32]Progress{})

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
	times := &countingTimes{topic: topic}
	positioner := NewPositionerFactory(clock, quietLogger()).New(offsets, times, plan.Plan{Source: plan.Endpoint{Name: topic}, Window: spec}, "fp", "topic-id")
	savedLower := 5 * hour
	saved := map[int32]k.TopicPartition{}
	metadata := map[int32]Progress{}
	parts := make([]k.TopicPartition, 200)
	for n := range parts {
		id := int32(n)
		parts[n] = k.TopicPartition{Topic: &topic, Partition: id}
		if n >= 150 {
			stored := int64(50) // Expired: below the log start.
			if n >= 175 {
				stored = 700
			}
			saved[id] = k.TopicPartition{Topic: &topic, Partition: id, Offset: k.Offset(stored)}
			metadata[id] = Progress{Window: &plan.Window{Lower: &savedLower}}
		}
	}

	// When
	positioned, err := positioner.Locate(parts, saved, metadata)

	// Then
	if err != nil {
		t.Fatal(err)
	}
	if len(offsets.calls) != 3 || times.calls != 1 {
		t.Fatalf("broker requests: offsets=%v timestamp lookups=%d", offsets.calls, times.calls)
	}
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

func TestEmptyAssignmentNeedsNoOffsetRequests(t *testing.T) {
	// Given
	offsets := &countingOffsets{}
	s := &Source{offsets: offsets}

	// When
	assignments, err := s.locate(nil, nil, nil)

	// Then
	if err != nil || len(assignments) != 0 || len(offsets.calls) != 0 {
		t.Fatalf("empty assignment: assignments=%v requests=%v err=%v", assignments, offsets.calls, err)
	}
}
