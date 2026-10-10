package kafka

import (
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
