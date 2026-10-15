package copy

import (
	"math"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/plan"
)

func TestPartitionTimestampThreshold(t *testing.T) {
	for _, tc := range []struct {
		name  string
		stamp *int64
		upper *int64
		stop  bool
		fail  bool
	}{
		{"no timestamp", nil, ptr(int64(1000)), false, false},
		{"unbounded", ptr(int64(2000)), nil, false, false},
		{"below", ptr(int64(1099)), ptr(int64(1000)), false, false},
		{"equal", ptr(int64(1100)), ptr(int64(1000)), false, false},
		{"above", ptr(int64(1101)), ptr(int64(1000)), true, false},
		{"overflow", ptr(int64(math.MaxInt64)), ptr(int64(math.MaxInt64)), false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given a record timestamp and a window with a 100ms cutoff.
			p := partition{window: plan.Window{Upper: tc.upper}}

			// When checking whether this record triggers the timestamp rule.
			stop, err := p.pastTimestamp(tc.stamp, 100)

			// Then the rule is strict, checked for overflow, and does not mutate state.
			if stop != tc.stop || (err != nil) != tc.fail || p.stopped || p.position != 0 {
				t.Fatal(stop, err, p)
			}
		})
	}
}

func TestPartitionSamplesOnlyAfterThresholdAndRetryDeadline(t *testing.T) {
	// Given a bounded partition at the time threshold, but not at its readable tip.
	upper := int64(1000)
	p := partition{window: plan.Window{Upper: &upper}}
	now := time.UnixMilli(1100)
	assertAction := func(want tipAction) {
		t.Helper()
		got, err := p.tipAction(now, 100, time.Second)
		if err != nil || got != want {
			t.Fatal(got, want, err)
		}
	}

	// When the tip is reached exactly at the threshold, then wall time advances.
	assertAction(tipWait)
	p.reachedTip(0, false)
	assertAction(tipWait)
	now = now.Add(time.Millisecond)
	assertAction(tipSample)
	p.sampling.failed(now)
	assertAction(tipWait)
	now = now.Add(time.Second)
	assertAction(tipSample)

	// Then a successful sample fixes the end and later input cannot request another.
	p.sampled(3, now)
	p.reachedTip(1, false)
	assertAction(tipWait)
	if p.reachedHigh(2) || !p.reachedHigh(3) || !p.reachedHigh(100) {
		t.Fatal("fixed end was not respected")
	}
	p.position = 3
	assertAction(tipStop)
}

func TestPartitionTipTimeoutIsStrictAndCompletionWins(t *testing.T) {
	// Given an unresolved sampled end and a one-second wait budget.
	upper := int64(1000)
	p := partition{window: plan.Window{Upper: &upper}}
	now := time.UnixMilli(2000)
	p.sampled(3, now)

	// When the wait budget is exactly reached, then exceeded.
	atDeadline, err := p.tipAction(now.Add(time.Second), 0, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	afterDeadline, err := p.tipAction(now.Add(time.Second+time.Nanosecond), 0, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	p.position = 3
	completed, err := p.tipAction(now.Add(2*time.Second), 0, time.Second)

	// Then equality keeps waiting, timeout is strict, and a reached end completes.
	if err != nil || atDeadline != tipWait || afterDeadline != tipTimeout || completed != tipStop {
		t.Fatal(atDeadline, afterDeadline, completed, err)
	}
}

func TestPartitionEmptyAndFileEnds(t *testing.T) {
	// Given an empty Kafka partition and a file input with a captured final size.
	upper := int64(1000)
	kafka := partition{window: plan.Window{Upper: &upper}}
	file := partition{}
	now := time.UnixMilli(2000)

	// When the empty log end is sampled and the file reaches its final cursor.
	kafka.sampled(0, now)
	action, err := kafka.tipAction(now, 0, time.Minute)
	file.reachedTip(80, true)

	// Then Kafka can stop immediately and file EOF stops at its captured position.
	if err != nil || action != tipStop || !file.stopped || !file.atTip || file.position != 80 || file.stable != 80 {
		t.Fatal(action, err, file)
	}
}

func ptr[T any](value T) *T { return &value }
