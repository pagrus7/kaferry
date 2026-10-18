package copy

import (
	"math"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
)

func TestPartitionTimestampThreshold(t *testing.T) {
	for _, tc := range []struct {
		name  string
		stamp *int64
		upper *int64
		stop  bool
		fail  bool
	}{
		{"no timestamp", nil, new(int64(1000)), false, false},
		{"unbounded", new(int64(2000)), nil, false, false},
		{"below", new(int64(1099)), new(int64(1000)), false, false},
		{"equal", new(int64(1100)), new(int64(1000)), false, false},
		{"above", new(int64(1101)), new(int64(1000)), true, false},
		{"overflow", new(int64(math.MaxInt64)), new(int64(math.MaxInt64)), false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given a record timestamp and a window with a 100ms cutoff.
			p := partition{window: plan.Window{Upper: tc.upper}}

			// When checking whether this record triggers the timestamp rule.
			stop, err := p.pastTimestamp(tc.stamp, 100)

			// Then the rule is strict, checked for overflow, and does not mutate state.
			require.Equal(t, tc.stop, stop)
			require.Equal(t, tc.fail, err != nil, "timestamp check error: %v", err)
			require.Nil(t, p.stop)
			require.Zero(t, p.position)
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
		require.NoError(t, err)
		require.Equal(t, want, got)
	}

	// When the readable tip is reached exactly at the threshold, equality still waits.
	assertAction(tipWait)
	p.reachedTip(0, false)
	assertAction(tipWait)
	// When more input clears the tip and wall time passes the threshold, it still waits.
	p.atTip = false
	now = now.Add(time.Millisecond)
	assertAction(tipWait)
	// Then sampling still waits until the tip is reached at the same wall time.
	p.reachedTip(0, false)
	assertAction(tipSample)
	p.sampling.failed(now)
	assertAction(tipWait)
	now = now.Add(time.Second)
	assertAction(tipSample)

	// Then a successful sample fixes the end and later input cannot request another.
	p.sampled(3, now)
	p.reachedTip(1, false)
	assertAction(tipWait)
	require.False(t, p.reachedHigh(2))
	require.True(t, p.reachedHigh(3))
	require.True(t, p.reachedHigh(100))
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
	require.NoError(t, err)
	afterDeadline, err := p.tipAction(now.Add(time.Second+time.Nanosecond), 0, time.Second)
	require.NoError(t, err)
	p.position = 3
	completed, err := p.tipAction(now.Add(2*time.Second), 0, time.Second)

	// Then equality keeps waiting, timeout is strict, and a reached end completes.
	require.NoError(t, err)
	require.Equal(t, tipWait, atDeadline)
	require.Equal(t, tipTimeout, afterDeadline)
	require.Equal(t, tipStop, completed)
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
	require.NoError(t, err)
	require.Equal(t, tipStop, action)
	require.Equal(t, &Stop{Reason: "eof", At: 80}, file.stop)
	require.True(t, file.atTip)
	require.Equal(t, int64(80), file.position)
	require.Equal(t, int64(80), file.stable)
}
