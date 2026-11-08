//go:build integration && load

package compat

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// This fixed workload exercises a single process over many active partitions.
// It is deliberately separate from the short CI soak and saturation tests.
func TestLoadEnduranceRejectsUnderloadedRuns(t *testing.T) {
	// Given the requested minute includes 12,000 base records and a 400-record burst.
	const duration = time.Minute
	for _, counts := range []struct {
		name                    string
		submitted, acknowledged int
	}{
		{"producer", 12400, 11000}, {"verifier-throttled-input", 11000, 11000},
	} {
		t.Run(counts.name, func(t *testing.T) {
			e := newEnduranceEvidence(duration)
			// When backpressure lowers input, then fidelity alone cannot qualify the run.
			require.ErrorContains(t, e.check(duration+5*time.Second, duration, counts.submitted, counts.acknowledged), "load invalid")
			require.False(t, e.LoadValidated)
		})
	}
	// A producer/verifier pause must also fail before the first minute ends.
	e := newEnduranceEvidence(duration)
	require.ErrorContains(t, e.check(8*time.Second, duration, 0, 0), "scheduling debt")
	// An almost complete final window still cannot drop its tail.
	e = newEnduranceEvidence(duration)
	require.NoError(t, e.check(duration, duration, 12399, 12399))
	require.ErrorContains(t, e.check(duration+5*time.Second, duration, 12399, 12399), "all scheduled input")
}

func TestLoadEnduranceAllowsFinalBurstToSettleDuringGrace(t *testing.T) {
	// Given the last 400-record burst is due exactly at a 70-second publishing boundary.
	duration := 70 * time.Second
	e := newEnduranceEvidence(duration)
	require.NoError(t, e.check(time.Minute, duration, 12400, 12400))
	// When that burst has not yet been submitted, then the final window waits for grace.
	require.NoError(t, e.check(duration, duration, 14400, 14400))
	require.False(t, e.LoadValidated)
	require.Equal(t, 1, e.Windows)
	// Then the last burst must be fully acknowledged before the run can qualify.
	require.NoError(t, e.check(duration+5*time.Second, duration, 14800, 14800))
	require.True(t, e.LoadValidated)
	require.Equal(t, 2, e.Windows)
	require.Equal(t, 1.0, e.MinWindowRatio)
}

func TestLoadEnduranceTracksBurstsWindowsAndFinalAcknowledgements(t *testing.T) {
	// Given an absolute schedule with bursts at 10 and 70 seconds.
	duration := 90 * time.Second
	e := newEnduranceEvidence(duration)
	require.Equal(t, 1800, enduranceScheduled(9*time.Second))
	require.Equal(t, 2400, enduranceScheduled(10*time.Second))
	require.Equal(t, 18800, e.TargetRecords)
	// When producer acknowledgement lags briefly, then the grace period can settle the tail.
	require.NoError(t, e.check(time.Minute, duration, 12400, 12398))
	require.NoError(t, e.check(duration, duration, 18799, 18798))
	require.False(t, e.LoadValidated)
	require.NoError(t, e.check(duration+5*time.Second, duration, 18800, 18800))
	// Then both windows and all scheduled input have evidence independent of output fidelity.
	require.True(t, e.LoadValidated)
	require.Equal(t, 2, e.Windows)
	require.InDelta(t, 12398.0/12400, e.MinWindowRatio, 1e-9)
}
