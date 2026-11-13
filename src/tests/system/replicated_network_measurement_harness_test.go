//go:build integration && fault && load && harness

package compat

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNetworkMeasurementRequiresCaughtUpSourceAndOutput(t *testing.T) {
	// Given positive healthy prefixes but fourteen past slots of debt on eight
	// partitions, a six-second catch-up burst inflates the reference above240/s.
	start := time.Unix(1, 0).UTC()
	before := networkMeasurementState(start, 1500*time.Millisecond, 15)
	for partition := range 8 {
		before.Saved[partition], before.Ends[partition] = 1, 1
		before.Publishing.Acknowledged[partition], before.Publishing.Offered[partition] = 1, 1
	}
	after := networkMeasurementState(start, 7500*time.Millisecond, 75)
	burst := networkInterval{Before: before, After: after}
	require.True(t, brokerHealthyCopy(before), "nonempty prefixes alone admit the old debt")
	require.InDelta(t, 258.6666666667, burst.rate(), 1e-9)
	require.Greater(t, .95*burst.rate(), 240.0, "a steady recovered240/s would fail the inflated reference")
	// When either measurement seeks its startpoint, reject old source debt and
	// destination backlog instead of truncating debt or changing the rate gate.
	require.False(t, networkMeasurementReady(before))
	clean := networkMeasurementState(start, 1500*time.Millisecond, 15)
	require.True(t, networkMeasurementReady(clean))
	clean.Publishing.Acknowledged[0]--
	clean.Ends[0]--
	clean.Saved[0]--
	require.True(t, networkMeasurementReady(clean), "one current slot may remain outstanding")
	clean.Publishing.Acknowledged[0]--
	require.False(t, networkMeasurementReady(clean), "an extra past slot must not enter either measurement")
	// Then even caught-up source counts cannot hide more than the existing24
	// records of output backlog; the healthy prefix and ISR proof still apply.
	backlog := networkMeasurementState(start, 7500*time.Millisecond, 75)
	backlog.Ends[0] -= 24
	backlog.Saved[0] -= 24
	require.True(t, networkMeasurementReady(backlog))
	backlog.Ends[0]--
	backlog.Saved[0]--
	require.False(t, networkMeasurementReady(backlog))
	backlog = networkMeasurementState(start, 7500*time.Millisecond, 75)
	backlog.Topology["source"][0].ISR = []int32{1, 2}
	require.False(t, networkMeasurementReady(backlog))
}

func TestNetworkHealthyPreconditionNeedsThirtySecondsAndBothRates(t *testing.T) {
	// Given the unchanged six-second session/recovery budget and steady input.
	start := time.Unix(1, 0).UTC()
	before := networkMeasurementState(start, time.Second, 10)
	after := networkMeasurementState(start, 31*time.Second, 310)
	window := networkInterval{Before: before, After: after}
	require.Equal(t, 6*time.Second, faultTimeouts(t, "network/latency")["session.timeout.ms"])
	// When the healthy reference spans thirty seconds, both independent rates
	// must meet the existing gate; a six-second sample cannot establish it.
	require.True(t, window.healthyPrecondition())
	short := networkInterval{Before: before, After: networkMeasurementState(start, 7*time.Second, 70)}
	require.False(t, short.healthyPrecondition())
	for _, values := range [][]int64{window.After.Ends, window.After.Publishing.Acknowledged} {
		for partition := range values {
			values[partition] -= 16
		}
		// Then even otherwise healthy output cannot hide underloaded input, or
		// the converse, and the thirty-second reference never changes recovery.
		require.False(t, window.healthyPrecondition())
		for partition := range values {
			values[partition] += 16
		}
	}
}

func TestNetworkDestinationLeadersRemainStableAcrossPhases(t *testing.T) {
	// Given fully replicated destinations before and after a worker-only fault.
	start := time.Unix(1, 0).UTC()
	healthy := networkMeasurementState(start, time.Second, 10)
	during := networkMeasurementState(start, 7*time.Second, 70)
	recovered := networkMeasurementState(start, 13*time.Second, 130)
	require.True(t, networkDestinationLeadersStable(healthy, during))
	require.True(t, networkDestinationLeadersStable(during, recovered))
	// When a leader changes while ISR remains complete, then the rate and RF3
	// checks alone cannot qualify that different destination topology.
	during.Topology["destination"][7].Leader = 2
	require.True(t, fullyReplicated(during, "destination"))
	require.False(t, networkDestinationLeadersStable(healthy, during))
	require.False(t, networkDestinationLeadersStable(during, recovered))
}
