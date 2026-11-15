//go:build integration && fault && load && harness

package compat

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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
