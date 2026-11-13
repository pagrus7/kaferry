//go:build integration && fault && load && harness

package compat

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBrokerWorkCapsRequireActualSamplesAndKeepMissingPeaksUnavailable(t *testing.T) {
	// Given a real progress sample, when no final summary exists, then observed
	// work remains bounded and the missing lifetime highwater stays unavailable.
	work, err := brokerWork(brokerWorkProgress)
	require.NoError(t, err)
	require.Nil(t, work.Summary)
	require.Equal(t, 2, work.SampledPending)
	// When the final summary exists, then its exact peaks must cover the samples
	// and remain within the configured record and byte caps.
	work, err = brokerWork(brokerWorkProgress + brokerWorkSummary)
	require.NoError(t, err)
	require.Equal(t, 3, work.Summary.PeakRecords)
	for _, invalid := range []string{
		brokerWorkSummary,
		strings.ReplaceAll(brokerWorkProgress, `"pending_entries":2`, `"pending_entries":1025`),
		strings.ReplaceAll(brokerWorkProgress, `"pending_estimated_bytes":4096`, `"pending_estimated_bytes":134217729`),
		brokerWorkProgress + strings.ReplaceAll(brokerWorkSummary, `"peak_uncommitted_records":3`, `"peak_uncommitted_records":1`),
		brokerWorkProgress + strings.ReplaceAll(brokerWorkSummary, `"peak_uncommitted_bytes":8192`, `"peak_uncommitted_bytes":null`),
	} {
		_, err := brokerWork(invalid)
		require.Error(t, err)
	}
}
