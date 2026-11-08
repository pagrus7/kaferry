//go:build integration && fault

package compat

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRebalanceChurnTimeoutCatalogUsesMilliseconds(t *testing.T) {
	// Given the shared millisecond catalog used by both Go fixtures and Python's
	// outer allowance, when reading it, then preserve the existing real deadlines.
	churn := faultTimeouts(t, "fault/churn")
	require.Equal(t, 45*time.Second, churn["session.timeout.ms"])
	require.Equal(t, 60*time.Second, churn["delivery_timeout"])
	require.Equal(t, 30*time.Second, churn["shutdown_timeout"])
	require.Equal(t, 2*time.Second, faultTimeouts(t, "recovery/delivery-timeout")["delivery_timeout"])
	require.Equal(t, 60*time.Second, faultTimeouts(t, "recovery/source-outage-shutdown")["delivery_timeout"])
}
