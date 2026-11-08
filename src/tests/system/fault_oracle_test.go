//go:build integration && fault

package compat

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRebalanceChurnTimeoutCatalogUsesMilliseconds(t *testing.T) {
	// Given each registered tier, when configuring a fixture, then both the
	// scenario and its independent observer use the selected millisecond budgets.
	for _, tier := range []struct {
		name                        string
		session, delivery, shutdown time.Duration
	}{
		{"full", 45 * time.Second, 60 * time.Second, 30 * time.Second},
		{"pr", 6 * time.Second, 5 * time.Second, 3 * time.Second},
	} {
		t.Run(tier.name, func(t *testing.T) {
			t.Setenv("KAFERRY_FAULT_TIER", tier.name)
			churn := faultTimeouts(t, "fault/churn")
			require.Equal(t, tier.session, churn["session.timeout.ms"])
			require.Equal(t, tier.delivery, churn["delivery_timeout"])
			require.Equal(t, tier.shutdown, churn["shutdown_timeout"])
			require.Equal(t, 2*time.Second, faultTimeouts(t, "recovery/delivery-timeout")["delivery_timeout"])
		})
	}
}
