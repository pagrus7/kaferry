//go:build integration && fault && load && harness

package compat

import (
	"testing"
)

func TestBrokerReadinessRequiresFreshPinnedMetadataWithinSharedBudget(t *testing.T) {
	// Given a running owned broker, when its fresh API response is absent or
	// invalid, then a running container and a healthy peer cannot satisfy readiness.
	for _, mode := range []string{"running", "ISR1", "worker route blocked", "stopped", "missing running", "missing start", "new start", "wrong service", "wrong owner", "wrong container", "malformed Docker", "API error", "wrong cluster", "wrong routes", "missing topic", "bad partition", "blocked target", "cancelled", "shared deadline"} {
		t.Run(mode, func(t *testing.T) {
			testBrokerReadiness(t, mode)
		})
	}
}
