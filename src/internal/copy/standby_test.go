package copy

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Follow-mode standbys and quiet partitions stay up; only a signal ends them.
func TestFollowStandbySurvivesLongQuiet(t *testing.T) {
	// Given
	for _, assigned := range []bool{false, true} {
		r, s, _, _, c := fixture(t)
		r.plan.Source.Kind = "TOPIC"
		if !assigned {
			s.assignments = nil
		}
		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)
		start := c.now
		c.tick = func() {
			if c.now.Sub(start) > 10*time.Minute {
				cancel()
			}
		}

		// When
		runErr := r.Run(ctx)

		// Then
		require.NoError(t, runErr, "assigned=%v", assigned)
		require.Greater(t, c.now.Sub(start), 10*time.Minute, "follow worker exited before cancellation: assigned=%v", assigned)
		require.ErrorIs(t, ctx.Err(), context.Canceled)
	}
}
