package copy

import (
	"context"
	"testing"
	"time"
)

// Follow-mode standbys and quiet partitions stay up; only a signal ends them.
func TestFollowStandbySurvivesLongQuiet(t *testing.T) {
	for _, assigned := range []bool{false, true} {
		r, s, _, _, c := fixture(t)
		r.plan.Source.Kind = "TOPIC"
		if !assigned {
			s.assignments = nil
		}
		ctx, cancel := context.WithCancel(context.Background())
		start := c.now
		c.tick = func() {
			if c.now.Sub(start) > 10*time.Minute {
				cancel()
			}
		}
		if err := r.Run(ctx); err != nil {
			t.Fatalf("assigned=%v: follow worker failed during quiet: %v", assigned, err)
		}
	}
}
