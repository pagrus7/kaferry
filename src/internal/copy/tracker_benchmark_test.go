package copy

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkTrackerBatch(b *testing.B) {
	for _, size := range []int{1, 5000, 20000} {
		b.Run(fmt.Sprintf("records=%d", size), func(b *testing.B) {
			// Given one partition and reusable delivery tokens.
			tokens := make([]Token, size)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				tracker := NewTracker()
				tracker.Assign(Cursor{})
				// When a batch is admitted and deliveries arrive in reverse order.
				for i := range tokens {
					token, err := tracker.Observe(Cursor{Position: int64(i)}, int64(i+1), 1024)
					require.NoError(b, err)

					tokens[i] = token
					tracker.Submitted(token)
				}
				for i := len(tokens) - 1; i >= 0; i-- {
					tracker.Settle(tokens[i], true)
				}
				// Then save the confirmed prefix and release its accounting.
				tracker.Committed(tracker.Confirmed(0))
			}
		})
	}
}
