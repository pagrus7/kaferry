package copy

import (
	"math"
	"testing"

	"github.com/pagrus7/kaferry/src/internal/plan"
	"github.com/stretchr/testify/require"
)

//kaferry:guarantee completion.timestamp
func TestPartitionTimestampThreshold(t *testing.T) {
	for _, tc := range []struct {
		name  string
		stamp *int64
		upper *int64
		stop  bool
		fail  bool
	}{
		{"no timestamp", nil, new(int64(1000)), false, false},
		{"unbounded", new(int64(2000)), nil, false, false},
		{"below", new(int64(1099)), new(int64(1000)), false, false},
		{"equal", new(int64(1100)), new(int64(1000)), false, false},
		{"above", new(int64(1101)), new(int64(1000)), true, false},
		{"overflow", new(int64(math.MaxInt64)), new(int64(math.MaxInt64)), false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given a record timestamp and a window with a 100ms disorder allowance.
			p := partition{window: plan.Window{Upper: tc.upper}}

			// When checking whether this record triggers the timestamp rule.
			stop, err := p.pastTimestamp(tc.stamp, 100)

			// Then the rule is strict, checked for overflow, and does not mutate state.
			require.Equal(t, tc.stop, stop)
			require.Equal(t, tc.fail, err != nil, "timestamp check error: %v", err)
			require.Nil(t, p.stop)
			require.Zero(t, p.position)
		})
	}
}
