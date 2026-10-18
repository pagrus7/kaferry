package plan

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWindowResolve(t *testing.T) {
	cases := []struct {
		name                 string
		lower, upper         Expression
		wantLower, wantUpper int64
		wantError            string
	}{
		{"positive adjustments", Expression{LiteralMS: 100, AdjustmentMS: 10}, Expression{LiteralMS: 200, AdjustmentMS: 20}, 110, 220, ""},
		{"negative adjustments", Expression{LiteralMS: 100, AdjustmentMS: -10}, Expression{LiteralMS: 200, AdjustmentMS: -20}, 90, 180, ""},
		{"one millisecond", Expression{}, Expression{LiteralMS: 1}, 0, 1, ""},
		{"maximum without overflow", Expression{LiteralMS: math.MaxInt64 - 2, AdjustmentMS: 1}, Expression{LiteralMS: math.MaxInt64 - 1, AdjustmentMS: 1}, math.MaxInt64 - 1, math.MaxInt64, ""},
		{"minimum without overflow", Expression{LiteralMS: math.MinInt64 + 1, AdjustmentMS: -1}, Expression{LiteralMS: math.MinInt64 + 2, AdjustmentMS: -1}, math.MinInt64, math.MinInt64 + 1, ""},
		{"spanning integer extremes", Expression{LiteralMS: math.MinInt64}, Expression{LiteralMS: math.MaxInt64}, math.MinInt64, math.MaxInt64, ""},
		{"equal bounds", Expression{LiteralMS: 100}, Expression{LiteralMS: 100}, 0, 0, "empty"},
		{"reversed bounds", Expression{LiteralMS: 200}, Expression{LiteralMS: 100}, 0, 0, "empty"},
		{"equal after adjustment", Expression{LiteralMS: 100, AdjustmentMS: 100}, Expression{LiteralMS: 200}, 0, 0, "empty"},
		{"reversed after adjustment", Expression{LiteralMS: 100, AdjustmentMS: 101}, Expression{LiteralMS: 200}, 0, 0, "empty"},
		{"lower positive overflow", Expression{LiteralMS: math.MaxInt64, AdjustmentMS: 1}, Expression{LiteralMS: math.MaxInt64}, 0, 0, "overflows"},
		{"upper positive overflow", Expression{}, Expression{LiteralMS: math.MaxInt64, AdjustmentMS: 1}, 0, 0, "overflows"},
		{"lower negative overflow", Expression{LiteralMS: math.MinInt64, AdjustmentMS: -1}, Expression{}, 0, 0, "overflows"},
		{"upper negative overflow", Expression{LiteralMS: math.MinInt64}, Expression{LiteralMS: math.MinInt64, AdjustmentMS: -1}, 0, 0, "overflows"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Given independently adjustable lower and upper timestamp bounds.
			spec := WindowSpec{Lower: &tc.lower, Upper: &tc.upper}

			// When resolving the window.
			window, err := spec.Resolve(time.Time{})

			// Then bounds resolve exactly; empty or overflowing ranges fail distinctly.
			if tc.wantError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantError)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, window.Lower)
			require.NotNil(t, window.Upper)
			require.Equal(t, tc.wantLower, *window.Lower)
			require.Equal(t, tc.wantUpper, *window.Upper)
			// Then valid windows include their first and last millisecond, excluding the upper bound.
			require.True(t, window.Contains(tc.wantLower))
			require.True(t, window.Contains(tc.wantUpper-1))
			require.False(t, window.Contains(tc.wantUpper))
			require.False(t, tc.wantLower > math.MinInt64 && window.Contains(tc.wantLower-1))
		})
	}
}
