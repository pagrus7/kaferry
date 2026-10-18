package sql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLimitSyntaxAndLocation(t *testing.T) {
	// Given a limit after a time filter, with comments between its tokens.
	query := "INSERT INTO FILE cbor('out') SELECT * FROM FILE cbor('in')\n" +
		"WHERE RECORD_TIMESTAMP >= NOW()\nlimit /* sample */ 005 OPTIONS(GROUP='g')"
	// When parsing without accessing endpoints.
	statement, err := NewParser().Parse(query)
	// Then preserve the number and its position for planner diagnostics.
	require.NoError(t, err)
	require.Equal(t, "005", statement.Limit)
	require.Equal(t, Position{Line: 3, Column: 20}, statement.LimitPosition)
}

func TestLimitSyntaxErrorsAreLocatedAndRedacted(t *testing.T) {
	const prefix = "INSERT INTO FILE cbor('out')\nSELECT * FROM FILE cbor('in')\nLIMIT "
	for _, value := range []string{"", "'private-value'", "private_identifier", "-1", "+1"} {
		t.Run(value, func(t *testing.T) {
			// Given a missing or non-integer limit.
			// When parsing the statement.
			_, err := NewParser().Parse(prefix + value)
			// Then point at the limit value and explain the expected category.
			var diagnostic *Diagnostic
			require.ErrorAs(t, err, &diagnostic)
			require.Equal(t, Position{Line: 3, Column: 7}, diagnostic.Position)
			require.Contains(t, diagnostic.Err.Error(), "expected integer")
			require.NotContains(t, err.Error(), "private")
		})
	}
	for _, fragment := range []string{"1.5", "1e3", "5 LIMIT 6"} {
		// Given a malformed number or a repeated clause.
		// When parsing, then reject it rather than truncate its meaning.
		_, err := NewParser().Parse(prefix + fragment + " OPTIONS(GROUP='g')")
		require.Error(t, err)
	}
}
