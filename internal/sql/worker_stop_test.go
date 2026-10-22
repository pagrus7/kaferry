package sql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWorkerStopSyntaxAndLocation(t *testing.T) {
	// Given a worker stop option after a time filter, with a comment before its value.
	query := "INSERT INTO FILE avro('out') SELECT * FROM FILE avro('in')\n" +
		"WHERE RECORD_TIMESTAMP >= NOW()\nOPTIONS(GROUP='g', worker_stop_after_records=/* sample */ 005)"
	// When parsing without accessing endpoints.
	statement, err := NewParser().Parse(query)
	// Then preserve the unquoted number and its position for planner diagnostics.
	require.NoError(t, err)
	option := statement.Options[1]
	require.Equal(t, "WORKER_STOP_AFTER_RECORDS", option.Name)
	require.Equal(t, "005", option.Value)
	require.Equal(t, Position{Line: 3, Column: 59}, option.ValuePosition)
}

func TestWorkerStopSyntaxErrorsAreLocatedAndRedacted(t *testing.T) {
	const prefix = "INSERT INTO FILE avro('out')\nSELECT * FROM FILE avro('in')\nOPTIONS(GROUP='g',WORKER_STOP_AFTER_RECORDS="
	for _, value := range []string{"", "'private-value'", "private_identifier", "-1", "+1"} {
		t.Run(value, func(t *testing.T) {
			// Given a missing or non-integer worker stop value.
			// When parsing the statement.
			_, err := NewParser().Parse(prefix + value)
			// Then point at the value and explain the expected category without input values.
			var diagnostic *Diagnostic
			require.ErrorAs(t, err, &diagnostic)
			require.Equal(t, Position{Line: 3, Column: 45}, diagnostic.Position)
			require.Contains(t, diagnostic.Err.Error(), "expected integer")
			require.NotContains(t, err.Error(), "private")
		})
	}
	for _, value := range []string{"1.5", "1e3"} {
		// Given a malformed number, when parsing, then reject it rather than truncate it.
		_, err := NewParser().Parse(prefix + value + ")")
		require.Error(t, err)
	}
	// Given query-level LIMIT syntax, when parsing, then require the explicit worker option instead.
	_, err := NewParser().Parse("INSERT INTO FILE avro('out') SELECT * FROM FILE avro('in') LIMIT 5 OPTIONS(GROUP='g')")
	require.ErrorContains(t, err, "expected WHERE or OPTIONS")
}
