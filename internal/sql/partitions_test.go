package sql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPartitionPredicatesComposeWithTimestampBounds(t *testing.T) {
	for _, clause := range []string{
		"PARTITION IN (2, 0, 2)",
		"PARTITION NOT IN (2, 0, 2) AND RECORD_TIMESTAMP >= NOW()",
		"RECORD_TIMESTAMP >= NOW() AND PARTITION IN (2, 0, 2) AND RECORD_TIMESTAMP < NOW() + INTERVAL '1' HOUR",
		"RECORD_TIMESTAMP BETWEEN NOW() AND NOW() + INTERVAL '1' HOUR AND PARTITION IN (2, 0, 2)",
	} {
		// Given a partition predicate alone or beside timestamp predicates.
		input := "INSERT INTO TOPIC 'b' SELECT * FROM TOPIC 'a' WHERE " + clause
		// When parsing, then preserve literals and their positions for planning.
		s, err := NewParser().Parse(input)
		require.NoError(t, err)
		require.Equal(t, []PartitionID{{Position: Position{1, strings.Index(input, "2,") + 1}, Value: "2"}, {Position: Position{1, strings.Index(input, "0,") + 1}, Value: "0"}, {Position: Position{1, strings.Index(input, "2)") + 1}, Value: "2"}}, s.Partitions.IDs)
		require.Equal(t, strings.Contains(clause, "NOT IN"), s.Partitions.Exclude)
	}
}

func TestPartitionSyntaxDiagnosticsLocateAndRedact(t *testing.T) {
	for _, tail := range []string{"IN ()", "IN ('sensitive_SENTINEL')", "IN (0,)", "IN (-1)", "NOT (0)", "= 0", "IN (0) OR PARTITION IN (1)", "IN (0) AND PARTITION NOT IN (1)"} {
		// Given an unsupported predicate with a precisely located invalid token.
		query := "INSERT INTO TOPIC 'b' SELECT * FROM TOPIC 'a'\nWHERE PARTITION " + tail
		// When parsing, then identify line two without exposing values.
		_, err := NewParser().Parse(query)
		var d *Diagnostic
		require.ErrorAs(t, err, &d)
		require.Equal(t, 2, d.Position.Line)
		require.NotContains(t, err.Error(), "sensitive_SENTINEL")
	}
	_, err := NewParser().Parse("INSERT INTO TOPIC 'b' SELECT * FROM TOPIC 'a'\nWHERE PARTITION IN ('sensitive_SENTINEL')")
	var d *Diagnostic
	require.ErrorAs(t, err, &d)
	require.Equal(t, Position{2, 21}, d.Position)
}
