package sql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPartitionPredicatesComposeWithTimestampBounds(t *testing.T) {
	for _, tc := range []struct {
		clause    string
		operators []string
	}{
		{"PARTITION IN (2, 0, 2)", nil},
		{"PARTITION NOT IN (2, 0, 2) AND RECORD_TIMESTAMP >= NOW()", []string{">="}},
		{"RECORD_TIMESTAMP >= NOW() AND PARTITION IN (2, 0, 2) AND RECORD_TIMESTAMP < NOW() + INTERVAL '1' HOUR", []string{">=", "<"}},
		{"RECORD_TIMESTAMP BETWEEN NOW() AND NOW() + INTERVAL '1' HOUR AND PARTITION IN (2, 0, 2)", []string{">=", "<="}},
	} {
		// Given a partition predicate alone or beside timestamp predicates.
		input := "INSERT INTO TOPIC 'b' SELECT * FROM TOPIC 'a' WHERE " + tc.clause
		// When parsing, then preserve literals and their positions for planning.
		s, err := NewParser().Parse(input)
		require.NoError(t, err)
		require.Equal(t, []PartitionID{{Position: Position{1, strings.Index(input, "2,") + 1}, Value: "2"}, {Position: Position{1, strings.Index(input, "0,") + 1}, Value: "0"}, {Position: Position{1, strings.Index(input, "2)") + 1}, Value: "2"}}, s.Partitions.IDs)
		require.Equal(t, strings.Contains(tc.clause, "NOT IN"), s.Partitions.Exclude)
		require.Len(t, s.Bounds, len(tc.operators))
		for i, bound := range s.Bounds {
			require.Equal(t, tc.operators[i], bound.Operator)
			require.True(t, bound.Time.Now)
			require.Empty(t, bound.Time.Literal)
			if i == 0 {
				require.Empty(t, bound.Time.Adjustments)
			} else {
				require.Len(t, bound.Time.Adjustments, 1)
				require.Equal(t, "1", bound.Time.Adjustments[0].Magnitude)
				require.Equal(t, "HOUR", bound.Time.Adjustments[0].Unit)
				require.False(t, bound.Time.Adjustments[0].Subtract)
			}
		}
	}
}

func TestPartitionSyntaxDiagnosticsLocateAndRedact(t *testing.T) {
	for _, tc := range []struct {
		tail   string
		column int
	}{
		{"IN ()", 21}, {"IN ('sensitive_SENTINEL')", 21}, {"IN (0,)", 23}, {"IN (-1)", 21},
		{"NOT (0)", 21}, {"= 0", 17}, {"IN (0) OR PARTITION IN (1)", 24}, {"IN (0) AND PARTITION NOT IN (1)", 28},
	} {
		// Given an unsupported predicate with a precisely located invalid token.
		query := "INSERT INTO TOPIC 'b' SELECT * FROM TOPIC 'a'\nWHERE PARTITION " + tc.tail
		// When parsing, then identify the full position without exposing values.
		_, err := NewParser().Parse(query)
		var d *Diagnostic
		require.ErrorAs(t, err, &d)
		require.Equal(t, Position{2, tc.column}, d.Position)
		require.NotContains(t, err.Error(), "sensitive_SENTINEL")
		if tc.column == 28 {
			require.Contains(t, err.Error(), "first declaration at line 2, column 7")
		}
	}
}
