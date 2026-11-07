package sql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBetweenPredicatesKeepBoundsAndPositions(t *testing.T) {
	// Given partition, offset and timestamp ranges in the same statement.
	query := "INSERT INTO TOPIC 'b'\nSELECT * FROM TOPIC 'a'\n" +
		"WHERE PARTITION NOT BETWEEN 2 AND 4\n" +
		"  AND RECORD_OFFSET BETWEEN 100 AND 199\n" +
		"  AND RECORD_TIMESTAMP BETWEEN NOW() AND NOW() + INTERVAL '1' HOUR"
	// When parsing, then each BETWEEN consumes its own AND and preserves locations.
	s, err := NewParser().Parse(query)
	require.NoError(t, err)
	require.True(t, s.Partitions.Exclude)
	require.Equal(t, &PartitionRange{
		Lower: PartitionID{Position: Position{3, 29}, Value: "2"},
		Upper: PartitionID{Position: Position{3, 35}, Value: "4"},
	}, s.Partitions.Range)
	require.Equal(t, []OffsetBound{
		{Position: Position{4, 21}, ValuePosition: Position{4, 29}, Operator: ">=", Value: "100"},
		{Position: Position{4, 37}, ValuePosition: Position{4, 37}, Operator: "<=", Value: "199"},
	}, s.OffsetBounds)
	require.Len(t, s.Bounds, 2)
	require.Equal(t, ">=", s.Bounds[0].Operator)
	require.Equal(t, "<=", s.Bounds[1].Operator)
}

func TestBetweenSyntaxDiagnosticsLocateAndRedact(t *testing.T) {
	const prefix = "WITH CONNECTION default AS ('sasl.password'='private_SENTINEL')\n" +
		"INSERT INTO TOPIC 'b' SELECT * FROM TOPIC 'a'\n"
	for _, tc := range []struct{ clause, marker string }{
		{"WHERE RECORD_OFFSET BETWEEN secret_LOW AND 9", "secret_LOW"},
		{"WHERE RECORD_OFFSET BETWEEN 1 AND 'secret_HIGH'", "'secret_HIGH'"},
		{"WHERE RECORD_OFFSET BETWEEN 1 secret_AND 9", "secret_AND"},
		{"WHERE RECORD_OFFSET BETWEEN 1 AND LATEST", "LATEST"},
		{"WHERE RECORD_OFFSET BETWEEN -1 AND 9", "-1"},
		{"WHERE PARTITION BETWEEN 'secret_LOW' AND 2", "'secret_LOW'"},
		{"WHERE PARTITION NOT BETWEEN 0 AND secret_HIGH", "secret_HIGH"},
		{"WHERE PARTITION BETWEEN 0 secret_AND 2", "secret_AND"},
		{"WHERE PARTITION BETWEEN 0 AND ;", ";"},
		{"WHERE PARTITION BETWEEN 0 AND -1", "-1"},
	} {
		t.Run(tc.clause, func(t *testing.T) {
			// Given malformed range syntax beside a sensitive connection literal.
			// When parsing, then report the offending token without exposing input.
			_, err := NewParser().Parse(prefix + tc.clause)
			var diagnostic *Diagnostic
			require.ErrorAs(t, err, &diagnostic)
			require.Equal(t, Position{3, strings.Index(tc.clause, tc.marker) + 1}, diagnostic.Position)
			require.NotContains(t, err.Error(), "secret_")
			require.NotContains(t, err.Error(), "private_SENTINEL")
		})
	}
}
