package plan

import (
	"strings"
	"testing"

	"github.com/pagrus7/kaferry/internal/sql"
	"github.com/stretchr/testify/require"
)

func TestBetweenNormalizesToExistingQueryIdentity(t *testing.T) {
	for _, tc := range []struct{ name, endpoints, between, expanded, digest string }{
		{"offsets", "TOPIC", "RECORD_OFFSET BETWEEN 100 AND 199", "RECORD_OFFSET >= 100 AND RECORD_OFFSET <= 199", "e1a71070b3bf1b36e98924658b73d8697a89376d5554fc0e33480b5c9f57ca27"},
		{"one offset", "TOPIC", "RECORD_OFFSET BETWEEN 0 AND 0", "RECORD_OFFSET >= 0 AND RECORD_OFFSET < 1", ""},
		{"large offsets", "TOPIC", "RECORD_OFFSET BETWEEN 9223372036854775805 AND 9223372036854775806", "RECORD_OFFSET >= 9223372036854775805 AND RECORD_OFFSET < 9223372036854775807", ""},
		{"partitions", "TOPIC", "PARTITION BETWEEN 0 AND 2", "PARTITION IN (2, 0, 1, 1)", "08c10c621df06d26903bd513dd2988d9a1bb10b56cfd252a76ec54c4da384faa"},
		{"exclude partitions", "TOPIC", "PARTITION NOT BETWEEN 1 AND 3", "PARTITION NOT IN (3, 2, 1)", "478e27b31b82f9b2d7fc505deced45ecefc39544d02eccc7a636f886ca54a0da"},
		{"one high partition", "TOPIC", "PARTITION BETWEEN 2147483647 AND 2147483647", "PARTITION IN (2147483647)", ""},
		{"archive", "ARCHIVE", "PARTITION BETWEEN 0 AND 2 AND RECORD_OFFSET BETWEEN 100 AND 199", "PARTITION IN (0, 1, 2) AND RECORD_OFFSET >= 100 AND RECORD_OFFSET < 200", ""},
		{"branches", "TOPIC", "(PARTITION BETWEEN 0 AND 1 AND RECORD_OFFSET BETWEEN 100 AND 199) OR (PARTITION IN (2) AND RECORD_OFFSET >= 900 AND RECORD_OFFSET < LATEST)", "(PARTITION IN (1, 0) AND RECORD_OFFSET >= 100 AND RECORD_OFFSET < 200) OR (PARTITION IN (2) AND RECORD_OFFSET >= 900 AND RECORD_OFFSET < LATEST)", "6e097cee577e01bbaa5daab1fc48940d160c9bc59174994b8941c82a28280558"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given equivalent inclusive ranges, lists and comparison predicates.
			prefix := "INSERT INTO " + tc.endpoints + " 'out' SELECT * FROM " + tc.endpoints + " 'in' WHERE "
			expanded := build(t, prefix+tc.expanded)
			// When the range spelling also shifts all source positions.
			current := build(t, "-- shifted positions\n"+prefix+tc.between)
			// Then normalized plans and saved query identities remain identical.
			require.Equal(t, expanded, current)
			x, err := expanded.testFingerprint("source-cluster", "destination-cluster")
			require.NoError(t, err)
			y, err := current.testFingerprint("source-cluster", "destination-cluster")
			require.NoError(t, err)
			require.Equal(t, x, y)
			if tc.digest != "" {
				// These digests fix the persisted identities of equivalent comparisons and lists.
				require.Equal(t, tc.digest, y)
			}
		})
	}
}

func TestCallerConstructedPartitionRangeRejectsNegativeIDs(t *testing.T) {
	// Given a caller-created boundary without parser source positions.
	ast, err := sql.NewParser().Parse("INSERT INTO TOPIC 'out' SELECT * FROM TOPIC 'in' WHERE PARTITION BETWEEN 0 AND 2")
	require.NoError(t, err)
	ast.Partitions.Range.Lower = sql.PartitionID{Value: "-1"}
	// When planning, then preserve the plain, redacted validation contract.
	_, err = NewPlanner(environment{}).Build(ast, profiles)
	require.EqualError(t, err, "PARTITION requires integers from 0 to 2147483647")
}

func TestBetweenDiagnosticsLocateAndRedact(t *testing.T) {
	const prefix = "INSERT INTO TOPIC 'out' SELECT * FROM TOPIC 'private_SENTINEL'\nWHERE "
	for _, tc := range []struct{ clause, marker, hint string }{
		{"RECORD_OFFSET BETWEEN 10 AND 5", "5", "offset range is empty"},
		{"RECORD_OFFSET BETWEEN 9223372036854775808 AND 9223372036854775809", "9223372036854775808", "non-negative signed 64-bit"},
		{"RECORD_OFFSET BETWEEN 0 AND 9223372036854775808", "9223372036854775808", "non-negative signed 64-bit"},
		{"RECORD_OFFSET BETWEEN 0 AND 9223372036854775807", "9223372036854775807", "boundary overflows"},
		{"RECORD_OFFSET >= 0 AND RECORD_OFFSET BETWEEN 1 AND 2", "BETWEEN", "lower bound repeats"},
		{"RECORD_OFFSET <= 10 AND RECORD_OFFSET BETWEEN 0 AND 9", "9", "upper bound repeats"},
		{"PARTITION BETWEEN 2147483648 AND 2147483649", "2147483648", "integers from 0 to 2147483647"},
		{"PARTITION BETWEEN 0 AND 2147483648", "2147483648", "integers from 0 to 2147483647"},
		{"PARTITION BETWEEN 8 AND 2", "2", "upper bound must not precede"},
		{"PARTITION BETWEEN 0 AND 2147483647", "2147483647", "at most 65536 IDs"},
		{"(PARTITION BETWEEN 0 AND 2 AND RECORD_OFFSET < 10) OR (PARTITION BETWEEN 2 AND 4 AND RECORD_OFFSET < 20)", "PARTITION BETWEEN 2", "branches overlap"},
		{"(PARTITION NOT BETWEEN 0 AND 2 AND RECORD_OFFSET < 10)", "(", "requires PARTITION IN or BETWEEN"},
		{"(PARTITION BETWEEN 0 AND 32767 AND RECORD_OFFSET < 10) OR (PARTITION BETWEEN 32768 AND 69999 AND RECORD_OFFSET < 20)", "69999", "at most 65536 IDs"},
	} {
		t.Run(tc.clause, func(t *testing.T) {
			// Given a syntactically valid range that violates a planning constraint.
			ast, err := sql.NewParser().Parse(prefix + tc.clause)
			require.NoError(t, err)
			// When planning, then identify the invalid boundary without SQL excerpts.
			_, err = NewPlanner(environment{}).Build(ast, profiles)
			var diagnostic *sql.Diagnostic
			require.ErrorAs(t, err, &diagnostic)
			require.Equal(t, sql.Position{Line: 2, Column: len("WHERE ") + strings.Index(tc.clause, tc.marker) + 1}, diagnostic.Position)
			require.Contains(t, err.Error(), tc.hint)
			require.NotContains(t, err.Error(), "private_SENTINEL")
			if len(tc.marker) > 10 {
				require.NotContains(t, err.Error(), tc.marker)
			}
		})
	}
}

func TestPartitionBetweenExpansionIsBoundedAndInclusive(t *testing.T) {
	// Given the largest permitted compact partition range.
	p := build(t, "INSERT INTO TOPIC 'out' SELECT * FROM TOPIC 'in' WHERE PARTITION BETWEEN 100 AND 65635")
	// When the planner expands it, then both boundaries are present within its budget.
	require.Len(t, p.Partitions.IDs, maxPartitionRangeIDs)
	require.EqualValues(t, 100, p.Partitions.IDs[0])
	require.EqualValues(t, 65635, p.Partitions.IDs[len(p.Partitions.IDs)-1])
}
