package sql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSettingsIdentifyTheirEndpointOrCopyScope(t *testing.T) {
	// Given every setting in its agreed clause.
	input := "INSERT INTO ARCHIVE 'out' WITH(ROTATE_AFTER_SIZE=2g,ROTATE_AFTER_AGE=INTERVAL '2' HOUR) SELECT * FROM TOPIC 'in' WITH(GROUP='g',MAX_STALENESS=INTERVAL '1' MINUTE) WHERE RECORD_TIMESTAMP < NOW() OPTIONS(WORKER_STOP_AFTER_RECORDS=5,ON_ERROR=FAIL)"
	// When parsing, then endpoint and whole-copy settings remain separate.
	s, err := NewParser().Parse(input)
	require.NoError(t, err)
	require.Len(t, s.Destination.Options, 2)
	require.Len(t, s.Source.Options, 2)
	require.Len(t, s.Options, 2)
	for _, tc := range []struct{ query, marker, message string }{
		{"INSERT INTO TOPIC 'out' WITH(GROUP='secret') SELECT * FROM TOPIC 'in' WITH(GROUP='g',START=EARLIEST)", "GROUP=", "GROUP is a source setting; put it in WITH after the FROM endpoint"},
		{"INSERT INTO TOPIC 'out' SELECT * FROM TOPIC 'in' WITH(GROUP='g',PARTITIONING=KEY)", "PARTITIONING=", "PARTITIONING is a destination setting; put it in WITH after the INTO endpoint"},
		{"INSERT INTO TOPIC 'out' SELECT * FROM TOPIC 'in' WITH(GROUP='g',ON_ERROR=FAIL)", "ON_ERROR=", "ON_ERROR is a copy setting; put it in OPTIONS"},
		{"INSERT INTO TOPIC 'out' SELECT * FROM TOPIC 'in' WITH(GROUP='g') OPTIONS(MAX_STALENESS=INTERVAL '1' SECOND)", "MAX_STALENESS=", "MAX_STALENESS is a source setting; put it in WITH after the FROM endpoint"},
		{"INSERT INTO ARCHIVE 'out' WITH(ON_REJECTED_TIMESTAMP=SKIP) SELECT * FROM ARCHIVE 'in' WITH(GROUP='g')", "ON_REJECTED_TIMESTAMP=", "ON_REJECTED_TIMESTAMP is a copy setting; put it in OPTIONS"},
	} {
		// Given a recognized setting in the wrong clause, when parsing, then name its correct clause at the setting.
		_, err := NewParser().Parse(tc.query)
		var d *Diagnostic
		require.ErrorAs(t, err, &d)
		require.Equal(t, Position{Line: 1, Column: strings.Index(tc.query, tc.marker) + 1}, d.Position)
		require.Equal(t, tc.message, d.Err.Error())
		require.NotContains(t, err.Error(), "secret")
	}
}

func TestUnsupportedSyntaxDoesNotAddImplicitCompatibilityOrFutureFeatures(t *testing.T) {
	for _, query := range []string{
		"INSERT INTO ARCHIVE 'out',ROTATE_AFTER_SIZE=1g SELECT * FROM ARCHIVE 'in' WITH(GROUP='g')",
		"INSERT INTO ARCHIVE 'out' SELECT * FROM ARCHIVE 'in' OPTIONS(GROUP='g')",
		"INSERT INTO ARCHIVE 'out' SELECT * FROM ARCHIVE 'in' WITH(GROUP='g') OPTIONS()",
		"INSERT INTO ARCHIVE 'out' SELECT * FROM ARCHIVE 'in' WITH()",
		"INSERT INTO TOPIC 'out' WITH(PARTITIONING=HASH) SELECT * FROM ARCHIVE 'in' WITH(GROUP='g')",
		"INSERT INTO TOPIC 'out' WITH(PARTITIONING=KEY,KEY_HASH=MURMUR2) SELECT * FROM ARCHIVE 'in' WITH(GROUP='g')",
		"INSERT INTO TOPIC 'out' WITH(PARTITIONING=PRESERVE,ON_MISSING_PARTITION=KEY) SELECT * FROM ARCHIVE 'in' WITH(GROUP='g')",
	} {
		// Given unsupported old or future syntax, when parsed, then reject it.
		_, err := NewParser().Parse(query)
		require.Error(t, err)
	}
}
