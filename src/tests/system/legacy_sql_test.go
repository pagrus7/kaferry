//go:build integration

package compat

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Legacy SQL is only for comparing binaries while the CLI grammar changes.
// It does not add compatibility syntax to the application.
func TestLegacyBinarySQLKeepsTheWorkloadAndScope(t *testing.T) {
	for _, tc := range []struct{ input, want string }{
		{"INSERT INTO TOPIC 'out' WITH(PARTITIONING=KEY) SELECT * FROM TOPIC 'in' WITH(GROUP='g',START=EARLIEST) OPTIONS(WORKER_STOP_AFTER_RECORDS=1);", "INSERT INTO TOPIC 'out'  SELECT * FROM TOPIC 'in'   OPTIONS(GROUP='g',START=EARLIEST,WORKER_STOP_AFTER_RECORDS=1);"},
		{"INSERT INTO ARCHIVE 'out' WITH(ROTATE_AFTER_SIZE=1g) SELECT * FROM ARCHIVE 'in' WITH(GROUP='g') WHERE RECORD_TIMESTAMP < NOW();", "INSERT INTO FILE avro('out',ROTATE_AFTER_SIZE=1g) SELECT * FROM FILE avro('in')  WHERE RECORD_TIMESTAMP < NOW() OPTIONS(GROUP='g');"},
	} {
		// Given a current workload, when adapting it for the previous binary, then change syntax alone.
		got, err := previousSQL(tc.input, true)
		require.NoError(t, err)
		require.Equal(t, strings.Fields(tc.want), strings.Fields(got))
	}
	for _, tc := range []struct{ input, message string }{
		{"INSERT INTO TOPIC 'out' SELECT * FROM ARCHIVE 'in' WITH(GROUP='g')", "explicit KEY"},
		{"INSERT INTO ARCHIVE 'out' SELECT * FROM ARCHIVE 'in'", "explicit source settings"},
		{"INSERT INTO TOPIC 'out' WITH(PARTITIONING=KEY) SELECT * FROM TOPIC 'in'", "explicit source settings"},
	} {
		// Given a workload the old syntax cannot represent, when adapting, then fail clearly without panicking.
		_, err := previousSQL(tc.input, true)
		require.ErrorContains(t, err, tc.message)
	}
}

func TestFileBinarySQLKeepsArchivePathsAndScopedSettings(t *testing.T) {
	// Given an archive workload and binaries with current or FILE syntax.
	const input = "INSERT INTO ARCHIVE 'outı''put' WITH(ROTATE_AFTER_SIZE=1g)\nSELECT * FROM ARCHIVE '源/in' WITH(GROUP='g') OPTIONS(WORKER_STOP_AFTER_RECORDS=1);"
	const want = "INSERT INTO FILE avro('outı''put') WITH(ROTATE_AFTER_SIZE=1g)\nSELECT * FROM FILE avro('源/in') WITH(GROUP='g') OPTIONS(WORKER_STOP_AFTER_RECORDS=1);"
	t.Setenv("KAFERRY_FILE_SQL_BINARIES", strings.Join([]string{"previous", "baseline"}, string(os.PathListSeparator)))
	t.Setenv("KAFERRY_LEGACY_SQL_BINARY", "")
	for _, binary := range []string{"previous", "baseline", "candidate"} {
		// When preparing SQL for each binary, then adapt only the selected binaries' endpoint syntax.
		got, err := binarySQL(binary, input)
		require.NoError(t, err)
		if binary == "candidate" {
			require.Equal(t, input, got)
		} else {
			require.Equal(t, want, got)
		}
	}
	// Given a topic capture, when adapting for an unscoped binary, then keep rotation with its archive destination.
	got, err := previousSQL("INSERT INTO ARCHIVE 'out' WITH(ROTATE_AFTER_SIZE=1g) SELECT * FROM TOPIC 'in' WITH(GROUP='g')", true)
	require.NoError(t, err)
	require.Equal(t, "INSERT INTO FILE avro('out',ROTATE_AFTER_SIZE=1g)  SELECT * FROM TOPIC 'in' OPTIONS(GROUP='g');", got)
}
