package plan

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pagrus7/kaferry/internal/sql"
	"github.com/stretchr/testify/require"
)

func TestArchiveOptionsValidateAndKeepOperationalSettingsOutOfFingerprint(t *testing.T) {
	// Given optional rotation settings and equivalent query identity.
	build := func(t *testing.T, options string) Plan {
		t.Helper()
		statement, err := sql.NewParser().Parse(fmt.Sprintf("INSERT INTO ARCHIVE 'out' %s SELECT * FROM ARCHIVE 'in' WITH (GROUP='g') ", options))
		require.NoError(t, err)
		p, err := NewPlanner(nil).Build(statement, nil)
		require.NoError(t, err)
		return p
	}
	p := build(t, "WITH(ROTATE_AFTER_SIZE=2G,ROTATE_AFTER_AGE=INTERVAL '3' MINUTE)")
	require.EqualValues(t, 2<<30, p.Destination.RotateAfterSize)
	require.EqualValues(t, 180000, p.Destination.RotateAfterAgeMS)
	// When fingerprinting, then rotation tuning is excluded while Avro query identity stays stable.
	fp, err := p.Fingerprint("", "")
	require.NoError(t, err)
	plain, err := build(t, "").Fingerprint("", "")
	require.NoError(t, err)
	require.Equal(t, plain, fp)
}
func TestArchiveOptionDiagnosticsLocateValuesWithoutLeakingPaths(t *testing.T) {
	for _, tc := range []struct{ options, marker, message string }{
		{",ROTATE_AFTER_SIZE=0", "0", "positive bytes"},
		{",ROTATE_AFTER_SIZE=9223372036854775807g", "922337", "positive bytes"},
		{",ROTATE_AFTER_AGE=INTERVAL '0' HOUR", "INTERVAL", "positive interval"},
		{",ROTATE_AFTER_SIZE=1k,ROTATE_AFTER_SIZE=2k", "ROTATE_AFTER_SIZE=2", "repeats"},
	} {
		t.Run(tc.options, func(t *testing.T) {
			// Given a malformed file option containing a private endpoint path.
			query := "INSERT INTO ARCHIVE 'private/path' WITH(" + strings.TrimPrefix(tc.options, ",") + ") SELECT * FROM ARCHIVE 'in' WITH(GROUP='g')"
			ast, err := sql.NewParser().Parse(query)
			require.NoError(t, err)
			// When planning, then the diagnostic identifies the option without echoing SQL values.
			_, err = NewPlanner(nil).Build(ast, nil)
			require.ErrorContains(t, err, tc.message)
			require.NotContains(t, err.Error(), "private/path")
			var diagnostic *sql.Diagnostic
			require.ErrorAs(t, err, &diagnostic)
			require.Equal(t, strings.Index(query, tc.marker)+1, diagnostic.Position.Column)
		})
	}
}

func TestSourceRotationDiagnosticLocatesOptionWithoutLeakingPaths(t *testing.T) {
	// Given rotation on a source endpoint with a private path.
	query := "INSERT INTO ARCHIVE 'private/output' SELECT * FROM ARCHIVE 'private/input' WITH(GROUP='g', ROTATE_AFTER_SIZE=1k)"
	_, err := sql.NewParser().Parse(query)
	// When parsing, then locate a destination setting in the source clause.
	require.ErrorContains(t, err, "put it in WITH after the INTO endpoint")
	require.NotContains(t, err.Error(), "private/")
	var diagnostic *sql.Diagnostic
	require.ErrorAs(t, err, &diagnostic)
	require.Equal(t, strings.Index(query, "ROTATE_AFTER_SIZE")+1, diagnostic.Position.Column)
}
