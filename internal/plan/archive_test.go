package plan

import (
	"fmt"
	"github.com/pagrus7/kaferry/internal/sql"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestArchiveOptionsValidateAndKeepOperationalSettingsOutOfFingerprint(t *testing.T) {
	// Given optional rotation settings and equivalent query identity.
	build := func(t *testing.T, format, options string) Plan {
		t.Helper()
		statement, err := sql.NewParser().Parse(fmt.Sprintf("INSERT INTO FILE %s('out'%s) SELECT * FROM FILE %s('in') OPTIONS(GROUP='g')", format, options, format))
		require.NoError(t, err)
		p, err := NewPlanner(nil).Build(statement, nil)
		require.NoError(t, err)
		return p
	}
	p := build(t, "avro", ",ROTATE_AFTER_SIZE=2G,ROTATE_AFTER_AGE=INTERVAL '3' MINUTE")
	require.EqualValues(t, 2<<30, p.Destination.RotateAfterSize)
	require.EqualValues(t, 180000, p.Destination.RotateAfterAgeMS)
	// When fingerprinting, then rotation tuning is excluded but the format is bound.
	fp, err := p.Fingerprint("", "")
	require.NoError(t, err)
	plain, err := build(t, "avro", "").Fingerprint("", "")
	require.NoError(t, err)
	require.Equal(t, plain, fp)
	legacy, err := build(t, "cbor", "").Fingerprint("", "")
	require.NoError(t, err)
	require.NotEqual(t, legacy, fp)
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
			query := "INSERT INTO FILE avro('private/path'" + tc.options + ") SELECT * FROM FILE avro('in') OPTIONS(GROUP='g')"
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

func TestArchiveEndpointRestrictionsLocateAndRedactDiagnostics(t *testing.T) {
	for _, tc := range []struct{ destination, source, marker, message string }{
		{"avro('private/output')", "avro('private/input',ROTATE_AFTER_SIZE=1k)", "ROTATE_AFTER_SIZE", "rotation options require a FILE avro destination"},
		{"cbor('private/output',ROTATE_AFTER_SIZE=1k)", "cbor('private/input')", "ROTATE_AFTER_SIZE", "rotation options require FILE avro"},
		{"avro('private/output')", "cbor('private/input')", "'private/output'", "file copies require matching formats"},
		{"cbor('private/output')", "avro('private/input')", "'private/output'", "file copies require matching formats"},
	} {
		t.Run(tc.destination+" from "+tc.source, func(t *testing.T) {
			// Given parsed SQL whose endpoint options or formats cannot be combined.
			query := "INSERT INTO FILE " + tc.destination + " SELECT * FROM FILE " + tc.source + " OPTIONS(GROUP='g')"
			ast, err := sql.NewParser().Parse(query)
			require.NoError(t, err)
			// When planning, then locate the restriction without echoing private paths.
			_, err = NewPlanner(nil).Build(ast, nil)
			require.ErrorContains(t, err, tc.message)
			require.NotContains(t, err.Error(), "private/")
			var diagnostic *sql.Diagnostic
			require.ErrorAs(t, err, &diagnostic)
			require.Equal(t, strings.Index(query, tc.marker)+1, diagnostic.Position.Column)
		})
	}
}
