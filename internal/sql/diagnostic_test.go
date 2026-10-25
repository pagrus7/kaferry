package sql

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParserDiagnosticsLocateTheOffendingToken(t *testing.T) {
	const bare = "INSERT INTO FILE avro('out')\nSELECT * FROM FILE avro('in')\n"
	const prefix = "INSERT INTO FILE avro('out')\nSELECT * FROM FILE avro('in') WITH(GROUP='g')\n"
	for _, tc := range []struct {
		name, query, message string
		position             Position
	}{
		{"unquoted endpoint", "INSERT INTO TOPIC sensitive_SENTINEL_5c27", "expected single-quoted string; found identifier", Position{1, 19}},
		{"invalid time operand", prefix + "WHERE RECORD_TIMESTAMP >= sensitive_SENTINEL_5c27", "expected NOW or TIMESTAMP; found identifier", Position{3, 27}},
		{"empty input", "", "expected WITH or INSERT; found end of input", Position{1, 1}},
		{"group name needs quotes", bare + "WITH(GROUP=orders)", "expected single-quoted string or ONE_OFF; found identifier", Position{3, 12}},
		{"quoted start", bare + "WITH(GROUP='g', START='EARLIEST')", "expected EARLIEST or LATEST; found string literal", Position{3, 23}},
		{"bad start before character", bare + "WITH(GROUP='g', START=BEGIN ?)", "expected EARLIEST or LATEST; found identifier", Position{3, 23}},
		{"quoted policy", prefix + "OPTIONS(ON_ERROR='SKIP')", "expected FAIL or SKIP; found string literal", Position{3, 18}},
		{"bad policy before character", prefix + "OPTIONS(ON_ERROR=RETRY ?)", "expected FAIL or SKIP; found identifier", Position{3, 18}},
		{"missing comma", bare + "WITH(GROUP='g' START=EARLIEST)", "expected \",\" or \")\"; found identifier", Position{3, 16}},
		{"missing WHERE", prefix + "WHER RECORD_TIMESTAMP < NOW()", "expected WHERE, OPTIONS, \";\" or end of input; found identifier", Position{3, 1}},
		{"bare source unknown clause", bare + "private_SENTINEL RECORD_TIMESTAMP < NOW()", "expected WITH, WHERE, OPTIONS, \";\" or end of input; found identifier", Position{3, 1}},
		{"BOM", "\uFEFFINSERT", "UTF-8 byte-order mark is not supported; save without a BOM", Position{1, 1}},
		{"interior FEFF", "INSERT\uFEFF INTO", "unexpected invisible or control character; remove it", Position{1, 7}},
		{"double quote", "INSERT INTO TOPIC \"b\"", "double quotes are not supported; use ASCII single quotes for strings", Position{1, 19}},
		{"typographic quote", "\u2018secret\u2019", "typographic quotes are not supported; use ASCII single quotes", Position{1, 1}},
		{"unquoted number", "7", "expected WITH or INSERT; found integer", Position{1, 1}},
		{"invisible character", "\x00", "unexpected invisible or control character; remove it", Position{1, 1}},
		{"format", "INSERT INTO FILE avro('out')\nSELECT * FROM FILE json('in')", "expected avro; found identifier", Position{2, 20}},
		{"unit before invalid character", prefix + "WHERE RECORD_TIMESTAMP >= NOW() - INTERVAL '1' WEEK ?", "expected SECOND, MINUTE, HOUR or DAY; found identifier", Position{3, 48}},
		{"unquoted interval", bare + "WITH(GROUP='g', MAX_STALENESS=INTERVAL 5 MINUTE)", "expected single-quoted string; found integer", Position{3, 40}},
		{"option before invalid character", prefix + "OPTIONS(STRAT ?)", "expected WORKER_STOP_AFTER_RECORDS, ON_ERROR, ON_MISSING_TIMESTAMP or ON_REJECTED_TIMESTAMP; found identifier", Position{3, 9}},
		{"projection", "INSERT INTO FILE avro('out')\nSELECT 'secret' FROM FILE avro('in')", "expected \"*\"; found string literal", Position{2, 8}},
		{"missing equals", bare + "WITH(GROUP 'g')", "expected \"=\"; found string literal", Position{3, 12}},
		{"OR", prefix + "WHERE RECORD_TIMESTAMP >= NOW() OR RECORD_TIMESTAMP < NOW()", "expected \"+\", \"-\", AND, OPTIONS, \";\" or end of input; found identifier", Position{3, 33}},
		{"EOF", bare + "WITH(GROUP=", "expected single-quoted string or ONE_OFF; found end of input", Position{3, 12}},
		{"unterminated string", bare + "WITH(GROUP='secret", "unterminated string literal; close it with a single quote", Position{3, 12}},
		{"CRLF and Unicode", "INSERT INTO FILE avro('源')\r\nSELECT * FROM FILE json('in')", "expected avro; found identifier", Position{2, 20}},
		{"tabs and Unicode", "INSERT INTO FILE avro('源')\tSELECT key", "expected \"*\"; found identifier", Position{1, 35}},
		{"invalid UTF8", "-- 源\n\xff", "statement is not valid UTF-8; save it as UTF-8", Position{2, 1}},
		{"unterminated comment", prefix + "/* private", "unterminated comment; close it with */", Position{3, 1}},
		{"nested comment", "/* outer /* private */", "nested comments are not supported", Position{1, 10}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given malformed SQL with a known offending token.
			// When parsing without accessing endpoints.
			_, err := NewParser().Parse(tc.query)
			// Then locate that token and give a redacted explanation.
			var diagnostic *Diagnostic
			require.ErrorAs(t, err, &diagnostic)
			require.Equal(t, tc.position, diagnostic.Position)
			require.Equal(t, tc.message, diagnostic.Err.Error())
			require.NotContains(t, err.Error(), "sensitive_SENTINEL_5c27")
		})
	}
}

func TestDiagnosticPreservesCauseAndSpecificLocation(t *testing.T) {
	// Given a cause already located at its offending value.
	cause := errors.New("invalid value")
	specific := At(Position{2, 9}, cause)
	// When a containing clause supplies a fallback location.
	err := At(Position{1, 1}, specific)
	// Then preserve both the value's position and the original cause.
	require.Same(t, specific, err)
	require.ErrorIs(t, err, cause)
	require.Nil(t, At(Position{}, nil))
	require.Equal(t, "invalid value", At(Position{}, cause).Error())
	require.Equal(t, "line 1, column 4: invalid value", At(Position{1, 4}, At(Position{}, cause)).Error())
	require.Equal(t, "invalid SQL", (&Diagnostic{}).Error())
}

func TestParserDiagnosticsNeverEchoArbitraryInput(t *testing.T) {
	const secret = "sensitive_SENTINEL_5c27"
	const base = "WITH CONNECTION prod AS ('sasl.password'='" + secret + "') INSERT INTO FILE avro('out') SELECT * FROM FILE avro('in') WITH(GROUP='g') OPTIONS("
	for _, query := range []string{base + secret + "=FAIL)", strings.Replace(base, "WITH(GROUP='g') OPTIONS(", "WITH(GROUP='g', START='"+secret+"')", 1), strings.Replace(base+")", "SELECT *", "SELECT "+secret, 1), base + "ON_ERROR=FAIL) " + secret, base + "ON_ERROR='" + secret} {
		// Given a secret in a valid property and in malformed SQL.
		// When reporting the first parse error.
		_, err := NewParser().Parse(query)
		// Then never print arbitrary token text or SQL source.
		require.Error(t, err)
		require.NotContains(t, strings.ToLower(err.Error()), strings.ToLower(secret))
		require.NotContains(t, err.Error(), "sasl.password")
	}
}

func TestASTCapturesEachAnchorBeforeAdvancingTokens(t *testing.T) {
	// Given one statement exercising every source-location field.
	input := "WITH CONNECTION prod AS ('sasl.password'=ENV('PASSWORD'), 'bootstrap.servers'='broker')\n" +
		"INSERT INTO TOPIC 'out@prod'\nSELECT * FROM TOPIC 'in@prod' WITH (GROUP='g', MAX_STALENESS=INTERVAL '3' MINUTE)\n" +
		"WHERE RECORD_TIMESTAMP >= TIMESTAMP '2026-10-01T00:00:00Z' + INTERVAL '1' HOUR\n" +
		"AND RECORD_TIMESTAMP < NOW() - INTERVAL '2' SECOND\nOPTIONS(ON_ERROR=FAIL)"
	// When parsing while preserving transient locations.
	statement, err := NewParser().Parse(input)
	require.NoError(t, err)
	for _, tc := range []struct {
		got    Position
		marker string
	}{
		{statement.Connections[0].Position, "prod AS"},
		{statement.Connections[0].Properties[0].Position, "'sasl.password'"},
		{statement.Connections[0].Properties[0].ValuePosition, "'PASSWORD'"},
		{statement.Connections[0].Properties[1].ValuePosition, "'broker'"},
		{statement.Source.Position, "'in@prod'"}, {statement.Destination.Position, "'out@prod'"},
		{statement.WherePosition, "WHERE"}, {statement.OptionsPosition, "OPTIONS"},
		{statement.Bounds[0].Position, ">="}, {statement.Bounds[0].Time.Position, "'2026"},
		{statement.Bounds[0].Time.Adjustments[0].Position, "'1'"},
		{statement.Bounds[1].Position, "< NOW"}, {statement.Bounds[1].Time.Position, "NOW"},
		{statement.Bounds[1].Time.Adjustments[0].Position, "'2'"},
		{statement.Source.Options[0].Position, "GROUP"}, {statement.Source.Options[0].ValuePosition, "'g'"},
		{statement.Source.Options[1].Position, "MAX_STALENESS"}, {statement.Source.Options[1].ValuePosition, "INTERVAL '3'"},
		{statement.Source.Options[1].Interval.Position, "'3'"},
	} {
		// Then each anchor points to its own token, never the following token.
		prefix := input[:strings.Index(input, tc.marker)]
		lines := strings.Split(prefix, "\n")
		want := Position{Line: len(lines), Column: len([]rune(lines[len(lines)-1])) + 1}
		require.Equal(t, want, tc.got)
	}
}

func TestRemovedArchiveFormatIsRejectedWithoutLeakingPaths(t *testing.T) {
	for _, endpoints := range [][2]string{{"cbor", "avro"}, {"avro", "cbor"}} {
		// Given the removed format at either endpoint and a private path.
		query := "INSERT INTO FILE " + endpoints[0] + "('private/path') SELECT * FROM FILE " + endpoints[1] + "('private/path') WITH(GROUP='g')"
		// When parsing, then reject the format itself with a precise, redacted diagnostic.
		_, err := NewParser().Parse(query)
		var diagnostic *Diagnostic
		require.ErrorAs(t, err, &diagnostic)
		require.Equal(t, strings.Index(query, "cbor")+1, diagnostic.Position.Column)
		require.Equal(t, "expected avro; found identifier", diagnostic.Err.Error())
		require.NotContains(t, err.Error(), "private/path")
	}
}

func TestCutoffIsRejectedAtItsNameWithoutEchoingValues(t *testing.T) {
	// Given an unsupported spelling with private input in its value.
	query := "INSERT INTO TOPIC 'out' SELECT * FROM TOPIC 'in'\nWITH(CUTOFF=INTERVAL 'private_SENTINEL' MINUTE)"
	// When parsing, then locate the setting name and keep its value private.
	_, err := NewParser().Parse(query)
	var diagnostic *Diagnostic
	require.ErrorAs(t, err, &diagnostic)
	require.Equal(t, Position{2, 6}, diagnostic.Position)
	require.NotContains(t, err.Error(), "private_SENTINEL")
}
