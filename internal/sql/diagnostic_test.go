package sql

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParserDiagnosticsLocateTheOffendingToken(t *testing.T) {
	const prefix = "INSERT INTO FILE cbor('out')\nSELECT * FROM FILE cbor('in')\n"
	for _, tc := range []struct {
		name, query, message string
		position             Position
	}{
		{"unquoted endpoint", "INSERT INTO TOPIC sensitive_SENTINEL_5c27", "expected single-quoted string; found identifier", Position{1, 19}},
		{"invalid time operand", prefix + "WHERE RECORD_TIMESTAMP >= sensitive_SENTINEL_5c27", "expected NOW or TIMESTAMP; found identifier", Position{3, 27}},
		{"empty input", "", "expected WITH or INSERT; found end of input", Position{1, 1}},
		{"group name needs quotes", prefix + "OPTIONS(GROUP=orders)", "expected single-quoted string or ONE_OFF; found identifier", Position{3, 15}},
		{"quoted start", prefix + "OPTIONS(GROUP='g', START='EARLIEST')", "expected EARLIEST or LATEST; found string literal", Position{3, 26}},
		{"bad start before character", prefix + "OPTIONS(GROUP='g', START=BEGIN ?)", "expected EARLIEST or LATEST; found identifier", Position{3, 26}},
		{"quoted policy", prefix + "OPTIONS(GROUP='g', ON_ERROR='SKIP')", "expected FAIL or SKIP; found string literal", Position{3, 29}},
		{"bad policy before character", prefix + "OPTIONS(GROUP='g', ON_ERROR=RETRY ?)", "expected FAIL or SKIP; found identifier", Position{3, 29}},
		{"missing comma", prefix + "OPTIONS(GROUP='g' START=EARLIEST)", "expected \",\" or \")\"; found identifier", Position{3, 19}},
		{"missing WHERE", prefix + "WHER RECORD_TIMESTAMP < NOW()", "expected WHERE or OPTIONS; found identifier", Position{3, 1}},
		{"BOM", "\uFEFFINSERT", "UTF-8 byte-order mark is not supported; save without a BOM", Position{1, 1}},
		{"interior FEFF", "INSERT\uFEFF INTO", "unexpected invisible or control character; remove it", Position{1, 7}},
		{"double quote", "INSERT INTO TOPIC \"b\"", "double quotes are not supported; use ASCII single quotes for strings", Position{1, 19}},
		{"typographic quote", "\u2018secret\u2019", "typographic quotes are not supported; use ASCII single quotes", Position{1, 1}},
		{"unquoted number", "7", "numbers must be single-quoted, for example INTERVAL '7' DAY", Position{1, 1}},
		{"invisible character", "\x00", "unexpected invisible or control character; remove it", Position{1, 1}},
		{"format", "INSERT INTO FILE cbor('out')\nSELECT * FROM FILE json('in')\nOPTIONS(GROUP='g')", "expected cbor; found identifier", Position{2, 20}},
		{"unit before invalid character", prefix + "WHERE RECORD_TIMESTAMP >= NOW() - INTERVAL '1' WEEK ?", "expected SECOND, MINUTE, HOUR or DAY; found identifier", Position{3, 48}},
		{"option before invalid character", prefix + "OPTIONS(STRAT ?)", "expected GROUP, START, CUTOFF, ON_ERROR, ON_MISSING_TIMESTAMP or ON_REJECTED_TIMESTAMP; found identifier", Position{3, 9}},
		{"projection", "INSERT INTO FILE cbor('out')\nSELECT 'secret' FROM FILE cbor('in') OPTIONS(GROUP='g')", "expected \"*\"; found string literal", Position{2, 8}},
		{"missing equals", prefix + "OPTIONS(GROUP 'g')", "expected \"=\"; found string literal", Position{3, 15}},
		{"OR", prefix + "WHERE RECORD_TIMESTAMP >= NOW() OR RECORD_TIMESTAMP < NOW()", "expected \"+\", \"-\", AND or OPTIONS; found identifier", Position{3, 33}},
		{"EOF", prefix + "OPTIONS(GROUP=", "expected single-quoted string or ONE_OFF; found end of input", Position{3, 15}},
		{"unterminated string", prefix + "OPTIONS(GROUP='secret", "unterminated string literal; close it with a single quote", Position{3, 15}},
		{"CRLF and Unicode", "INSERT INTO FILE cbor('源')\r\nSELECT * FROM FILE json('in') OPTIONS(GROUP='g')", "expected cbor; found identifier", Position{2, 20}},
		{"tabs and Unicode", "INSERT INTO FILE cbor('源')\tSELECT key", "expected \"*\"; found identifier", Position{1, 35}},
		{"invalid UTF8", "-- 源\n\xff", "statement is not valid UTF-8; save it as UTF-8", Position{2, 1}},
		{"unterminated comment", prefix + "/* private", "unterminated comment; close it with */", Position{3, 1}},
		{"nested comment", "/* outer /* private */", "nested comments are not supported", Position{1, 10}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given malformed SQL with a known offending token.
			// When parsing without accessing endpoints.
			_, err := NewParser().Parse(tc.query)
			// Then identify that token and give a precise, redacted explanation.
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
	const base = "WITH CONNECTION prod AS ('sasl.password'='" + secret + "') INSERT INTO FILE cbor('out') SELECT * FROM FILE cbor('in') OPTIONS(GROUP='g'"
	for _, query := range []string{base + ", " + secret + "=FAIL)", base + ", START='" + secret + "')", strings.Replace(base+")", "SELECT *", "SELECT "+secret, 1), base + ") " + secret, base + ", ON_ERROR='" + secret} {
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
		"INSERT INTO TOPIC 'out@prod'\nSELECT * FROM TOPIC 'in@prod'\n" +
		"WHERE RECORD_TIMESTAMP >= TIMESTAMP '2026-10-01T00:00:00Z' + INTERVAL '1' HOUR\n" +
		"AND RECORD_TIMESTAMP < NOW() - INTERVAL '2' SECOND\nOPTIONS(GROUP='g', CUTOFF=INTERVAL '3' MINUTE)"
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
		{statement.Options[0].Position, "GROUP"}, {statement.Options[0].ValuePosition, "'g'"},
		{statement.Options[1].Position, "CUTOFF"}, {statement.Options[1].ValuePosition, "INTERVAL '3'"},
		{statement.Options[1].Interval.Position, "'3'"},
	} {
		// Then each anchor points to its own token, never the following token.
		prefix := input[:strings.Index(input, tc.marker)]
		lines := strings.Split(prefix, "\n")
		want := Position{Line: len(lines), Column: len([]rune(lines[len(lines)-1])) + 1}
		require.Equal(t, want, tc.got)
	}
}
