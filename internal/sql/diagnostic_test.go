package sql

import (
	"errors"
	"strings"
	"testing"
)

func TestParserDiagnosticsLocateTheOffendingToken(t *testing.T) {
	const prefix = "INSERT INTO FILE cbor('out')\nSELECT * FROM FILE cbor('in')\n"
	for _, tc := range []struct {
		name, query, message string
		position             Position
	}{
		{"format", "INSERT INTO FILE cbor('out')\nSELECT * FROM FILE json('in')\nOPTIONS(GROUP='g')", "expected cbor; found identifier", Position{2, 20}},
		{"unit before invalid character", prefix + "WHERE RECORD_TIMESTAMP >= NOW() - INTERVAL '1' WEEK ?", "expected SECOND, MINUTE, HOUR, or DAY; found identifier", Position{3, 48}},
		{"option before invalid character", prefix + "OPTIONS(STRAT ?)", "expected GROUP, START, CUTOFF, ON_ERROR, ON_MISSING_TIMESTAMP, or ON_REJECTED_TIMESTAMP; found identifier", Position{3, 9}},
		{"projection", "INSERT INTO FILE cbor('out')\nSELECT 'secret' FROM FILE cbor('in') OPTIONS(GROUP='g')", "expected *; found string literal", Position{2, 8}},
		{"missing equals", prefix + "OPTIONS(GROUP 'g')", "expected =; found string literal", Position{3, 15}},
		{"OR", prefix + "WHERE RECORD_TIMESTAMP >= NOW() OR RECORD_TIMESTAMP < NOW()", "expected AND or OPTIONS; found identifier", Position{3, 33}},
		{"EOF", prefix + "OPTIONS(GROUP=", "expected ONE_OFF; found end of statement", Position{3, 15}},
		{"unterminated string", prefix + "OPTIONS(GROUP='secret", "unterminated string literal; close it with a single quote", Position{3, 15}},
		{"CRLF and Unicode", "INSERT INTO FILE cbor('源')\r\nSELECT * FROM FILE json('in') OPTIONS(GROUP='g')", "expected cbor; found identifier", Position{2, 20}},
		{"tabs and Unicode", "INSERT INTO FILE cbor('源')\tSELECT key", "expected *; found identifier", Position{1, 35}},
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
			if !errors.As(err, &diagnostic) || diagnostic.Position != tc.position || diagnostic.Err.Error() != tc.message {
				t.Fatalf("error=%v, want %v: %s", err, tc.position, tc.message)
			}
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
	if err != specific || !errors.Is(err, cause) || At(Position{}, nil) != nil {
		t.Fatal("diagnostic lost location or cause", err)
	}
	if got := At(Position{}, cause).Error(); got != "invalid value" {
		t.Fatal("unknown location rendered as a false position", got)
	}
	if got := At(Position{1, 4}, At(Position{}, cause)).Error(); got != "line 1, column 4: invalid value" {
		t.Fatal("fallback location was lost", got)
	}
}

func TestParserDiagnosticsNeverEchoArbitraryInput(t *testing.T) {
	const secret = "sensitive_SENTINEL_5c27"
	const base = "WITH CONNECTION prod AS ('sasl.password'='" + secret + "') INSERT INTO FILE cbor('out') SELECT * FROM FILE cbor('in') OPTIONS(GROUP='g'"
	for _, query := range []string{base + ", " + secret + "=FAIL)", base + ", START='" + secret + "')", strings.Replace(base+")", "SELECT *", "SELECT "+secret, 1), base + ") " + secret, base + ", ON_ERROR='" + secret} {
		// Given a secret in a valid property and in malformed SQL.
		// When reporting the first parse error.
		_, err := NewParser().Parse(query)
		// Then never print arbitrary token text or SQL source.
		if err == nil || strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "sasl.password") {
			t.Fatal("unsafe diagnostic", err)
		}
	}
}
