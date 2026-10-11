package plan

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/pagrus7/kaferry/internal/sql"
)

func TestPlannerDiagnosticsIdentifyTheInvalidClauseOrValue(t *testing.T) {
	const files = "INSERT INTO FILE cbor('out')\nSELECT * FROM FILE cbor('in')\n"
	const topics = "INSERT INTO TOPIC 'b'\nSELECT * FROM TOPIC 'a'\n"
	for _, tc := range []struct{ name, query, marker, message string }{
		{"missing group", files + "OPTIONS(ON_ERROR=SKIP)", "OPTIONS", "GROUP is required; add GROUP = 'name' or GROUP = ONE_OFF to OPTIONS"},
		{"empty group", files + "OPTIONS(GROUP='')", "''", "GROUP must not be empty"},
		{"duplicate group", files + "OPTIONS(GROUP='g', GROUP='h')", "GROUP='h'", "option GROUP repeats; specify it once"},
		{"invalid policy", files + "OPTIONS(GROUP='g', ON_ERROR=RETRY)", "RETRY", "ON_ERROR must be FAIL or SKIP"},
		{"invalid start", topics + "OPTIONS(GROUP='g', START=BEGIN)", "BEGIN", "START must be EARLIEST or LATEST"},
		{"file start", files + "OPTIONS(GROUP='g', START=EARLIEST)", "START", "START requires a topic source; remove START for FILE sources"},
		{"missing start", topics + "OPTIONS(GROUP='g')", "OPTIONS", "topic sources require WHERE or START; add START = EARLIEST or START = LATEST to OPTIONS"},
		{"conflicting policies", topics + "WHERE RECORD_TIMESTAMP < NOW()\nOPTIONS(GROUP='g', ON_ERROR=SKIP, ON_MISSING_TIMESTAMP=FAIL)", "ON_MISSING_TIMESTAMP", "ON_ERROR cannot be combined with specific error policies; choose ON_ERROR or the specific policies"},
		{"reversed policy conflict", topics + "WHERE RECORD_TIMESTAMP < NOW()\nOPTIONS(GROUP='g', ON_MISSING_TIMESTAMP=FAIL, ON_ERROR=SKIP)", "ON_ERROR", "ON_ERROR cannot be combined with specific error policies; choose ON_ERROR or the specific policies"},
		{"invalid date", files + "WHERE RECORD_TIMESTAMP >= TIMESTAMP '2026-02-30T00:00:00Z'\nOPTIONS(GROUP='g')", "'2026-02-30", "invalid RFC 3339 calendar date or clock time; check day, month and time fields"},
		{"missing zone", files + "WHERE RECORD_TIMESTAMP >= TIMESTAMP '2026-10-01T00:00:00'\nOPTIONS(GROUP='g')", "'2026-10-01", "timestamp must be RFC 3339 with an explicit zone and at most 3 fractional digits, for example '2026-10-01T09:00:00Z'"},
		{"negative interval", files + "WHERE RECORD_TIMESTAMP >= NOW() - INTERVAL '-1' HOUR\nOPTIONS(GROUP='g')", "'-1'", "interval must be a non-negative integer, for example INTERVAL '7' DAY"},
		{"duplicate lower bound", files + "WHERE RECORD_TIMESTAMP >= NOW() AND RECORD_TIMESTAMP > NOW()\nOPTIONS(GROUP='g')", "> NOW()", "at most one lower and one upper bound are allowed; remove this repeated bound"},
		{"undefined connection", "INSERT INTO FILE cbor('out')\nSELECT * FROM TOPIC 'a@missing'\nOPTIONS(GROUP='g', START=EARLIEST)", "'a@missing'", "FROM endpoint: connection alias is not defined; add it with WITH CONNECTION or TOML [connections.alias]"},
		{"duplicate alias", "WITH CONNECTION prod AS ('bootstrap.servers'='x'), CONNECTION prod AS ('bootstrap.servers'='y')\n" + files + "OPTIONS(GROUP='g')", "prod AS ('bootstrap.servers'='y')", "connection alias repeats; declare each alias once"},
		{"duplicate property", "WITH CONNECTION prod AS ('private'='x', 'private'='y')\n" + files + "OPTIONS(GROUP='g')", "'private'='y'", "inline connection property repeats; specify it once per CONNECTION"},
		{"empty property", "WITH CONNECTION prod AS (''='x')\n" + files + "OPTIONS(GROUP='g')", "''", "inline connection property name must not be empty"},
		{"unset env", "WITH CONNECTION prod AS ('sasl.password'=ENV('PRIVATE_ENV'))\n" + files + "OPTIONS(GROUP='g')", "'PRIVATE_ENV'", "ENV(...) variable is unset; set it before running"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given syntactically valid SQL with an invalid clause or value.
			statement, err := sql.NewParser().Parse(tc.query)
			if err != nil {
				t.Fatal(err)
			}
			prefix := tc.query[:strings.Index(tc.query, tc.marker)]
			lines := strings.Split(prefix, "\n")
			position := sql.Position{Line: len(lines), Column: utf8.RuneCountInString(lines[len(lines)-1]) + 1}
			// When planning without opening endpoints.
			_, err = NewPlanner(environment{}).Build(statement, profiles)
			// Then identify the offending value or second conflicting entry precisely.
			var diagnostic *sql.Diagnostic
			if !errors.As(err, &diagnostic) || diagnostic.Position != position || diagnostic.Err.Error() != tc.message {
				t.Fatalf("error=%v, want %v: %s", err, position, tc.message)
			}
		})
	}
}

func TestSourcePositionsDoNotChangePlansOrSavedIdentity(t *testing.T) {
	// Given equivalent queries with different comments, whitespace and locations.
	query := "INSERT INTO TOPIC 'b' SELECT * FROM TOPIC 'a' WHERE RECORD_TIMESTAMP >= NOW() - INTERVAL '1' HOUR OPTIONS(GROUP='g')"
	formatted := "-- header\n" + strings.ReplaceAll(query, " SELECT ", "\nSELECT /* comment */ ")
	first, _ := sql.NewParser().Parse(query)
	second, _ := sql.NewParser().Parse(formatted)
	if first.Source.Position == second.Source.Position {
		t.Fatal("test did not change the source location")
	}
	// When compiling and fingerprinting both queries.
	a, err := NewPlanner(environment{}).Build(first, profiles)
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewPlanner(environment{}).Build(second, profiles)
	if err != nil {
		t.Fatal(err)
	}
	fa, err := a.Fingerprint("source", "destination")
	if err != nil {
		t.Fatal(err)
	}
	fb, err := b.Fingerprint("source", "destination")
	// Then transient positions never change normalized plans or checkpoint identity.
	if err != nil || !reflect.DeepEqual(a, b) || fa != fb {
		t.Fatal("source positions affected copy identity", err)
	}
}

func TestPlannerDiagnosticsRedactLiteralAndResolvedSecrets(t *testing.T) {
	const secret = "sensitive_SENTINEL_5c27"
	const files = " INSERT INTO FILE cbor('out') SELECT * FROM FILE cbor('in') "
	for _, query := range []string{
		"WITH CONNECTION prod AS ('sasl.password'=ENV('" + secret + "'))" + files + "OPTIONS(GROUP='g')",
		"WITH CONNECTION prod AS ('sasl.password'=ENV('PASSWORD'), 'bootstrap.servers'='x')" + files + "OPTIONS(GROUP='g', ON_ERROR=" + secret + ")",
		"WITH CONNECTION prod AS ('" + secret + "'='x', '" + secret + "'='y')" + files + "OPTIONS(GROUP='g')",
		"INSERT INTO FILE cbor('out') SELECT * FROM TOPIC 'a@" + secret + "' OPTIONS(GROUP='g', START=EARLIEST)",
		files + "WHERE RECORD_TIMESTAMP >= TIMESTAMP '" + secret + "' OPTIONS(GROUP='g')",
	} {
		// Given sensitive literals and a resolved environment credential.
		statement, err := sql.NewParser().Parse(query)
		if err != nil {
			t.Fatal(err)
		}
		// When planning fails after reading available environment values.
		_, err = NewPlanner(environment{"PASSWORD": secret}).Build(statement, profiles)
		// Then neither the arbitrary SQL text nor the resolved credential is exposed.
		if err == nil || strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "PASSWORD") {
			t.Fatal("unsafe planner diagnostic", err)
		}
	}
}

func TestConnectionErrorsDistinguishUndefinedAndMissingSettings(t *testing.T) {
	// Given an existing connection with no bootstrap.servers.
	query := "INSERT INTO FILE cbor('out') SELECT * FROM TOPIC 'a@empty' OPTIONS(GROUP='g', START=EARLIEST)"
	statement, err := sql.NewParser().Parse(query)
	if err != nil {
		t.Fatal(err)
	}
	// When comparing validation of a defined and an undefined connection.
	for _, tc := range []struct {
		profiles Profiles
		message  string
	}{{Profiles{"empty": {}}, "connection requires bootstrap.servers"}, {nil, "connection alias is not defined"}} {
		_, err := NewPlanner(environment{}).Build(statement, tc.profiles)
		// Then the corrective action matches the actual missing configuration.
		if err == nil || !strings.Contains(err.Error(), tc.message) {
			t.Fatal("conflated connection diagnostic", err)
		}
	}
	// Given a caller-created AST without source locations, then omit false coordinates.
	statement.Source.Position = sql.Position{}
	_, err = NewPlanner(environment{}).Build(statement, nil)
	if err == nil || strings.Contains(err.Error(), "line 0") || strings.Contains(err.Error(), "column 0") {
		t.Fatal("invented location for an AST without source metadata", err)
	}
}
