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
		{"empty group", files + "OPTIONS(GROUP='')", "''", "GROUP must not be blank; use a quoted name or ONE_OFF"},
		{"duplicate group", files + "OPTIONS(GROUP='g', GROUP='h')", "GROUP='h'", "option GROUP repeats; first declaration at line 3, column 9"},
		{"invalid policy", files + "OPTIONS(GROUP='g', ON_ERROR=RETRY)", "RETRY", "ON_ERROR must be FAIL or SKIP"},
		{"invalid start", topics + "OPTIONS(GROUP='g', START=BEGIN)", "BEGIN", "START must be EARLIEST or LATEST"},
		{"file start", files + "OPTIONS(GROUP='g', START=EARLIEST)", "START", "START requires a topic source; remove START for FILE sources"},
		{"missing start", topics + "OPTIONS(GROUP='g')", "OPTIONS", "topic sources require WHERE or START; add START = EARLIEST or START = LATEST to OPTIONS"},
		{"WHERE and START", topics + "WHERE RECORD_TIMESTAMP < NOW()\nOPTIONS(GROUP='g', START=EARLIEST)", "START", "START cannot be combined with WHERE; remove START"},
		{"CUTOFF file", files + "OPTIONS(GROUP='g', CUTOFF=INTERVAL '1' MINUTE)", "CUTOFF", "CUTOFF requires a topic source; remove CUTOFF for FILE sources"},
		{"CUTOFF no upper", topics + "WHERE RECORD_TIMESTAMP >= NOW()\nOPTIONS(GROUP='g', CUTOFF=INTERVAL '1' MINUTE)", "CUTOFF", "CUTOFF requires an upper timestamp bound in WHERE"},
		{"missing timestamp policy", files + "OPTIONS(GROUP='g', ON_MISSING_TIMESTAMP=SKIP)", "ON_MISSING_TIMESTAMP", "ON_MISSING_TIMESTAMP requires WHERE"},
		{"rejected timestamp policy", files + "OPTIONS(GROUP='g', ON_REJECTED_TIMESTAMP=SKIP)", "ON_REJECTED_TIMESTAMP", "ON_REJECTED_TIMESTAMP requires a topic destination"},
		{"interval too large", files + "WHERE RECORD_TIMESTAMP >= NOW() - INTERVAL '9223372036854775808' SECOND\nOPTIONS(GROUP='g')", "'9223372036854775808'", "interval is too large; reduce its magnitude"},
		{"interval overflow", files + "WHERE RECORD_TIMESTAMP >= NOW() - INTERVAL '9223372036854775807' HOUR\nOPTIONS(GROUP='g')", "'9223372036854775807'", "interval arithmetic overflows milliseconds; reduce its magnitude"},
		{"CUTOFF overflow", topics + "WHERE RECORD_TIMESTAMP < TIMESTAMP '9999-12-31T23:59:59Z'\nOPTIONS(GROUP='g', CUTOFF=INTERVAL '9223372036854775' SECOND)", "CUTOFF", "upper bound plus CUTOFF: timestamp arithmetic overflows milliseconds"},
		{"default connection", "INSERT INTO FILE cbor('out')\nSELECT * FROM TOPIC 'a'\nOPTIONS(GROUP='g', START=EARLIEST)", "'a'", "FROM endpoint: default connection is not defined; add WITH CONNECTION default or TOML [connections.default]"},
		{"conflicting policies", topics + "WHERE RECORD_TIMESTAMP < NOW()\nOPTIONS(GROUP='g', ON_ERROR=SKIP, ON_MISSING_TIMESTAMP=FAIL)", "ON_MISSING_TIMESTAMP", "ON_ERROR cannot be combined with specific error policies; conflicting declaration at line 4, column 20"},
		{"reversed policy conflict", topics + "WHERE RECORD_TIMESTAMP < NOW()\nOPTIONS(GROUP='g', ON_MISSING_TIMESTAMP=FAIL, ON_ERROR=SKIP)", "ON_ERROR", "ON_ERROR cannot be combined with specific error policies; conflicting declaration at line 4, column 20"},
		{"invalid date", files + "WHERE RECORD_TIMESTAMP >= TIMESTAMP '2026-02-30T00:00:00Z'\nOPTIONS(GROUP='g')", "'2026-02-30", "invalid RFC 3339 calendar date or clock time; check day, month and time fields"},
		{"missing zone", files + "WHERE RECORD_TIMESTAMP >= TIMESTAMP '2026-10-01T00:00:00'\nOPTIONS(GROUP='g')", "'2026-10-01", "timestamp must be RFC 3339 with an explicit zone and at most 3 fractional digits, for example '2026-10-01T09:00:00Z'"},
		{"negative interval", files + "WHERE RECORD_TIMESTAMP >= NOW() - INTERVAL '-1' HOUR\nOPTIONS(GROUP='g')", "'-1'", "interval must be a non-negative integer, for example INTERVAL '7' DAY"},
		{"duplicate lower bound", files + "WHERE RECORD_TIMESTAMP >= NOW() AND RECORD_TIMESTAMP > NOW()\nOPTIONS(GROUP='g')", "> NOW()", "at most one lower and one upper bound are allowed; first bound at line 3, column 24"},
		{"undefined connection", "INSERT INTO FILE cbor('out')\nSELECT * FROM TOPIC 'a@missing'\nOPTIONS(GROUP='g', START=EARLIEST)", "'a@missing'", "FROM endpoint: connection alias is not defined; add it with WITH CONNECTION or TOML [connections.alias]"},
		{"duplicate alias", "WITH CONNECTION prod AS ('bootstrap.servers'='x'), CONNECTION prod AS ('bootstrap.servers'='y')\n" + files + "OPTIONS(GROUP='g')", "prod AS ('bootstrap.servers'='y')", "connection alias repeats; first declaration at line 1, column 17"},
		{"duplicate property", "WITH CONNECTION prod AS ('private'='x', 'private'='y')\n" + files + "OPTIONS(GROUP='g')", "'private'='y'", "inline connection property repeats; first declaration at line 1, column 26"},
		{"empty property", "WITH CONNECTION prod AS (''='x')\n" + files + "OPTIONS(GROUP='g')", "''", "inline connection property name must not be empty"},
		{"unset env", "WITH CONNECTION prod AS ('sasl.password'=ENV('PRIVATE_ENV'))\n" + files + "OPTIONS(GROUP='g')", "'PRIVATE_ENV'", "ENV(...) variable is unset; set it before running"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given syntactically valid SQL with an invalid clause or value.
			parseQuery := tc.query
			if tc.name == "invalid policy" {
				parseQuery = strings.Replace(parseQuery, "ON_ERROR=RETRY", "ON_ERROR=FAIL", 1)
			}
			if tc.name == "invalid start" {
				parseQuery = strings.Replace(parseQuery, "START=BEGIN", "START=EARLIEST", 1)
			}
			statement, err := sql.NewParser().Parse(parseQuery)
			if tc.name == "invalid policy" && err == nil {
				statement.Options[len(statement.Options)-1].Value = "RETRY"
			}
			if tc.name == "invalid start" && err == nil {
				statement.Options[len(statement.Options)-1].Value = "BEGIN"
			}
			if err != nil {
				t.Fatal(err)
			}
			prefix := tc.query[:strings.Index(tc.query, tc.marker)]
			lines := strings.Split(prefix, "\n")
			position := sql.Position{Line: len(lines), Column: utf8.RuneCountInString(lines[len(lines)-1]) + 1}
			// When planning without opening endpoints.
			connections := profiles
			if tc.name == "default connection" {
				connections = nil
			}
			_, err = NewPlanner(environment{}).Build(statement, connections)
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
	for index, query := range []string{
		"WITH CONNECTION prod AS ('sasl.password'=ENV('" + secret + "'))" + files + "OPTIONS(GROUP='g')",
		"WITH CONNECTION prod AS ('sasl.password'=ENV('PASSWORD'), 'bootstrap.servers'='x')" + files + "OPTIONS(GROUP='g', ON_ERROR=" + secret + ")",
		"WITH CONNECTION prod AS ('" + secret + "'='x', '" + secret + "'='y')" + files + "OPTIONS(GROUP='g')",
		"INSERT INTO FILE cbor('out') SELECT * FROM TOPIC 'a@" + secret + "' OPTIONS(GROUP='g', START=EARLIEST)",
		files + "WHERE RECORD_TIMESTAMP >= TIMESTAMP '" + secret + "' OPTIONS(GROUP='g')",
	} {
		// Given sensitive literals and a resolved environment credential.
		parseQuery := query
		if index == 1 {
			parseQuery = strings.Replace(parseQuery, "ON_ERROR="+secret, "ON_ERROR=FAIL", 1)
		}
		statement, err := sql.NewParser().Parse(parseQuery)
		if index == 1 && err == nil {
			statement.Options[len(statement.Options)-1].Value = secret
		}
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
	if err == nil || err.Error() != "FROM endpoint: connection alias is not defined; add it with WITH CONNECTION or TOML [connections.alias]" {
		t.Fatal("invented location for an AST without source metadata", err)
	}
}

func TestConnectionSettingsReportEmptyValuesAndOrigins(t *testing.T) {
	for _, tc := range []struct{ connection, reason, origin string }{
		{"'sasl.password'='x'", "connection requires bootstrap.servers", "inline connection at line 1, column 17"},
		{"'bootstrap.servers'=''", "bootstrap.servers is empty", "inline connection at line 1, column 17"},
		{"'bootstrap.servers'=ENV('EMPTY')", "bootstrap.servers is empty", "inline connection at line 1, column 17"},
	} {
		// Given an inline connection with missing or empty broker addresses.
		query := "WITH CONNECTION prod AS (" + tc.connection + ")\nINSERT INTO FILE cbor('out')\nSELECT * FROM TOPIC 'a@prod' OPTIONS(GROUP='g', START=EARLIEST)"
		statement, err := sql.NewParser().Parse(query)
		if err != nil {
			t.Fatal(err)
		}
		// When resolving literal or environment-supplied settings.
		_, err = NewPlanner(environment{"EMPTY": ""}).Build(statement, nil)
		// Then state both the actual setting problem and the declaration's location.
		if err == nil || !strings.Contains(err.Error(), tc.reason) || !strings.Contains(err.Error(), tc.origin) {
			t.Fatal("missing settings context", err)
		}
	}
	// Given an invalid configured alias, then identify configuration without echoing it.
	_, err := NewPlanner(environment{}).Build(sql.Statement{}, Profiles{"sensitive-SENTINEL": {}})
	if err == nil || !strings.HasPrefix(err.Error(), "configuration:") || strings.Contains(err.Error(), "sensitive-SENTINEL") {
		t.Fatal("unsafe or ambiguous configuration error", err)
	}
}
