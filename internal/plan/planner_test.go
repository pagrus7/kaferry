package plan

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/sql"
	"github.com/stretchr/testify/require"
)

type environment map[string]string

func (e environment) LookupEnv(name string) (string, bool) { value, ok := e[name]; return value, ok }

var profiles = Profiles{"default": {"bootstrap.servers": "local:9092"}, "prod": {"bootstrap.servers": "prod:9092"}, "staging": {"bootstrap.servers": "staging:9092"}, "analytics": {"bootstrap.servers": "analytics:9092"}}

func build(t *testing.T, input string) Plan {
	t.Helper()
	statement, err := sql.NewParser().Parse(input)
	require.NoError(t, err)
	plan, err := NewPlanner(environment{"DR_PASSWORD": "secret"}).Build(statement, profiles)
	require.NoError(t, err)
	return plan
}

func TestDocumentedExamples(t *testing.T) {
	// Given
	data, err := os.ReadFile("../../design/grammar.md")
	require.NoError(t, err)
	examples := strings.Split(string(data), "```sql\n")[1:]
	require.NotEmpty(t, examples, "no documented SQL examples found")
	for _, part := range examples {
		statement, err := sql.NewParser().Parse(strings.SplitN(part, "```", 2)[0])
		require.NoError(t, err)

		// When planning a documented example with its connection profiles.
		_, err = NewPlanner(environment{"DR_PASSWORD": "secret"}).Build(statement, profiles)

		// Then the example is valid under the documented rules.
		require.NoError(t, err)
	}
}

func TestWindowBoundaries(t *testing.T) {
	// Given
	p := build(t, "INSERT INTO FILE cbor('b') SELECT * FROM FILE cbor('a') WHERE RECORD_TIMESTAMP BETWEEN TIMESTAMP '2026-10-01T09:00:00Z' AND TIMESTAMP '2026-10-01T09:30:00Z' OPTIONS(GROUP=ONE_OFF)")
	w, err := p.Window.Resolve(time.Time{})
	require.NoError(t, err)
	lo := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC).UnixMilli()
	hi := lo + 30*60000
	for _, test := range []struct {
		timestamp int64
		wanted    bool
	}{{lo - 1, false}, {lo, true}, {hi, true}, {hi + 1, false}} {

		// When
		containsGot := w.Contains(test.timestamp)

		// Then
		require.Equal(t, test.wanted, containsGot)
	}
	require.True(t, p.OneOff)
}

func TestNowRemainsSymbolicAndUsesOneInstant(t *testing.T) {
	// Given
	p := build(t, "INSERT INTO TOPIC 'b' SELECT * FROM TOPIC 'a' WHERE RECORD_TIMESTAMP > NOW() - INTERVAL '1' HOUR AND RECORD_TIMESTAMP <= NOW() OPTIONS(GROUP='g', CUTOFF=INTERVAL '0' SECOND)")
	require.True(t, p.Window.Relative())
	require.Zero(t, p.CutoffMS)
	now := time.Date(2026, 10, 3, 0, 0, 0, 123000000, time.UTC)

	// When
	w, err := p.Window.Resolve(now)

	// Then
	require.NoError(t, err)
	require.Equal(t, now.UnixMilli()-3600000+1, *w.Lower)
	require.Equal(t, now.UnixMilli()+1, *w.Upper)
}

func TestInvalidTimeExpressions(t *testing.T) {
	// Given
	filters := []struct{ filter, want string }{
		{"RECORD_TIMESTAMP >= TIMESTAMP '2026-10-01T00:00:00'", "RFC 3339"},
		{"RECORD_TIMESTAMP >= TIMESTAMP '2026-10-01T00:00:00.0000Z'", "RFC 3339"},
		{"RECORD_TIMESTAMP >= TIMESTAMP '2026-10-01T00:00:00,001Z'", "RFC 3339"},
		{"RECORD_TIMESTAMP >= TIMESTAMP '2026-02-30T00:00:00Z'", "RFC 3339"},
		{"RECORD_TIMESTAMP >= NOW() - INTERVAL '-1' SECOND", "non-negative integer"},
		{"RECORD_TIMESTAMP >= NOW() - INTERVAL '9223372036854775807' HOUR", "overflows"},
		{"RECORD_TIMESTAMP >= NOW() + INTERVAL '9223372036854775808' SECOND", "too large"},
		{"RECORD_TIMESTAMP >= NOW() + INTERVAL '9223372036854775' SECOND + INTERVAL '9223372036854775' SECOND", "overflows"},
		{"RECORD_TIMESTAMP >= TIMESTAMP '9999-12-31T23:59:59Z' + INTERVAL '9223372036854775' SECOND", "overflows"},
		{"RECORD_TIMESTAMP >= NOW() AND RECORD_TIMESTAMP < NOW()", "empty"},
		{"RECORD_TIMESTAMP >= NOW() AND RECORD_TIMESTAMP < NOW() - INTERVAL '1' SECOND", "empty"},
		{"RECORD_TIMESTAMP >= NOW() AND RECORD_TIMESTAMP > NOW()", "at most one"},
		{"RECORD_TIMESTAMP >= TIMESTAMP '2026-10-02T00:00:00Z' AND RECORD_TIMESTAMP < TIMESTAMP '2026-10-01T00:00:00Z'", "empty"},
	}
	for _, tc := range filters {
		statement, err := sql.NewParser().Parse("INSERT INTO FILE cbor('b') SELECT * FROM FILE cbor('a') WHERE " + tc.filter + " OPTIONS(GROUP='g')")
		require.NoError(t, err)

		// When
		_, buildErr := NewPlanner(environment{}).Build(statement, nil)

		// Then
		require.Error(t, buildErr)
		require.Contains(t, buildErr.Error(), tc.want)
	}
}

func TestPlannerRejectsOverflowingCutoff(t *testing.T) {
	// Given valid SQL whose upper timestamp plus cutoff exceeds int64 milliseconds.
	statement, err := sql.NewParser().Parse("INSERT INTO TOPIC 'b@staging' SELECT * FROM TOPIC 'a@prod' WHERE RECORD_TIMESTAMP < TIMESTAMP '9999-12-31T23:59:59Z' OPTIONS(GROUP='g', CUTOFF=INTERVAL '9223372036854775' SECOND)")
	require.NoError(t, err)

	// When compiling the copy plan, before any endpoint is opened.
	_, err = NewPlanner(environment{}).Build(statement, profiles)

	// Then the stopping threshold cannot wrap into an earlier time.
	require.Error(t, err)
	require.Contains(t, err.Error(), "overflows")
}

func TestInlinePrecedenceAndSecretRedaction(t *testing.T) {
	// Given
	statement, err := sql.NewParser().Parse("WITH CONNECTION prod AS ('bootstrap.servers'='override:9092', 'sasl.password'=ENV('PASSWORD')) INSERT INTO TOPIC 'b@prod' SELECT * FROM TOPIC 'a@prod' OPTIONS(GROUP='g', START=EARLIEST)")
	require.NoError(t, err)
	secret := "unique-secret-marker"

	// When
	p, err := NewPlanner(environment{"PASSWORD": secret}).Build(statement, profiles)

	// Then
	require.NoError(t, err)
	require.Equal(t, "override:9092", p.Source.Properties.Get("bootstrap.servers"))
	require.Equal(t, secret, p.Source.Properties.Get("sasl.password"))
	require.Equal(t, "prod:9092", profiles["prod"]["bootstrap.servers"])
	for _, format := range []string{"%v", "%+v", "%#v"} {
		require.NotContains(t, fmt.Sprintf(format, p), secret)
	}
	values := p.Source.Properties.Values()
	values["sasl.password"] = "changed"
	require.Equal(t, secret, p.Source.Properties.Get("sasl.password"))
	{
		_, err := NewPlanner(environment{}).Build(statement, profiles)
		require.Error(t, err)
		require.NotContains(t, err.Error(), secret)
	}
}

func TestFingerprintIdentity(t *testing.T) {
	// Given
	base := "INSERT INTO TOPIC 'b@staging' SELECT * FROM TOPIC 'a@prod' WHERE RECORD_TIMESTAMP >= TIMESTAMP '2026-10-01T00:00:00Z' OPTIONS(GROUP='g')"
	p := build(t, base)

	// When
	want, err := p.Fingerprint("source-cluster", "destination-cluster")

	// Then
	require.NoError(t, err)
	q := build(t, strings.Replace(base, "GROUP='g'", "GROUP='h', ON_ERROR=SKIP", 1))
	got, err := q.Fingerprint("source-cluster", "destination-cluster")
	require.NoError(t, err)
	require.Equal(t, want, got)
	q = build(t, strings.Replace(base, "2026-10-01T00:00:00Z", "2026-10-01T01:00:00+01:00", 1))
	got, err = q.Fingerprint("source-cluster", "destination-cluster")
	require.NoError(t, err)
	require.Equal(t, want, got)
	q = build(t, strings.Replace(base, "TIMESTAMP '2026-10-01T00:00:00Z'", "TIMESTAMP '2026-09-30T23:00:00Z' + INTERVAL '1' HOUR", 1))
	got, err = q.Fingerprint("source-cluster", "destination-cluster")
	require.NoError(t, err)
	require.Equal(t, want, got)
	got, err = p.Fingerprint("other-cluster", "destination-cluster")
	require.NoError(t, err)
	require.NotEqual(t, want, got)
	_, err = p.Fingerprint("", "destination-cluster")
	require.Error(t, err)
	self := build(t, "INSERT INTO TOPIC 'a@staging' SELECT * FROM TOPIC 'a@prod' OPTIONS(GROUP='g', START=EARLIEST)")
	_, err = self.Fingerprint("same", "same")
	require.Error(t, err)
}

func TestStrictRFC3339AndPersistedWindow(t *testing.T) {
	// Given
	for _, literal := range []string{"2026-10-01T9:00:00Z", "2026-10-01T09:00:00+24:00", "2026-10-01T09:00:00+01:60", "2026-10-01T09:00:00.0000Z"} {
		_, err := expression(sql.Time{Literal: literal})
		require.Error(t, err)
	}
	lo, hi := int64(100), int64(200)
	spec := WindowSpec{Lower: &Expression{LiteralMS: lo}, Upper: &Expression{Now: true}}
	for _, tc := range []struct {
		window Window
		want   string
	}{
		{Window{Upper: &hi}, "different bounds"},
		{Window{Lower: &lo}, "different bounds"},
		{Window{Lower: &hi, Upper: &lo}, "literal time bound differs"},
		{Window{Lower: &lo, Upper: &lo}, "stored time range is empty"},
	} {

		// When
		validateStoredErr := spec.ValidateStored(tc.window, 0)

		// Then
		require.Error(t, validateStoredErr)
		require.Contains(t, validateStoredErr.Error(), tc.want)
	}
	require.NoError(t, spec.ValidateStored(Window{Lower: &lo, Upper: &hi}, 0))
}

func TestQuotedOneOffGroupRemainsNamed(t *testing.T) {
	for _, quoted := range []bool{false, true} {
		t.Run(fmt.Sprint(quoted), func(t *testing.T) {
			// Given either the ONE_OFF keyword or a quoted group with the same text.
			group := "ONE_OFF"
			if quoted {
				group = "'ONE_OFF'"
			}
			// When building the copy plan.
			p := build(t, "INSERT INTO FILE cbor('b') SELECT * FROM FILE cbor('a') OPTIONS(GROUP="+group+")")
			// Then quoting selects a named group that can resume.
			require.Equal(t, !quoted, p.OneOff)
			if quoted {
				require.Equal(t, "ONE_OFF", p.Group)
			}
		})
	}
}
