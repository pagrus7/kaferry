package plan

import (
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/sql"
)

type environment map[string]string

func (e environment) LookupEnv(name string) (string, bool) { value, ok := e[name]; return value, ok }

var profiles = Profiles{"default": {"bootstrap.servers": "local:9092"}, "prod": {"bootstrap.servers": "prod:9092"}, "staging": {"bootstrap.servers": "staging:9092"}, "analytics": {"bootstrap.servers": "analytics:9092"}}

func build(t *testing.T, input string) Plan {
	t.Helper()
	statement, err := sql.NewParser().Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := NewPlanner(environment{"DR_PASSWORD": "secret"}).Build(statement, profiles)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func TestDocumentedExamples(t *testing.T) {
	// Given
	data, err := os.ReadFile("../../design/grammar.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range strings.Split(string(data), "```sql\n")[1:] {
		statement, err := sql.NewParser().Parse(strings.SplitN(part, "```", 2)[0])
		if err != nil {
			t.Fatal(err)
		}

		// When planning a documented example with its connection profiles.
		_, err = NewPlanner(environment{"DR_PASSWORD": "secret"}).Build(statement, profiles)

		// Then the example is valid under the documented rules.
		if err != nil {
			t.Fatal(err)
		}

	}
}

func TestInvalidPlanning(t *testing.T) {
	// Given
	cases := []string{
		"INSERT INTO TOPIC 'b' SELECT * FROM TOPIC 'a' OPTIONS(GROUP='g')",
		"INSERT INTO TOPIC 'b' SELECT * FROM TOPIC 'a' WHERE RECORD_TIMESTAMP >= NOW() OPTIONS(GROUP='g', START=EARLIEST)",
		"INSERT INTO FILE cbor('b') SELECT * FROM FILE cbor('a') OPTIONS(GROUP='g', START=EARLIEST)",
		"INSERT INTO FILE cbor('b') SELECT * FROM FILE cbor('a') OPTIONS(GROUP='g', CUTOFF=INTERVAL '1' MINUTE)",
		"INSERT INTO FILE cbor('b') SELECT * FROM FILE cbor('a') OPTIONS(GROUP='g', ON_REJECTED_TIMESTAMP=SKIP)",
		"INSERT INTO FILE cbor('b') SELECT * FROM FILE cbor('a') OPTIONS(GROUP='g', ON_MISSING_TIMESTAMP=SKIP)",
		"INSERT INTO TOPIC 'b' SELECT * FROM TOPIC 'a' WHERE RECORD_TIMESTAMP >= NOW() OPTIONS(GROUP='g', ON_ERROR=SKIP, ON_MISSING_TIMESTAMP=FAIL)",
		"INSERT INTO FILE cbor('b') SELECT * FROM FILE cbor('a') OPTIONS(ON_ERROR=FAIL)",
		"INSERT INTO FILE cbor('b') SELECT * FROM FILE cbor('a') OPTIONS(GROUP='g', GROUP='h')",
		"INSERT INTO FILE cbor('b') SELECT * FROM FILE cbor('a') OPTIONS(GROUP=' ')",
		"INSERT INTO TOPIC 'b@missing' SELECT * FROM TOPIC 'a' OPTIONS(GROUP='g', START=EARLIEST)",
		"INSERT INTO TOPIC 'b' SELECT * FROM TOPIC 'a' OPTIONS(GROUP='g', START=INVALID)",
		"INSERT INTO FILE cbor('b') SELECT * FROM FILE cbor('a') OPTIONS(GROUP='g', ON_ERROR=CONTINUE)",
		"WITH CONNECTION default AS ('bootstrap.servers'='a', 'bootstrap.servers'='b') INSERT INTO TOPIC 'b' SELECT * FROM TOPIC 'a' OPTIONS(GROUP='g', START=EARLIEST)",
		"WITH CONNECTION X AS ('bootstrap.servers'='a'), CONNECTION X AS ('bootstrap.servers'='b') INSERT INTO TOPIC 'b@X' SELECT * FROM TOPIC 'a' OPTIONS(GROUP='g', START=EARLIEST)",
	}
	for i, input := range cases {
		statement, err := sql.NewParser().Parse(input)
		if err == nil {

			// When
			_, err = NewPlanner(environment{}).Build(statement, profiles)
		}

		// Then invalid syntax or semantics are rejected.
		if err == nil {
			t.Errorf("case %d accepted invalid plan", i)
		}
	}
}

func TestWindowBoundaries(t *testing.T) {
	// Given
	p := build(t, "INSERT INTO FILE cbor('b') SELECT * FROM FILE cbor('a') WHERE RECORD_TIMESTAMP BETWEEN TIMESTAMP '2026-10-01T09:00:00Z' AND TIMESTAMP '2026-10-01T09:30:00Z' OPTIONS(GROUP=ONE_OFF)")
	w, err := p.Window.Resolve(time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	lo := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC).UnixMilli()
	hi := lo + 30*60000
	for _, test := range []struct {
		timestamp int64
		wanted    bool
	}{{lo - 1, false}, {lo, true}, {hi, true}, {hi + 1, false}} {

		// When
		containsGot := w.Contains(test.timestamp)

		// Then
		if containsGot != test.wanted {
			t.Errorf("Contains(%d) = %v", test.timestamp, containsGot)
		}
	}
	if !p.OneOff {
		t.Fatal("ONE_OFF was not recognized")
	}
}

func TestNowRemainsSymbolicAndUsesOneInstant(t *testing.T) {
	// Given
	p := build(t, "INSERT INTO TOPIC 'b' SELECT * FROM TOPIC 'a' WHERE RECORD_TIMESTAMP > NOW() - INTERVAL '1' HOUR AND RECORD_TIMESTAMP <= NOW() OPTIONS(GROUP='g', CUTOFF=INTERVAL '0' SECOND)")
	if !p.Window.Relative() || p.CutoffMS != 0 {
		t.Fatal("relative window or explicit zero cutoff lost")
	}
	now := time.Date(2026, 10, 3, 0, 0, 0, 123000000, time.UTC)

	// When
	w, err := p.Window.Resolve(now)

	// Then
	if err != nil {
		t.Fatal(err)
	}
	if *w.Lower != now.UnixMilli()-3600000+1 || *w.Upper != now.UnixMilli()+1 {
		t.Fatalf("wrong normalized bounds: %+v", w)
	}
}

func TestInvalidTimeExpressions(t *testing.T) {
	// Given
	filters := []string{
		"RECORD_TIMESTAMP >= TIMESTAMP '2026-10-01T00:00:00'",
		"RECORD_TIMESTAMP >= TIMESTAMP '2026-10-01T00:00:00.0000Z'",
		"RECORD_TIMESTAMP >= TIMESTAMP '2026-10-01T00:00:00,001Z'",
		"RECORD_TIMESTAMP >= NOW() - INTERVAL '-1' SECOND",
		"RECORD_TIMESTAMP >= NOW() - INTERVAL '9223372036854775807' HOUR",
		"RECORD_TIMESTAMP >= NOW() AND RECORD_TIMESTAMP < NOW()",
		"RECORD_TIMESTAMP >= NOW() AND RECORD_TIMESTAMP < NOW() - INTERVAL '1' SECOND",
		"RECORD_TIMESTAMP >= NOW() AND RECORD_TIMESTAMP > NOW()",
		"RECORD_TIMESTAMP >= TIMESTAMP '2026-10-02T00:00:00Z' AND RECORD_TIMESTAMP < TIMESTAMP '2026-10-01T00:00:00Z'",
	}
	for _, filter := range filters {
		statement, err := sql.NewParser().Parse("INSERT INTO FILE cbor('b') SELECT * FROM FILE cbor('a') WHERE " + filter + " OPTIONS(GROUP='g')")
		if err != nil {
			t.Fatal(err)
		}

		// When
		_, buildErr := NewPlanner(environment{}).Build(statement, nil)

		// Then
		if buildErr == nil {
			t.Errorf("accepted invalid filter: %s", filter)
		}
	}
	if _, err := checkedAdd(math.MaxInt64, 1); err == nil {
		t.Error("positive overflow accepted")
	}
	if _, err := checkedAdd(math.MinInt64, -1); err == nil {
		t.Error("negative overflow accepted")
	}
}

func TestInlinePrecedenceAndSecretRedaction(t *testing.T) {
	// Given
	statement, err := sql.NewParser().Parse("WITH CONNECTION prod AS ('bootstrap.servers'='override:9092', 'sasl.password'=ENV('PASSWORD')) INSERT INTO TOPIC 'b@prod' SELECT * FROM TOPIC 'a@prod' OPTIONS(GROUP='g', START=EARLIEST)")
	if err != nil {
		t.Fatal(err)
	}
	secret := "unique-secret-marker"

	// When
	p, err := NewPlanner(environment{"PASSWORD": secret}).Build(statement, profiles)

	// Then
	if err != nil {
		t.Fatal(err)
	}
	if p.Source.Properties.Get("bootstrap.servers") != "override:9092" || p.Source.Properties.Get("sasl.password") != secret {
		t.Fatal("inline precedence or ENV resolution failed")
	}
	if profiles["prod"]["bootstrap.servers"] != "prod:9092" {
		t.Fatal("planner mutated input profiles")
	}
	if strings.Contains(fmt.Sprint(p), secret) {
		t.Fatal("plan exposes secret in diagnostics")
	}
	values := p.Source.Properties.Values()
	values["sasl.password"] = "changed"
	if p.Source.Properties.Get("sasl.password") != secret {
		t.Fatal("properties are not isolated")
	}
	if _, err := NewPlanner(environment{}).Build(statement, profiles); err == nil || strings.Contains(err.Error(), secret) {
		t.Fatal("unset ENV was accepted or exposed")
	}
}

func TestFingerprintIdentity(t *testing.T) {
	// Given
	base := "INSERT INTO TOPIC 'b@staging' SELECT * FROM TOPIC 'a@prod' WHERE RECORD_TIMESTAMP >= TIMESTAMP '2026-10-01T00:00:00Z' OPTIONS(GROUP='g')"
	p := build(t, base)

	// When
	want, err := p.Fingerprint("source-cluster", "destination-cluster")

	// Then
	if err != nil {
		t.Fatal(err)
	}
	q := build(t, strings.Replace(base, "GROUP='g'", "GROUP='h', ON_ERROR=SKIP", 1))
	got, _ := q.Fingerprint("source-cluster", "destination-cluster")
	if got != want {
		t.Fatal("GROUP or policy changed fingerprint")
	}
	q = build(t, strings.Replace(base, "2026-10-01T00:00:00Z", "2026-10-01T01:00:00+01:00", 1))
	got, _ = q.Fingerprint("source-cluster", "destination-cluster")
	if got != want {
		t.Fatal("equivalent timestamp zones changed fingerprint")
	}
	q = build(t, strings.Replace(base, "TIMESTAMP '2026-10-01T00:00:00Z'", "TIMESTAMP '2026-09-30T23:00:00Z' + INTERVAL '1' HOUR", 1))
	got, _ = q.Fingerprint("source-cluster", "destination-cluster")
	if got != want {
		t.Fatal("equivalent literal arithmetic changed fingerprint")
	}
	got, _ = p.Fingerprint("other-cluster", "destination-cluster")
	if got == want {
		t.Fatal("cluster identity not fingerprinted")
	}
	if _, err := p.Fingerprint("", "destination-cluster"); err == nil {
		t.Fatal("missing cluster accepted")
	}
	self := build(t, "INSERT INTO TOPIC 'a@staging' SELECT * FROM TOPIC 'a@prod' OPTIONS(GROUP='g', START=EARLIEST)")
	if _, err := self.Fingerprint("same", "same"); err == nil {
		t.Fatal("cross-alias self-copy accepted")
	}
}

func FuzzWindowNormalization(f *testing.F) {
	f.Add(int64(0), int64(1), int64(0))
	f.Add(int64(math.MaxInt64), int64(math.MaxInt64), int64(1))
	f.Fuzz(func(t *testing.T, lo, hi, delta int64) {
		s := WindowSpec{Lower: &Expression{LiteralMS: lo, AdjustmentMS: delta}, Upper: &Expression{LiteralMS: hi, AdjustmentMS: delta}}
		w, err := s.Resolve(time.Time{})
		if err != nil {
			return
		}
		if !w.Contains(*w.Lower) || w.Contains(*w.Upper) {
			t.Fatal("half-open window invariant failed")
		}
	})
}

func TestStrictRFC3339AndPersistedWindow(t *testing.T) {
	// Given
	for _, literal := range []string{"2026-10-01T9:00:00Z", "2026-10-01T09:00:00+24:00", "2026-10-01T09:00:00+01:60", "2026-10-01T09:00:00.0000Z"} {
		if _, err := expression(sql.Time{Literal: literal}); err == nil {
			t.Fatal("accepted relaxed timestamp", literal)
		}
	}
	lo, hi := int64(100), int64(200)
	spec := WindowSpec{Lower: &Expression{LiteralMS: lo}, Upper: &Expression{Now: true}}
	for _, window := range []Window{{Upper: &hi}, {Lower: &lo}, {Lower: &hi, Upper: &lo}} {

		// When
		validateStoredErr := spec.ValidateStored(window, 0)

		// Then
		if validateStoredErr == nil {
			t.Fatal("accepted incompatible persisted window")
		}
	}
	if err := spec.ValidateStored(Window{Lower: &lo, Upper: &hi}, 0); err != nil {
		t.Fatal(err)
	}
}
