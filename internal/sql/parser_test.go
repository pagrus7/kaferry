package sql

import (
	"os"
	"strings"
	"testing"
)

func TestDocumentedExamples(t *testing.T) {
	// Given
	data, err := os.ReadFile("../../design/grammar.md")
	if err != nil {
		t.Fatal(err)
	}
	examples := strings.Split(string(data), "```sql\n")[1:]
	if len(examples) != 5 {
		t.Fatalf("expected five documented examples, got %d", len(examples))
	}
	for _, example := range examples {
		statement := strings.SplitN(example, "```", 2)[0]

		// When
		_, parseErr := NewParser().Parse(statement)

		// Then
		if parseErr != nil {
			t.Fatalf("documented example failed: %v", parseErr)
		}
	}
}

func TestLexicalRules(t *testing.T) {
	// Given
	input := "-- header\ninsert /* block */ into file CBOR('a''b.cbor') select * from file cbor('源.cbor') options(group='ONE_OFF'); -- tail"

	// When
	s, err := NewParser().Parse(input)

	// Then
	if err != nil {
		t.Fatal(err)
	}
	if s.Destination.Name != "a'b.cbor" || s.Source.Name != "源.cbor" || !s.Options[0].Quoted {
		t.Fatalf("incorrect parse: %+v", s)
	}
}

func TestInvalidStatements(t *testing.T) {
	// Given
	base := "INSERT INTO FILE cbor('b') SELECT * FROM FILE cbor('a') OPTIONS (GROUP = 'g')"
	cases := []string{
		"", base + ";" + base, base + ";;", strings.Replace(base, "SELECT *", "SELECT key", 1),
		strings.Replace(base, "OPTIONS", "WHERE RECORD_TIMESTAMP >= NOW() OR RECORD_TIMESTAMP < NOW() OPTIONS", 1),
		strings.Replace(base, "GROUP = 'g'", "GROUP = 'g',", 1), strings.Replace(base, "cbor('a')", "json('a')", 1),
		base + " /* unfinished", "/* nested /* comment */ */ " + base, base + " '\n", base + " \x00", base + "\xff",
		strings.Replace(base, "GROUP = 'g'", "ON_SOURCE_FILE_CHANGED = CONTINUE", 1),
		strings.Replace(base, "GROUP = 'g'", "CUTOFF = INTERVAL '1' WEEK", 1),
		strings.Replace(base, "OPTIONS", "WHERE RECORD_TIMESTAMP '>=' NOW() OPTIONS", 1),
		strings.Replace(base, "OPTIONS", "WHERE RECORD_TIMESTAMP '<' NOW() OPTIONS", 1),
	}
	for i, input := range cases {

		// When
		_, parseErr := NewParser().Parse(input)

		// Then
		if parseErr == nil {
			t.Errorf("case %d accepted invalid syntax", i)
		}
	}
}

func TestErrorsHavePositionsAndDoNotEchoSecrets(t *testing.T) {
	// Given

	// When
	_, err := NewParser().Parse("INSERT\nINTO TOPIC 'secret-value' SELECT 'password' FROM TOPIC 'a' OPTIONS(GROUP='g')")

	// Then
	if err == nil || !strings.Contains(err.Error(), "line 2, column") || strings.Contains(err.Error(), "password") || strings.Contains(err.Error(), "secret-value") {
		t.Fatalf("unsafe or missing diagnostic: %v", err)
	}
}

func FuzzParse(f *testing.F) {
	for _, seed := range []string{"", "INSERT INTO FILE cbor('b') SELECT * FROM FILE cbor('a') OPTIONS(GROUP=ONE_OFF)", "/*", "'", "NOW()"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) { _, _ = NewParser().Parse(input) })
}
