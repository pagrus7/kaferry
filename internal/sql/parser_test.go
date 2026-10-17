package sql

import (
	"errors"
	"os"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestDocumentedExamples(t *testing.T) {
	// Given
	data, err := os.ReadFile("../../design/grammar.md")
	if err != nil {
		t.Fatal(err)
	}
	examples := strings.Split(string(data), "```sql\n")[1:]
	if len(examples) == 0 {
		t.Fatal("no documented SQL examples found")
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
	f.Fuzz(func(t *testing.T, input string) {
		_, err := NewParser().Parse(input)
		_, changedErr := NewParser().Parse(swapInputText(input))
		if (err == nil) != (changedErr == nil) || err != nil && err.Error() != changedErr.Error() {
			t.Fatalf("diagnostic depends on arbitrary token text: %v vs %v", err, changedErr)
		}
		if err == nil {
			return
		}
		var diagnostic *Diagnostic
		if !errors.As(err, &diagnostic) {
			t.Fatalf("unstructured parser error: %v", err)
		}
		lines := strings.Split(input, "\n")
		position := diagnostic.Position
		if position.Line < 1 || position.Line > len(lines) || position.Column < 1 || position.Column > utf8.RuneCountInString(lines[position.Line-1])+1 {
			t.Fatalf("diagnostic outside input: %v", err)
		}
	})
}

func swapInputText(input string) string {
	changed := []byte(input)
	for i, c := range changed {
		switch {
		case c >= 'a' && c <= 'z':
			changed[i] = c - 'a' + 'A'
		case c >= 'A' && c <= 'Z':
			changed[i] = c - 'A' + 'a'
		case c >= '0' && c <= '9':
			changed[i] = '0' + (c-'0'+1)%10
		}
	}
	return string(changed)
}
