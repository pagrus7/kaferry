package sql

import (
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

func TestLexicalRules(t *testing.T) {
	// Given
	input := "-- header\ninsert /* block */ into ArChIvE 'a''b.avro' select * from ARCHIVE '源.avro' WITH (group='ONE_OFF') ; -- tail"

	// When
	s, err := NewParser().Parse(input)

	// Then
	require.NoError(t, err)
	require.Equal(t, "ARCHIVE", s.Destination.Kind)
	require.Equal(t, "ARCHIVE", s.Source.Kind)
	require.Equal(t, "avro", s.Source.Format)
	require.Equal(t, (Position{Line: 2, Column: 33}), s.Destination.Position)
	require.Equal(t, "a'b.avro", s.Destination.Name)
	require.Equal(t, "源.avro", s.Source.Name)
	require.True(t, s.Source.Options[0].Quoted)
}

func TestInvalidStatements(t *testing.T) {
	// Given
	base := "INSERT INTO ARCHIVE 'b' SELECT * FROM ARCHIVE 'a' WITH (GROUP = 'g') "
	cases := []string{
		"", base + ";" + base, base + ";;", strings.Replace(base, "SELECT *", "SELECT key", 1),
		base + " WHERE RECORD_TIMESTAMP >= NOW() OR RECORD_TIMESTAMP < NOW()",
		strings.Replace(base, "GROUP = 'g'", "GROUP = 'g',", 1), strings.Replace(base, "ARCHIVE 'a'", "ARCHIVE json('a')", 1),
		base + " /* unfinished", "/* nested /* comment */ */ " + base, base + " '\n", base + " \x00", base + "\xff",
		strings.Replace(base, "GROUP = 'g'", "ON_SOURCE_FILE_CHANGED = CONTINUE", 1),
		strings.Replace(base, "GROUP = 'g'", "CUTOFF = INTERVAL '1' WEEK", 1),
		base + " WHERE RECORD_TIMESTAMP '>=' NOW()",
		base + " WHERE RECORD_TIMESTAMP '<' NOW()",
	}
	for _, input := range cases {

		// When
		_, parseErr := NewParser().Parse(input)

		// Then
		require.Error(t, parseErr)
	}
}

func FuzzParse(f *testing.F) {
	for _, seed := range []string{"", "INSERT INTO ARCHIVE 'b' SELECT * FROM ARCHIVE 'a'", "INSERT INTO TOPIC 'b' SELECT * FROM TOPIC 'a' WHERE RECORD_TIMESTAMP < NOW()", "INSERT INTO ARCHIVE 'b' SELECT * FROM ARCHIVE 'a' WITH (GROUP=ONE_OFF) ", "/*", "'", "NOW()", "INSERT INTO ARCHIVE 'b' SELECT * FROM ARCHIVE 'a' OPTIONS (WORKER_STOP_AFTER_RECORDS=5)"} {
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
