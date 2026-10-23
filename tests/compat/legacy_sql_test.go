//go:build integration

package compat

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/pagrus7/kaferry/internal/sql"
	"github.com/stretchr/testify/require"
)

// Legacy SQL is only for comparing binaries while the CLI grammar changes.
// It does not add compatibility syntax to the application.
func needsLegacySQL(binary string) bool {
	return binary != "" && binary == os.Getenv("KAFERRY_LEGACY_SQL_BINARY")
}

func legacySQL(input string) (string, error) {
	statement, err := sql.NewParser().Parse(input)
	if err != nil {
		return "", err
	}
	type edit struct {
		start, end int
		text       string
	}
	span := func(pos sql.Position) edit {
		line, column, start := 1, 1, len(input)
		for index, r := range input {
			if line == pos.Line && column == pos.Column {
				start = index
				break
			}
			if r == '\n' {
				line++
				column = 1
			} else {
				column++
			}
		}
		depth := 0
		quoted := false
		for i := start; i < len(input); i++ {
			switch input[i] {
			case '\'':
				if quoted && i+1 < len(input) && input[i+1] == '\'' {
					i++
					continue
				}
				quoted = !quoted
			case '(':
				if !quoted {
					depth++
				}
			case ')':
				if !quoted {
					depth--
					if depth == 0 {
						return edit{start: start, end: i + 1}
					}
				}
			}
		}
		return edit{start: start, end: len(input)}
	}
	contents := func(e edit) string { return input[e.start+strings.Index(input[e.start:e.end], "(")+1 : e.end-1] }
	source := span(statement.Source.OptionsPosition)
	options := contents(source)
	edits := []edit{source}
	if statement.OptionsPosition.Line != 0 {
		global := span(statement.OptionsPosition)
		options += "," + contents(global)
		edits = append(edits, global)
	}
	if statement.Destination.OptionsPosition.Line != 0 {
		destination := span(statement.Destination.OptionsPosition)
		if statement.Destination.Kind == "TOPIC" {
			if len(statement.Destination.Options) != 1 || statement.Destination.Options[0].Name != "PARTITIONING" || statement.Destination.Options[0].Value != "KEY" {
				return "", fmt.Errorf("legacy test binary supports only key routing")
			}
		} else {
			rotation := contents(destination)
			destination.start = strings.LastIndex(input[:destination.start], ")")
			destination.text = "," + rotation + ")"
		}
		edits = append(edits, destination)
	} else if statement.Destination.Kind == "TOPIC" {
		return "", fmt.Errorf("legacy test binary needs explicit KEY routing")
	}
	sort.Slice(edits, func(i, j int) bool { return edits[i].start > edits[j].start })
	for _, e := range edits {
		input = input[:e.start] + e.text + input[e.end:]
	}
	return strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(input), ";")) + " OPTIONS(" + options + ");", nil
}

func TestLegacyBinarySQLKeepsTheWorkloadAndScope(t *testing.T) {
	for _, tc := range []struct{ input, want string }{
		{"INSERT INTO TOPIC 'out' WITH(PARTITIONING=KEY) SELECT * FROM TOPIC 'in' WITH(GROUP='g',START=EARLIEST) OPTIONS(WORKER_STOP_AFTER_RECORDS=1);", "INSERT INTO TOPIC 'out'  SELECT * FROM TOPIC 'in'   OPTIONS(GROUP='g',START=EARLIEST,WORKER_STOP_AFTER_RECORDS=1);"},
		{"INSERT INTO FILE avro('out') WITH(ROTATE_AFTER_SIZE=1g) SELECT * FROM FILE avro('in') WITH(GROUP='g') WHERE RECORD_TIMESTAMP < NOW();", "INSERT INTO FILE avro('out',ROTATE_AFTER_SIZE=1g) SELECT * FROM FILE avro('in')  WHERE RECORD_TIMESTAMP < NOW() OPTIONS(GROUP='g');"},
	} {
		// Given a current workload, when adapting it for the previous binary, then change syntax alone.
		got, err := legacySQL(tc.input)
		require.NoError(t, err)
		require.Equal(t, strings.Fields(tc.want), strings.Fields(got))
	}
	_, err := legacySQL("INSERT INTO TOPIC 'out' SELECT * FROM FILE avro('in') WITH(GROUP='g')")
	require.ErrorContains(t, err, "explicit KEY")
}
