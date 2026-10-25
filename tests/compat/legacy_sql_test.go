//go:build integration

package compat

import (
	"fmt"
	"os"
	"regexp"
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

func binarySQL(binary, input string) (string, error) {
	legacy := needsLegacySQL(binary)
	fileSyntax := false
	for _, path := range strings.Split(os.Getenv("KAFERRY_FILE_SQL_BINARIES"), string(os.PathListSeparator)) {
		fileSyntax = fileSyntax || binary != "" && binary == path
	}
	if !legacy && !fileSyntax {
		return input, nil
	}
	return previousSQL(input, legacy)
}

func previousSQL(input string, legacy bool) (string, error) {
	statement, err := sql.NewParser().Parse(input)
	if err != nil {
		return "", err
	}
	if legacy && statement.Source.OptionsPosition.Line == 0 {
		return "", fmt.Errorf("legacy test binary requires explicit source settings")
	}
	type edit struct {
		start, end int
		text       string
	}
	offset := func(pos sql.Position) int {
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
		return start
	}
	span := func(pos sql.Position) edit {
		start := offset(pos)
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
	archiveKeyword := regexp.MustCompile(`(?i)\bARCHIVE\b`)
	var edits []edit
	for _, endpoint := range []sql.Endpoint{statement.Source, statement.Destination} {
		if endpoint.Kind != "ARCHIVE" {
			continue
		}
		start := offset(endpoint.Position)
		literal := "'" + strings.ReplaceAll(endpoint.Name, "'", "''") + "'"
		keywords := archiveKeyword.FindAllStringIndex(input[:start], -1)
		keyword := keywords[len(keywords)-1][0]
		edits = append(edits, edit{start: keyword, end: start + len(literal), text: "FILE avro(" + literal + ")"})
	}
	options := ""
	if legacy {
		source := span(statement.Source.OptionsPosition)
		options = contents(source)
		edits = append(edits, source)
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
				index := 0
				if statement.Source.Kind == "ARCHIVE" {
					index = 1
				}
				last := &edits[index]
				last.text = strings.TrimSuffix(last.text, ")") + "," + contents(destination) + ")"
			}
			edits = append(edits, destination)
		} else if statement.Destination.Kind == "TOPIC" {
			return "", fmt.Errorf("legacy test binary needs explicit KEY routing")
		}
	}
	sort.Slice(edits, func(i, j int) bool { return edits[i].start > edits[j].start })
	for _, e := range edits {
		input = input[:e.start] + e.text + input[e.end:]
	}
	if legacy {
		return strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(input), ";")) + " OPTIONS(" + options + ");", nil
	}
	return input, nil
}

func TestLegacyBinarySQLKeepsTheWorkloadAndScope(t *testing.T) {
	for _, tc := range []struct{ input, want string }{
		{"INSERT INTO TOPIC 'out' WITH(PARTITIONING=KEY) SELECT * FROM TOPIC 'in' WITH(GROUP='g',START=EARLIEST) OPTIONS(WORKER_STOP_AFTER_RECORDS=1);", "INSERT INTO TOPIC 'out'  SELECT * FROM TOPIC 'in'   OPTIONS(GROUP='g',START=EARLIEST,WORKER_STOP_AFTER_RECORDS=1);"},
		{"INSERT INTO ARCHIVE 'out' WITH(ROTATE_AFTER_SIZE=1g) SELECT * FROM ARCHIVE 'in' WITH(GROUP='g') WHERE RECORD_TIMESTAMP < NOW();", "INSERT INTO FILE avro('out',ROTATE_AFTER_SIZE=1g) SELECT * FROM FILE avro('in')  WHERE RECORD_TIMESTAMP < NOW() OPTIONS(GROUP='g');"},
	} {
		// Given a current workload, when adapting it for the previous binary, then change syntax alone.
		got, err := previousSQL(tc.input, true)
		require.NoError(t, err)
		require.Equal(t, strings.Fields(tc.want), strings.Fields(got))
	}
	for _, tc := range []struct{ input, message string }{
		{"INSERT INTO TOPIC 'out' SELECT * FROM ARCHIVE 'in' WITH(GROUP='g')", "explicit KEY"},
		{"INSERT INTO ARCHIVE 'out' SELECT * FROM ARCHIVE 'in'", "explicit source settings"},
		{"INSERT INTO TOPIC 'out' WITH(PARTITIONING=KEY) SELECT * FROM TOPIC 'in'", "explicit source settings"},
	} {
		// Given a workload the old syntax cannot represent, when adapting, then fail clearly without panicking.
		_, err := previousSQL(tc.input, true)
		require.ErrorContains(t, err, tc.message)
	}
}

func TestFileBinarySQLKeepsArchivePathsAndScopedSettings(t *testing.T) {
	// Given an archive workload and binaries with current or FILE syntax.
	const input = "INSERT INTO ARCHIVE 'outı''put' WITH(ROTATE_AFTER_SIZE=1g)\nSELECT * FROM ARCHIVE '源/in' WITH(GROUP='g') OPTIONS(WORKER_STOP_AFTER_RECORDS=1);"
	const want = "INSERT INTO FILE avro('outı''put') WITH(ROTATE_AFTER_SIZE=1g)\nSELECT * FROM FILE avro('源/in') WITH(GROUP='g') OPTIONS(WORKER_STOP_AFTER_RECORDS=1);"
	t.Setenv("KAFERRY_FILE_SQL_BINARIES", strings.Join([]string{"previous", "baseline"}, string(os.PathListSeparator)))
	t.Setenv("KAFERRY_LEGACY_SQL_BINARY", "")
	for _, binary := range []string{"previous", "baseline", "candidate"} {
		// When preparing SQL for each binary, then adapt only the selected binaries' endpoint syntax.
		got, err := binarySQL(binary, input)
		require.NoError(t, err)
		if binary == "candidate" {
			require.Equal(t, input, got)
		} else {
			require.Equal(t, want, got)
		}
	}
	// Given a topic capture, when adapting for an unscoped binary, then keep rotation with its archive destination.
	got, err := previousSQL("INSERT INTO ARCHIVE 'out' WITH(ROTATE_AFTER_SIZE=1g) SELECT * FROM TOPIC 'in' WITH(GROUP='g')", true)
	require.NoError(t, err)
	require.Equal(t, "INSERT INTO FILE avro('out',ROTATE_AFTER_SIZE=1g)  SELECT * FROM TOPIC 'in' OPTIONS(GROUP='g');", got)
}
