package app

import (
	"maps"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/pagrus7/kaferry/src/internal/plan"
	"github.com/pagrus7/kaferry/src/internal/sql"
	"github.com/stretchr/testify/require"
)

func TestHelpSQLMatchesDocumentation(t *testing.T) {
	t.Run("grammar", func(t *testing.T) {
		// Given the canonical EBNF and the embedded SQL topic.
		grammar := readHelpDoc(t, "design/grammar.md")

		// When extracting the two EBNF blocks.
		documented := helpSection(t, grammar, "```ebnf\n", "\n```")
		embedded := helpSection(t, sqlHelp, "SQL statement grammar\n\n", "\n\nDefaults\n")

		// Then the embedded grammar preserves the exact text.
		require.Equal(t, documented, embedded)
	})

	t.Run("defaults", func(t *testing.T) {
		// Given the reference tables and the three scopes in the embedded topic.
		reference := strings.ReplaceAll(readHelpDoc(t, "reference.md"), "`", "")
		const docRow = `(?m)^\| ([A-Z_]+) \| ([^|\n]+) \|`
		const topicRow = `(?m)^  ([A-Z_]+) +\| ([^|\n]+)`

		// When reading the keys and defaults for each scope.
		source := helpTableRows(t, helpSection(t, reference, "#### Kafka source\n", "\n#### Archive source\n"), docRow)
		archive := helpTableRows(t, helpSection(t, reference, "#### Archive source\n", "\n#### Kafka sink\n"), docRow)
		for key, value := range archive {
			if previous, shared := source[key]; shared {
				require.Equal(t, previous, value, "%s has different source defaults", key)
			}
			source[key] = value
		}
		sink := helpTableRows(t, helpSection(t, reference, "#### Kafka sink\n", "\n#### Archive sink\n"), docRow)
		maps.Copy(sink, helpTableRows(t, helpSection(t, reference, "#### Archive sink\n", "\n### Record-error policies\n"), docRow))
		final := helpTableRows(t, helpSection(t, reference, "### Final OPTIONS\n", "\n### SQL errors\n"), docRow)

		// Then each scope lists exactly the reference's keys and defaults.
		require.Equal(t, source, helpTableRows(t, helpSection(t, sqlHelp, "Source WITH (after FROM)\n", "\nSink WITH"), topicRow))
		require.Equal(t, sink, helpTableRows(t, helpSection(t, sqlHelp, "Sink WITH (after INSERT INTO)\n", "\nFinal OPTIONS"), topicRow))
		require.Equal(t, final, helpTableRows(t, helpSection(t, sqlHelp, "Final OPTIONS\n", "\nhttps://"), topicRow))
	})
}

func TestHelpExitCodesMatchReference(t *testing.T) {
	// Given the authoritative CLI table and the overview's short labels.
	reference := readHelpDoc(t, "reference.md")
	table := helpSection(t, reference, "| Normal exit | Meaning |\n", "\n\n")
	labels := helpSection(t, overviewHelp, "Normal exit codes\n", "\nhttps://")

	// When extracting the unique exit codes from both lists.
	documented := helpTableRows(t, table, `(?m)^\| ([0-9]+) \| ([^|\n]+) \|`)
	embedded := helpTableRows(t, labels, `(?m)^  ([0-9]+)  ([^\n]+)`)

	// Then each code appears once in each list.
	require.Equal(t, slices.Sorted(maps.Keys(documented)), slices.Sorted(maps.Keys(embedded)))
}

func TestHelpSQLExamplesParseAndPlan(t *testing.T) {
	// Given the indented SQL blocks actually shipped in the help topics.
	blocks := regexp.MustCompile(`(?m)^  (?:WITH CONNECTION |INSERT INTO )[^\n]+(?:\n  [^\n]+)*`)
	var examples []string
	for _, topic := range []string{overviewHelp, sqlHelp, statusHelp} {
		examples = append(examples, blocks.FindAllString(topic, -1)...)
	}
	require.Len(t, examples, 4)

	for index, example := range examples {
		// When parsing and planning the example without a broker or TOML file.
		statement, err := sql.NewParser().Parse(example)
		require.NoError(t, err, "example %d", index+1)
		_, err = plan.NewPlanner(nil).Build(statement, nil)

		// Then the statement is valid and supplies its own connection.
		require.NoError(t, err, "example %d", index+1)
	}
}

func readHelpDoc(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile("../../../docs/" + name)
	require.NoError(t, err)
	return string(data)
}

func helpSection(t *testing.T, text, start, end string) string {
	t.Helper()
	_, rest, found := strings.Cut(text, start)
	require.True(t, found, "missing start %q", start)
	section, _, found := strings.Cut(rest, end)
	require.True(t, found, "missing end %q after %q", end, start)
	require.NotEmpty(t, section, "empty section after %q", start)
	return section
}

func helpTableRows(t *testing.T, text, pattern string) map[string]string {
	t.Helper()
	rows := make(map[string]string)
	for _, match := range regexp.MustCompile(pattern).FindAllStringSubmatch(text, -1) {
		require.NotContains(t, rows, match[1], "duplicate row %q", match[1])
		rows[match[1]] = strings.TrimSpace(match[2])
	}
	require.NotEmpty(t, rows)
	return rows
}
