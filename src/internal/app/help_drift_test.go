package app

import (
	"maps"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/pagrus7/kaferry/src/internal/inspect"
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
		// Given the reference tables and their matching help sections.
		reference := strings.ReplaceAll(readHelpDoc(t, "reference.md"), "`", "")
		const docRow = `(?m)^\| ([A-Z0-9_]+) \| ([^|\n]+) \|`
		const topicRow = `(?m)^  ([A-Z0-9_]+) {2,}([^\n]+)`
		for _, scope := range []struct {
			title, docStart, docEnd, topicEnd string
		}{
			{"Kafka source", "#### Kafka source", "#### Archive source", "Archive source"},
			{"Archive source", "#### Archive source", "#### Kafka sink", "Sink WITH"},
			{"Kafka sink", "#### Kafka sink", "#### Archive sink", "Archive sink"},
			{"Archive sink", "#### Archive sink", "### Record-error policies", "Final OPTIONS"},
			{"Final OPTIONS", "### Final OPTIONS", "### SQL errors", "https://"},
		} {
			t.Run(scope.title, func(t *testing.T) {
				// When extracting the keys and defaults for this scope.
				documented := helpSection(t, reference, scope.docStart+"\n", "\n"+scope.docEnd)
				embedded := helpSection(t, sqlHelp, scope.title+"\n", "\n"+scope.topicEnd)

				// Then the options and defaults match for this endpoint or clause.
				require.Equal(t, helpTableRows(t, documented, docRow), helpTableRows(t, embedded, topicRow))
			})
		}
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

func TestHelpStatusVerdictsMatchInspection(t *testing.T) {
	// Given the verdicts used by status and their descriptions in help and the reference.
	verdicts := inspect.StatusVerdicts()
	reference := strings.ReplaceAll(readHelpDoc(t, "reference.md"), "`", "")
	docTable := helpSection(t, reference, "| Status | Meaning |\n", "\n\n")
	helpTable := helpSection(t, statusHelp, "Verdict                   Exit  Meaning\n", "\n\n")

	// When extracting meanings and exit codes, joining wrapped help lines.
	documented := helpTableRows(t, docTable, `(?m)^\| ([a-z_]+) \| ([^|\n]+) \|$`)
	rows := regexp.MustCompile(`(?m)^([a-z_]+) +([0-9]+) {2,}([^\n]+(?:\n +[^\n]+)*)$`)
	embedded := make(map[string]string, len(verdicts))
	codes := make(map[string]int, len(verdicts))
	for _, match := range rows.FindAllStringSubmatch(helpTable, -1) {
		verdict := match[1]
		require.NotContains(t, embedded, verdict, "duplicate row %q", verdict)
		code, err := strconv.Atoi(match[2])
		require.NoError(t, err)
		codes[verdict] = code
		embedded[verdict] = strings.Join(strings.Fields(match[3]), " ")
	}

	// Then both tables list the exact meanings and help lists the exit codes.
	want := make(map[string]string, len(verdicts))
	for verdict, info := range verdicts {
		want[verdict] = info.Meaning
	}
	require.Equal(t, want, embedded)
	require.Equal(t, want, documented)
	require.Equal(t, inspect.StatusExitCodes(), codes)
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
