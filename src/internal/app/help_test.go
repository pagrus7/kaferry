package app

import (
	"bytes"
	"os"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

func TestCLIHelpTopics(t *testing.T) {
	for _, tc := range []struct {
		args []string
		file string
	}{
		{[]string{"help"}, "overview.txt"},
		{[]string{"--help"}, "overview.txt"},
		{[]string{"-h"}, "overview.txt"},
		{[]string{"help", "--help"}, "overview.txt"},
		{[]string{"help", "-h"}, "overview.txt"},
		{[]string{"help", "sql"}, "sql.txt"},
		{[]string{"help", "status"}, "status.txt"},
	} {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			// Given a help invocation without SQL or configuration.
			t.Setenv("HOME", "")
			topic, err := os.ReadFile("help/" + tc.file)
			require.NoError(t, err)
			var stdout, stderr bytes.Buffer

			// When requesting help.
			code := Execute(t.Context(), tc.args, forbiddenInput{}, &stdout, &stderr)

			// Then the requested topic is printed to stdout.
			require.Zero(t, code)
			require.Equal(t, string(topic), stdout.String())
			require.Empty(t, stderr.String())
		})
	}
}

func TestCLIHelpUsageErrors(t *testing.T) {
	const topics = "Valid help topics: sql, status, run, explain. Run kaferry help for an overview.\n"
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"no args", nil, overviewHelp},
		{"unknown topic", []string{"help", "nope"}, topics},
		{"extra topic argument", []string{"help", "sql", "status"}, topics},
		{"extra alias argument", []string{"--help", "sql"}, topics},
		{"unknown command", []string{"nope"}, "unknown command. Run kaferry help\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given missing or unrecognized arguments.
			var stdout, stderr bytes.Buffer

			// When calling the CLI.
			code := Execute(t.Context(), tc.args, forbiddenInput{}, &stdout, &stderr)

			// Then the diagnostic goes to stderr with a usage exit code.
			require.Equal(t, 2, code)
			require.Empty(t, stdout.String())
			require.Equal(t, tc.want, stderr.String())
		})
	}
}

func TestCLIHelpOutputFailure(t *testing.T) {
	// Given a closed stdout stream.
	var stderr bytes.Buffer

	// When requesting help.
	code := Execute(t.Context(), []string{"help"}, forbiddenInput{}, closedOutput{}, &stderr)

	// Then report the output failure.
	require.Equal(t, 1, code)
	require.Contains(t, stderr.String(), "output closed")
}

func TestCLICommandHelp(t *testing.T) {
	for _, command := range []string{"run", "explain", "status"} {
		t.Run(command, func(t *testing.T) {
			// Given a command and a request for its options.
			var stdout, stderr bytes.Buffer

			// When requesting command help.
			code := Execute(t.Context(), []string{command, "--help"}, forbiddenInput{}, &stdout, &stderr)

			// Then one usage line, one entry per option and one example go to stdout.
			require.Zero(t, code)
			require.Empty(t, stderr.String())
			help := stdout.String()
			require.True(t, strings.HasPrefix(help, "Usage: kaferry "+command+" [options]\n"))
			require.Equal(t, 1, strings.Count(help, "Usage: "))
			require.Equal(t, 1, strings.Count(help, "\nExample: kaferry "+command+" -f copy.sql"))

			flags := []string{"-f, --file PATH", "-e, --sql SQL", "-c, --config PATH", "-h, --help"}
			if command != "run" {
				flags = append(flags, "--json")
			}
			require.Equal(t, len(flags), strings.Count(help, "\n  -"))
			for _, name := range flags {
				require.Equal(t, 1, strings.Count(help, "\n  "+name+" "), "%s", name)
			}
		})
	}
}

func TestCLICommandFlagErrorUsesStderr(t *testing.T) {
	// Given an unsupported flag.
	var stdout, stderr bytes.Buffer

	// When parsing the command options.
	code := Execute(t.Context(), []string{"run", "--unknown"}, forbiddenInput{}, &stdout, &stderr)

	// Then the error and the compact usage go to stderr.
	require.Equal(t, 2, code)
	require.Empty(t, stdout.String())
	require.Contains(t, stderr.String(), "flag provided but not defined")
	require.Contains(t, stderr.String(), "Usage: kaferry run [options]")
	require.Contains(t, stderr.String(), "-f, --file PATH")
}

func TestCLIHelpCommandAliases(t *testing.T) {
	for _, command := range []string{"run", "explain"} {
		t.Run(command, func(t *testing.T) {
			// Given a statement command with its usual flag help.
			var canonical, alias, stderr bytes.Buffer
			require.Zero(t, Execute(t.Context(), []string{command, "--help"}, forbiddenInput{}, &canonical, &stderr))

			// When asking for help using the command name as a topic.
			code := Execute(t.Context(), []string{"help", command}, forbiddenInput{}, &alias, &stderr)

			// Then both paths print the same command usage to stdout.
			require.Zero(t, code)
			require.Empty(t, stderr.String())
			require.Equal(t, canonical.String(), alias.String())
		})
	}
}

func TestHelpLinksUseReleaseTagWithinWidth(t *testing.T) {
	for _, tc := range []struct {
		name, version, topic, link string
	}{
		{"release", "v0.14.0", overviewHelp, "/blob/v0.14.0/docs/reference.md#exit-codes"},
		{"docs index", "v0.14.0", overviewHelp, "/tree/v0.14.0/docs"},
		{"prerelease", "v0.14.0-rc.1", sqlHelp, "/blob/v0.14.0-rc.1/docs/reference.md\n"},
		{"long tag", "v0.14.0-" + strings.Repeat("1", 60), overviewHelp, "/blob/main/docs/reference.md#exit-codes"},
		{"development", "(devel)", overviewHelp, "/blob/main/docs/reference.md#exit-codes"},
		{"pseudo-version", "v0.14.1-0.20261122052201-cf8f1aa3528f", statusHelp, "/blob/main/docs/reference.md#saved-status"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given an embedded topic and the module version stamped by Go.
			// When rendering the topic.
			rendered := versionHelpLinks(tc.topic, tc.version)

			// Then the documentation link uses the expected destination and fits.
			require.Contains(t, rendered, "https://github.com/pagrus7/kaferry"+tc.link)
			for _, line := range strings.Split(rendered, "\n") {
				require.LessOrEqual(t, utf8.RuneCountInString(line), 80, "%q", line)
			}
		})
	}
}

func TestHelpTopicFormat(t *testing.T) {
	for name, topic := range map[string]string{"overview": overviewHelp, "sql": sqlHelp, "status": statusHelp} {
		t.Run(name, func(t *testing.T) {
			// Given an embedded help topic, when checking its text and dimensions.
			lines := strings.Split(strings.TrimSuffix(topic, "\n"), "\n")

			// Then it fits on an 80-column terminal without control sequences.
			require.NotEmpty(t, strings.TrimSpace(topic))
			require.LessOrEqual(t, len(lines), 80)
			require.NotRegexp(t, "[\\x00-\\x09\\x0b-\\x1f\\x7f]", topic)
			for _, line := range lines {
				require.LessOrEqual(t, utf8.RuneCountInString(line), 80, "%q", line)
			}
		})
	}
}
