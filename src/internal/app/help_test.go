package app

import (
	"bytes"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

//kaferry:guarantee cli.help
func TestCLIHelpTopics(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"help"}, overviewHelp},
		{[]string{"--help"}, overviewHelp},
		{[]string{"-h"}, overviewHelp},
		{[]string{"help", "--help"}, overviewHelp},
		{[]string{"help", "-h"}, overviewHelp},
		{[]string{"help", "sql"}, sqlHelp},
		{[]string{"help", "status"}, statusHelp},
	} {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			// Given a help invocation without SQL or configuration.
			t.Setenv("HOME", "")
			var stdout, stderr bytes.Buffer

			// When requesting help.
			code := Execute(t.Context(), tc.args, forbiddenInput{}, &stdout, &stderr)

			// Then the requested topic is printed to stdout.
			require.Zero(t, code)
			require.Equal(t, tc.want, stdout.String())
			require.Empty(t, stderr.String())
		})
	}
}

//kaferry:guarantee cli.help
func TestCLIHelpUsageErrors(t *testing.T) {
	const topics = "Valid help topics: sql, status. Run kaferry help for the overview.\n"
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
