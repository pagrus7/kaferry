package app

import (
	_ "embed"
	"fmt"
	"io"
	"regexp"
	"runtime/debug"
	"strings"
)

//go:embed help/overview.txt
var overviewHelp string

//go:embed help/sql.txt
var sqlHelp string

//go:embed help/status.txt
var statusHelp string

func executeHelp(args []string, stdout, stderr io.Writer) int {
	if len(args) == 1 {
		return printCLIText(stdout, stderr, helpTopic(overviewHelp))
	}
	if len(args) == 2 && args[0] == "help" {
		switch args[1] {
		case "--help", "-h":
			return printCLIText(stdout, stderr, helpTopic(overviewHelp))
		case "sql":
			return printCLIText(stdout, stderr, helpTopic(sqlHelp))
		case "status":
			return printCLIText(stdout, stderr, helpTopic(statusHelp))
		case "run", "explain":
			return printCLIText(stdout, stderr, commandHelp(args[1]))
		}
	}
	fmt.Fprintln(stderr, "Valid help topics: sql, status, run, explain. Run kaferry help for an overview.")
	return 2
}

var helpReleaseTag = regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+(?:[.-][A-Za-z0-9.-]+)?$`)
var helpPseudoVersion = regexp.MustCompile(`[.-][0-9]{14}-[0-9a-f]{12}$`)

func helpTopic(text string) string {
	// The Go module version distinguishes source at a tag from a candidate build
	// whose linker version names a tag that has yet to be published.
	info, _ := debug.ReadBuildInfo()
	return versionHelpLinks(text, toolchainBuild(info).version)
}

func versionHelpLinks(text, moduleVersion string) string {
	if !helpReleaseTag.MatchString(moduleVersion) || helpPseudoVersion.MatchString(moduleVersion) {
		return text
	}
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		if !strings.HasPrefix(line, "https://github.com/pagrus7/kaferry/") {
			continue
		}
		link := strings.Replace(line, "/main/docs", "/"+moduleVersion+"/docs", 1)
		if len(link) > 80 {
			link, _, _ = strings.Cut(link, "#")
		}
		if len(link) <= 80 {
			lines[i] = link
		}
	}
	return strings.Join(lines, "\n")
}

func printCLIText(stdout, stderr io.Writer, text string) int {
	if _, err := io.WriteString(stdout, text); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

func commandHelp(command string) string {
	usage := "Usage: kaferry " + command + " [options]\n" + `
Choose exactly one SQL input.
  -f, --file PATH      Read SQL from a file, or from stdin with -.
  -e, --sql SQL        Read SQL from the command line.
  -c, --config PATH    Load TOML configuration.
  -h, --help           Show command help.
`
	if command != "run" {
		usage += "  --json               Print JSON.\n"
	}

	example := "kaferry " + command + " -f copy.sql"
	if command == "status" {
		example += " --json"
		usage += "\nThe statement must use a named GROUP.\n"
	}
	return usage + "\nExample: " + example + "\n"
}
