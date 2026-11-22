package app

import (
	_ "embed"
	"fmt"
	"io"
)

//go:embed help/overview.txt
var overviewHelp string

//go:embed help/sql.txt
var sqlHelp string

//go:embed help/status.txt
var statusHelp string

func executeHelp(args []string, stdout, stderr io.Writer) int {
	if len(args) == 1 {
		return printCLIText(stdout, stderr, overviewHelp)
	}
	if len(args) == 2 && args[0] == "help" {
		switch args[1] {
		case "--help", "-h":
			return printCLIText(stdout, stderr, overviewHelp)
		case "sql":
			return printCLIText(stdout, stderr, sqlHelp)
		case "status":
			return printCLIText(stdout, stderr, statusHelp)
		}
	}
	fmt.Fprintln(stderr, "Valid help topics: sql, status. Run kaferry help for the overview.")
	return 2
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
