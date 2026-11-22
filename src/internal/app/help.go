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
		return printHelp(stdout, stderr, overviewHelp)
	}
	if len(args) == 2 && args[0] == "help" {
		switch args[1] {
		case "--help", "-h":
			return printHelp(stdout, stderr, overviewHelp)
		case "sql":
			return printHelp(stdout, stderr, sqlHelp)
		case "status":
			return printHelp(stdout, stderr, statusHelp)
		}
	}
	fmt.Fprintln(stderr, "Valid help topics: sql, status. Run kaferry help for the overview.")
	return 2
}

func printHelp(stdout, stderr io.Writer, help string) int {
	if _, err := io.WriteString(stdout, help); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
