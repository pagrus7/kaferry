package app

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/pagrus7/kaferry/internal/config"
	"github.com/pagrus7/kaferry/internal/inspect"
	"github.com/pagrus7/kaferry/internal/sql"
	"go.uber.org/fx"
)

func Execute(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	budget := newShutdownBudget(ctx, config.DefaultShutdownTimeout, forceShutdown)
	defer budget.Done()
	return executeWithShutdown(ctx, args, stdin, stdout, stderr, budget)
}
func executeWithShutdown(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer, budget Shutdown) int {
	if len(args) == 1 && args[0] == "--version" {
		budget.Stage("printing version")
		if _, err := fmt.Fprintln(stdout, versionString()); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		return 0
	}
	budget.Stage("parsing command options")
	options, err := parseArgs(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	budget.Stage("reading SQL input")
	statement, err := readStatement(options, stdin)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	var command Command
	budget.Stage("composing components")
	graph := fx.New(fx.NopLogger, RuntimeModules, fx.Populate(&command))
	if err := graph.Err(); err != nil {
		fmt.Fprintln(stderr, "component composition failed")
		return 2
	}
	if options.command == "explain" {
		var report inspect.Report
		report, err = command.Explain(ctx, statement, options.configPath, budget)
		if err == nil {
			budget.Begin()
			budget.Stage("printing explanation")
			err = renderExplanation(stdout, report, options.json)
		}
	} else {
		err = command.Run(ctx, statement, options.configPath, budget)
	}
	if err != nil {
		budget.Begin()
		budget.Stage("printing error")
		printCommandError(stderr, options.file, err)
		var coded interface{ ExitCode() int }
		if errors.As(err, &coded) {
			return coded.ExitCode()
		}
		return 1
	}
	return 0
}

func printCommandError(output io.Writer, source string, err error) {
	var diagnostic *sql.Diagnostic
	if source != "" && errors.As(err, &diagnostic) && diagnostic.Position.Line != 0 {
		if source == "-" {
			source = "<stdin>"
		}
		fmt.Fprintf(output, "%q: %v\n", source, err)
		return
	}
	fmt.Fprintln(output, err)
}

type commandOptions struct {
	command    string
	json       bool
	statement  string
	file       string
	configPath string
}

func parseArgs(args []string, stderr io.Writer) (commandOptions, error) {
	var options commandOptions
	if len(args) == 0 || (args[0] != "run" && args[0] != "explain") {
		err := fmt.Errorf("usage: kaferry --version | run|explain (--sql statement | --file path | --file -) [--config path]; explain accepts --json")
		fmt.Fprintln(stderr, err)
		return options, err
	}
	options.command = args[0]
	flags := flag.NewFlagSet(options.command, flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&options.statement, "sql", "", "copy statement")
	flags.StringVar(&options.statement, "e", "", "copy statement")
	flags.StringVar(&options.file, "file", "", "statement file; - reads stdin")
	flags.StringVar(&options.file, "f", "", "statement file; - reads stdin")
	flags.StringVar(&options.configPath, "config", "", "TOML configuration")
	flags.StringVar(&options.configPath, "c", "", "TOML configuration")
	if options.command == "explain" {
		flags.BoolVar(&options.json, "json", false, "print a JSON explanation")
	}
	if err := flags.Parse(args[1:]); err != nil {
		return options, err
	}
	inputs := 0
	flags.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "sql", "e", "file", "f":
			inputs++
		}
	})
	if inputs != 1 || flags.NArg() != 0 {
		err := fmt.Errorf("exactly one statement input is required")
		fmt.Fprintln(stderr, err)
		return options, err
	}
	return options, nil
}

func readStatement(options commandOptions, stdin io.Reader) (string, error) {
	statement := options.statement
	if options.file != "" {
		reader := stdin
		if options.file == "-" && reader == nil {
			return "", fmt.Errorf("stdin is unavailable")
		}
		if options.file != "-" {
			opened, err := os.Open(options.file)
			if err != nil {
				return "", err
			}
			defer opened.Close()
			reader = opened
		}
		data, err := io.ReadAll(io.LimitReader(reader, (1<<20)+1))
		if err != nil {
			return "", err
		}
		statement = string(data)
	}
	if len(statement) > 1<<20 {
		return "", fmt.Errorf("statement exceeds 1 MiB")
	}
	return statement, nil
}
