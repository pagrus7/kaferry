package app

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"go.uber.org/fx"
	"kafka-fwd2/internal/config"
	"kafka-fwd2/internal/plan"
	"kafka-fwd2/internal/sql"
)

func Execute(ctx context.Context, args []string, stdin io.Reader, stderr io.Writer) int {
	if len(args) == 0 || args[0] != "run" {
		fmt.Fprintln(stderr, "usage: kafka-fwd2 run (--sql statement | --file path | --file -) [--config path]")
		return 2
	}
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var statement, file, configPath string
	flags.StringVar(&statement, "sql", "", "copy statement")
	flags.StringVar(&statement, "e", "", "copy statement")
	flags.StringVar(&file, "file", "", "statement file; - reads stdin")
	flags.StringVar(&file, "f", "", "statement file; - reads stdin")
	flags.StringVar(&configPath, "config", "", "TOML configuration")
	flags.StringVar(&configPath, "c", "", "TOML configuration")
	if err := flags.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	inputs := 0
	flags.Visit(func(f *flag.Flag) {
		if f.Name == "sql" || f.Name == "e" || f.Name == "file" || f.Name == "f" {
			inputs++
		}
	})
	if inputs != 1 || flags.NArg() != 0 {
		fmt.Fprintln(stderr, "exactly one statement input is required")
		return 2
	}
	if file != "" {
		reader := stdin
		if file == "-" && reader == nil {
			fmt.Fprintln(stderr, "stdin is unavailable")
			return 2
		}
		if file != "-" {
			opened, err := os.Open(file)
			if err != nil {
				fmt.Fprintln(stderr, err)
				return 2
			}
			defer opened.Close()
			reader = opened
		}
		data, err := io.ReadAll(io.LimitReader(reader, (1<<20)+1))
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 2
		}
		statement = string(data)
	}
	if len(statement) > 1<<20 {
		fmt.Fprintln(stderr, "statement exceeds 1 MiB")
		return 2
	}
	var parser sql.Parser
	var planner *plan.Planner
	var loader *config.Loader
	var engine Engine
	graph := fx.New(fx.NopLogger, RuntimeModules, fx.Populate(&parser, &planner, &loader, &engine))
	if err := graph.Err(); err != nil {
		fmt.Fprintln(stderr, "component composition failed")
		return 2
	}
	ast, err := parser.Parse(statement)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	cfg, err := loader.Load(configPath)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	p, err := planner.Build(ast, cfg.Connections)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	if err = engine.Run(ctx, p, cfg.Worker); err != nil {
		fmt.Fprintln(stderr, err)
		var coded interface{ ExitCode() int }
		if errors.As(err, &coded) {
			return coded.ExitCode()
		}
		return 1
	}
	return 0
}
