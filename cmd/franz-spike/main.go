//go:build franzspike

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/pagrus7/kaferry/internal/config"
	"github.com/pagrus7/kaferry/internal/franzspike"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/pagrus7/kaferry/internal/sql"
)

type disk struct{}

func (disk) ReadFile(path string) ([]byte, error) { return os.ReadFile(path) }
func (disk) LookupEnv(key string) (string, bool)  { return os.LookupEnv(key) }
func run() error {
	args := os.Args[1:]
	if len(args) > 0 && args[0] == "run" {
		args = args[1:]
	}
	flags := flag.NewFlagSet("franz-spike", flag.ContinueOnError)
	file := flags.String("file", "-", "SQL file")
	expression := flags.String("sql", "", "SQL")
	cfgPath := flags.String("config", "", "TOML")
	flags.StringVar(expression, "e", "", "SQL")
	flags.StringVar(cfgPath, "c", "", "TOML")
	if err := flags.Parse(args); err != nil {
		return err
	}
	var data []byte
	var err error
	if *expression != "" {
		data = []byte(*expression)
	} else if *file == "-" {
		data, err = io.ReadAll(io.LimitReader(os.Stdin, 1<<20))
	} else {
		data, err = os.ReadFile(*file)
	}
	if err != nil {
		return err
	}
	ast, err := sql.NewParser().Parse(string(data))
	if err != nil {
		return err
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	cfg, err := config.NewLoader(disk{}, home).Load(*cfgPath)
	if err != nil {
		return err
	}
	p, err := plan.NewPlanner(disk{}).Build(ast, cfg.Connections)
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	return franzspike.Execute(ctx, p, cfg.Worker, slog.New(slog.NewJSONHandler(os.Stderr, nil)))
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
