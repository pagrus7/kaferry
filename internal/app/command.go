package app

import (
	"context"

	"github.com/pagrus7/kaferry/internal/config"
	copy "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/pagrus7/kaferry/internal/sql"
)

// Command plans one statement before starting its execution.
type Command interface {
	Run(context.Context, string, string) error
}
type command struct {
	parser  sql.Parser
	config  config.Reader
	planner plan.Builder
	engine  Engine
}

func newCommand(parser sql.Parser, config config.Reader, planner plan.Builder, engine Engine) Command {
	return &command{parser: parser, config: config, planner: planner, engine: engine}
}

func (c *command) Run(ctx context.Context, statement, configPath string) error {
	ast, err := c.parser.Parse(statement)
	if err != nil {
		return &copy.Exit{Err: err, Code: 2}
	}
	cfg, err := c.config.Load(configPath)
	if err != nil {
		return &copy.Exit{Err: err, Code: 2}
	}
	p, err := c.planner.Build(ast, cfg.Connections)
	if err != nil {
		return &copy.Exit{Err: err, Code: 2}
	}
	return c.engine.Run(ctx, p, cfg.Worker)
}
