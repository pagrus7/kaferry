package app

import (
	"context"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	copy "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/pagrus7/kaferry/internal/sql"
)

// Command plans one statement before starting its execution.
type Command interface {
	Run(context.Context, string, string, Shutdown) error
}
type QueryValidator interface {
	Validate(plan.Plan, config.Worker) error
}
type command struct {
	parser    sql.Parser
	config    config.Reader
	planner   plan.Builder
	validator QueryValidator
	engine    Engine
}

func newCommand(parser sql.Parser, config config.Reader, planner plan.Builder, validator QueryValidator, engine Engine) Command {
	return &command{parser: parser, config: config, planner: planner, validator: validator, engine: engine}
}

func (c *command) Run(ctx context.Context, statement, configPath string, stop Shutdown) error {
	stop.Stage("parsing SQL")
	ast, err := c.parser.Parse(statement)
	if err != nil {
		return &copy.Exit{Err: err, Code: 2}
	}
	stop.Stage("reading configuration")
	cfg, err := c.config.Load(configPath)
	if err != nil {
		return &copy.Exit{Err: err, Code: 2}
	}
	budget, _ := time.ParseDuration(cfg.Worker.ShutdownTimeout)
	stop.Configure(budget)
	stop.Stage("planning query")
	p, err := c.planner.Build(ast, cfg.Connections)
	if err != nil {
		return &copy.Exit{Err: err, Code: 2}
	}
	stop.Stage("checking connection settings")
	if err = c.validator.Validate(p, cfg.Worker); err != nil {
		return &copy.Exit{Err: err, Code: 2}
	}
	return c.engine.Run(ctx, p, cfg.Worker, stop)
}
