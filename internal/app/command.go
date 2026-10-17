package app

import (
	"context"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	copy "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/inspect"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/pagrus7/kaferry/internal/sql"
)

// Command plans one statement before starting its execution.
type Command interface {
	Run(context.Context, string, string, Shutdown) error
	Explain(context.Context, string, string, Shutdown) (inspect.Report, error)
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
	explainer inspect.Explainer
}

func newCommand(parser sql.Parser, config config.Reader, planner plan.Builder, validator QueryValidator, engine Engine, explainer inspect.Explainer) Command {
	return &command{parser: parser, config: config, planner: planner, validator: validator, engine: engine, explainer: explainer}
}

func (c *command) Run(ctx context.Context, statement, configPath string, stop Shutdown) error {
	p, limits, err := c.prepare(statement, configPath, stop)
	if err != nil {
		return err
	}
	return c.engine.Run(ctx, p, limits, stop)
}

func (c *command) Explain(ctx context.Context, statement, configPath string, stop Shutdown) (inspect.Report, error) {
	p, limits, err := c.prepare(statement, configPath, stop)
	if err != nil {
		return inspect.Report{}, err
	}
	return c.explainer.Explain(ctx, p, limits, stop)
}

func (c *command) prepare(statement, configPath string, stop Shutdown) (plan.Plan, config.Worker, error) {
	stop.Stage("parsing SQL")
	ast, err := c.parser.Parse(statement)
	if err != nil {
		return plan.Plan{}, config.Worker{}, &copy.Exit{Err: err, Code: 2}
	}
	stop.Stage("reading configuration")
	cfg, err := c.config.Load(configPath)
	if err != nil {
		return plan.Plan{}, config.Worker{}, &copy.Exit{Err: err, Code: 2}
	}
	budget, _ := time.ParseDuration(cfg.Worker.ShutdownTimeout)
	stop.Configure(budget)
	stop.Stage("planning query")
	p, err := c.planner.Build(ast, cfg.Connections)
	if err != nil {
		return plan.Plan{}, config.Worker{}, &copy.Exit{Err: err, Code: 2}
	}
	stop.Stage("checking connection settings")
	if err = c.validator.Validate(p, cfg.Worker); err != nil {
		return plan.Plan{}, config.Worker{}, &copy.Exit{Err: err, Code: 2}
	}
	return p, cfg.Worker, nil
}
