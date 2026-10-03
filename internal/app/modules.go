// Package app composes components. Domain packages do not depend on Fx.
package app

import (
	"log/slog"
	"os"

	"go.uber.org/fx"
	"kafka-fwd2/internal/config"
	copy "kafka-fwd2/internal/copy"
	"kafka-fwd2/internal/file"
	"kafka-fwd2/internal/plan"
	"kafka-fwd2/internal/sql"
)

type system struct{}

func (*system) ReadFile(path string) ([]byte, error) { return os.ReadFile(path) }
func (*system) LookupEnv(name string) (string, bool) { return os.LookupEnv(name) }

var EnvironmentModule = fx.Module("environment", fx.Provide(
	func() *system { return &system{} },
	func(s *system) config.FileReader { return s },
	func(s *system) plan.Environment { return s },
))
var SQLModule = fx.Module("sql", fx.Provide(fx.Annotate(sql.NewParser, fx.As(new(sql.Parser)))))
var ConfigurationModule = fx.Module("configuration", fx.Provide(func(files config.FileReader) (*config.Loader, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	return config.NewLoader(files, home), nil
}))
var PlannerModule = fx.Module("planner", fx.Provide(plan.NewPlanner))

var PlanningModules = fx.Options(EnvironmentModule, SQLModule, ConfigurationModule, PlannerModule)

var ProgressModule = fx.Module("progress", fx.Provide(copy.NewTracker))
var KafkaModule = fx.Module("kafka", fx.Provide(NewKafkaFactory))
var FileModule = fx.Module("file", fx.Provide(NewFileFactory, func() file.AtomicWriter { return file.DurableWriter{} }))
var RunnerModule = fx.Module("runner", fx.Provide(newEngine))
var RuntimeModules = fx.Options(PlanningModules, ProgressModule, KafkaModule, FileModule, RunnerModule, fx.Provide(func() copy.Clock { return copy.RealClock{} }, func() *slog.Logger { return slog.New(slog.NewJSONHandler(os.Stderr, nil)) }))
