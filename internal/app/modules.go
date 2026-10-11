// Package app composes components. Domain packages do not depend on Fx.
package app

import (
	"log/slog"
	"os"

	"github.com/pagrus7/kaferry/internal/config"
	copy "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/file"
	"github.com/pagrus7/kaferry/internal/kafka"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/pagrus7/kaferry/internal/sql"
	"go.uber.org/fx"
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
var ConfigurationModule = fx.Module("configuration", fx.Provide(func(files config.FileReader) config.Reader {
	return config.NewLoader(files, "")
}))
var PlannerModule = fx.Module("planner", fx.Provide(fx.Annotate(plan.NewPlanner, fx.As(new(plan.Builder)))))

var PlanningModules = fx.Options(EnvironmentModule, SQLModule, ConfigurationModule, PlannerModule)

var ProgressModule = fx.Module("progress", fx.Provide(copy.NewProgressFactory))
var KafkaClientModule = fx.Module("kafka-client", fx.Provide(kafka.NewClients))
var PositioningModule = fx.Module("positioning", fx.Provide(kafka.NewPositionerFactory))
var KafkaModule = fx.Module("kafka", fx.Provide(fx.Annotate(kafka.NewFactory, fx.As(new(KafkaFactory)))))
var CheckpointModule = fx.Module("checkpoint", fx.Provide(func() file.AtomicWriter { return file.DurableWriter{} }))
var PublicationModule = fx.Module("publication", fx.Provide(file.NewPublisher))
var FileModule = fx.Module("file", fx.Provide(fx.Annotate(file.NewFactory, fx.As(new(FileFactory)))))
var RunnerModule = fx.Module("runner", fx.Provide(newRunnerFactory))
var EngineModule = fx.Module("engine", fx.Provide(newEngine))
var CommandModule = fx.Module("command", fx.Provide(newCommand))
var ClockModule = fx.Module("clock", fx.Provide(func() copy.Clock { return copy.RealClock{} }))
var LoggingModule = fx.Module("logging", fx.Provide(func() *slog.Logger { return slog.New(slog.NewJSONHandler(os.Stderr, nil)) }))
var RuntimeModules = fx.Options(PlanningModules, ProgressModule, KafkaClientModule, PositioningModule, KafkaModule, CheckpointModule, PublicationModule, FileModule, RunnerModule, EngineModule, CommandModule, ClockModule, LoggingModule)
