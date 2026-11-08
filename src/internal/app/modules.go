// Package app composes components. Domain packages do not depend on Fx.
package app

import (
	"log/slog"
	"os"

	"github.com/pagrus7/kaferry/src/internal/archive/io"
	"github.com/pagrus7/kaferry/src/internal/checkpoint"
	"github.com/pagrus7/kaferry/src/internal/config"
	copy "github.com/pagrus7/kaferry/src/internal/copy"
	"github.com/pagrus7/kaferry/src/internal/inspect"
	"github.com/pagrus7/kaferry/src/internal/kafka"
	"github.com/pagrus7/kaferry/src/internal/kafka/commitmeta"
	"github.com/pagrus7/kaferry/src/internal/kafka/franz"
	"github.com/pagrus7/kaferry/src/internal/observability"
	"github.com/pagrus7/kaferry/src/internal/plan"
	"github.com/pagrus7/kaferry/src/internal/sql"
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
var KafkaClientModule = fx.Module("kafka-client", fx.Provide(franz.NewClients))
var PositioningModule = fx.Module("positioning", fx.Provide(kafka.NewOffsetLookupFactory, kafka.NewPositionerFactory))
var CommitMetadataModule = fx.Module("commit-metadata", fx.Provide(commitmeta.NewCodec))
var KafkaModule = fx.Module("kafka", fx.Provide(franz.NewFactory,
	func(f *franz.Factory) KafkaFactory { return f },
	func(f *franz.Factory) QueryValidator { return f },
	func(f *franz.Factory) inspect.KafkaFactory { return f },
))
var CheckpointModule = fx.Module("checkpoint", fx.Provide(func() checkpoint.AtomicWriter { return checkpoint.DurableWriter{} }))
var ArchiveModule = fx.Module("archive", fx.Provide(func(clock copy.Clock, writer checkpoint.AtomicWriter, log *slog.Logger) ArchiveFactory {
	return archiveio.NewFactory(clock, writer, log)
}))
var RunnerModule = fx.Module("runner", fx.Provide(newRunnerFactory))
var ObservabilityModule = fx.Module("observability", fx.Provide(func(env *system, log *slog.Logger) MetricsFactory {
	return observability.NewFactory(env, log, currentBuild().version)
}))
var EngineModule = fx.Module("engine", fx.Provide(newEngine))
var ArchiveInspectionModule = fx.Module("archive-inspection", fx.Provide(func(clock copy.Clock) inspect.Archives { return archiveio.NewInspector(clock) }))
var InspectionModule = fx.Module("inspection", fx.Provide(inspect.NewExplainer, func(s *inspect.Service) inspect.Explainer { return s }, func(s *inspect.Service) inspect.StatusReader { return s }))
var CommandModule = fx.Module("command", fx.Provide(newCommand))
var ClockModule = fx.Module("clock", fx.Provide(func() copy.Clock { return copy.RealClock{} }))
var LoggingModule = fx.Module("logging", fx.Provide(func() *slog.Logger { return slog.New(slog.NewJSONHandler(os.Stderr, nil)) }))
var RuntimeModules = fx.Options(PlanningModules, ProgressModule, KafkaClientModule, PositioningModule, CommitMetadataModule, KafkaModule, CheckpointModule, ArchiveModule, RunnerModule, ObservabilityModule, EngineModule, ArchiveInspectionModule, InspectionModule, CommandModule, ClockModule, LoggingModule)
