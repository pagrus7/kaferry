// Package app composes components. Domain packages do not depend on Fx.
package app

import (
	"log/slog"
	"os"

	"github.com/pagrus7/kaferry/internal/archiveio"
	"github.com/pagrus7/kaferry/internal/checkpoint"
	"github.com/pagrus7/kaferry/internal/commitmeta"
	"github.com/pagrus7/kaferry/internal/config"
	copy "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/franz"
	"github.com/pagrus7/kaferry/internal/inspect"
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/observability"
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
var KafkaClientModule = fx.Module("kafka-client", fx.Provide(franz.NewClients))
var PositioningModule = fx.Module("positioning", fx.Provide(kafkaport.NewOffsetLookupFactory, kafkaport.NewPositionerFactory))
var CommitMetadataModule = fx.Module("commit-metadata", fx.Provide(commitmeta.NewCodec))
var KafkaModule = fx.Module("kafka", fx.Provide(franz.NewFactory,
	func(f *franz.Factory) KafkaFactory { return f },
	func(f *franz.Factory) QueryValidator { return f },
	func(f *franz.Factory) inspect.KafkaFactory { return f },
))
var CheckpointModule = fx.Module("checkpoint", fx.Provide(func() checkpoint.AtomicWriter { return checkpoint.DurableWriter{} }))
var ArchiveModule = fx.Module("archive", fx.Provide(func(clock copy.Clock, writer checkpoint.AtomicWriter, log *slog.Logger) FileFactory {
	return archiveio.NewFactory(clock, writer, log)
}))
var RunnerModule = fx.Module("runner", fx.Provide(newRunnerFactory))
var ObservabilityModule = fx.Module("observability", fx.Provide(func(env *system, log *slog.Logger) MetricsFactory {
	return observability.NewFactory(env, log, currentBuild().version)
}))
var EngineModule = fx.Module("engine", fx.Provide(newEngine))
var FileInspectionModule = fx.Module("file-inspection", fx.Provide(func(clock copy.Clock) inspect.Files { return archiveio.NewInspector(clock) }))
var InspectionModule = fx.Module("inspection", fx.Provide(newDeliveryInspection, func(s *deliveryInspection) inspect.Explainer { return s }, func(s *deliveryInspection) inspect.StatusReader { return s }))
var CommandModule = fx.Module("command", fx.Provide(newCommand))
var ClockModule = fx.Module("clock", fx.Provide(func() copy.Clock { return copy.RealClock{} }))
var LoggingModule = fx.Module("logging", fx.Provide(func() *slog.Logger { return slog.New(slog.NewJSONHandler(os.Stderr, nil)) }))
var RuntimeModules = fx.Options(PlanningModules, ProgressModule, KafkaClientModule, PositioningModule, CommitMetadataModule, KafkaModule, CheckpointModule, ArchiveModule, RunnerModule, ObservabilityModule, EngineModule, FileInspectionModule, InspectionModule, CommandModule, ClockModule, LoggingModule)
