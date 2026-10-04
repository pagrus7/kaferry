package app

import (
	"log/slog"

	"kafka-fwd2/internal/config"
	copy "kafka-fwd2/internal/copy"
	"kafka-fwd2/internal/plan"
)

type runnerFactory struct {
	clock    copy.Clock
	logger   *slog.Logger
	progress copy.ProgressFactory
}

func newRunnerFactory(clock copy.Clock, logger *slog.Logger, progress copy.ProgressFactory) RunnerFactory {
	return &runnerFactory{clock: clock, logger: logger, progress: progress}
}

func (f *runnerFactory) New(source copy.Source, sink copy.Sink, store copy.ProgressStore, p plan.Plan, limits config.Worker) Execution {
	return copy.NewRunner(source, sink, store, f.clock, f.logger, p, limits, f.progress.New())
}
