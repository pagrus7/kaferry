package app

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/pagrus7/kaferry/internal/archive/capture"
	"github.com/pagrus7/kaferry/internal/archiveio"
	"github.com/pagrus7/kaferry/internal/checkpoint"
	"github.com/pagrus7/kaferry/internal/commitmeta"
	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/delivery"
	"github.com/pagrus7/kaferry/internal/franz"
	"github.com/pagrus7/kaferry/internal/inspect"
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
)

type Options struct {
	Clock         core.Clock
	Stop          core.StopObserver
	Logger        *slog.Logger
	ProgressTopic string
	Hook          func(string, int32)
	Observer      core.Observer
	Clients       franz.Clients
}
type Identity = delivery.Identity
type Frame = delivery.Frame
type archiveEnvelope struct {
	Partition int32
	Version   int
	Identity  Identity
	Frame     *Frame
	Capture   capture.State
}
type replayBinding struct {
	Format   string
	Version  int
	Identity Identity
}

func deliveryFactories(o Options) (*franz.Factory, *archiveio.Factory, RunnerFactory) {
	clock := o.Clock
	if clock == nil {
		clock = core.RealClock{}
	}
	logger := o.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	positioning := kafkaport.NewPositionerFactory(clock, logger, kafkaport.NewOffsetLookupFactory())
	clients := o.Clients
	if clients == nil {
		clients = franz.NewClients()
	}
	return franz.NewFactory(commitmeta.NewCodec(), clients, positioning, clock, logger, kafkaport.NewOffsetLookupFactory()), archiveio.NewFactory(clock, checkpoint.DurableWriter{}, logger), newRunnerFactory(clock, logger, core.NewProgressFactory())
}
func Run(ctx context.Context, p plan.Plan, cfg config.Config, o Options) error {
	cfg.Worker.ProgressTopic = "progress"
	if o.ProgressTopic != "" {
		cfg.Worker.ProgressTopic = o.ProgressTopic
	}
	kafka, files, runner := deliveryFactories(o)
	logger := o.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	engine := newEngine(kafka, files, runner, logger, nil).(*runtimeEngine)
	engine.sessions = func(p plan.Plan, w config.Worker) *delivery.Session {
		s := delivery.New(p, w)
		s.Trace = o.Hook
		return s
	}
	if o.Observer != nil {
		engine.runner = observedRunner{RunnerFactory: runner, observer: o.Observer}
	}
	return engine.Run(ctx, p, cfg, noopShutdown{})
}

type observedRunner struct {
	RunnerFactory
	observer core.Observer
}

func (r observedRunner) New(src core.Source, sink core.Sink, store core.ProgressStore, p plan.Plan, w config.Worker, stop core.StopObserver, _ core.Observer) Execution {
	return r.RunnerFactory.New(src, sink, store, p, w, stop, r.observer)
}
func RecoverLocal(ctx context.Context, p plan.Plan, cfg config.Config, o Options) error {
	p.RecoverLocal = true
	return Run(ctx, p, cfg, o)
}

type Inspection struct{ Options Options }

func (i Inspection) service() *deliveryInspection {
	kafka, _, _ := deliveryFactories(i.Options)
	clock := i.Options.Clock
	if clock == nil {
		clock = core.RealClock{}
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return newDeliveryInspection(kafka, kafka, kafkaport.NewPositionerFactory(clock, logger, kafkaport.NewOffsetLookupFactory()), clock)
}
func (i Inspection) Explain(ctx context.Context, p plan.Plan, w config.Worker, stop core.StopObserver) (inspect.Report, error) {
	w.ProgressTopic = i.Options.ProgressTopic
	return i.service().Explain(ctx, p, w, stop)
}
func (i Inspection) Status(ctx context.Context, p plan.Plan, w config.Worker, stop core.StopObserver) (inspect.StatusReport, error) {
	w.ProgressTopic = i.Options.ProgressTopic
	return i.service().Status(ctx, p, w, stop)
}
func productionReader(t *testing.T, p plan.Plan, cfg config.Config, identity delivery.Identity, topic string) delivery.Reader {
	t.Helper()
	s := delivery.New(p, cfg.Worker)
	s.Identity = identity
	f, _, _ := deliveryFactories(Options{})
	connection, err := f.ForQuery(s).Open(t.Context(), p.Source)
	require.NoError(t, err)
	t.Cleanup(func() { _ = connection.Close(context.Background()) })
	log, err := connection.(kafkaport.DeliveryConnection).ProgressLog(t.Context(), s, topic)
	require.NoError(t, err)
	return delivery.Reader{Session: s, Log: log}
}
