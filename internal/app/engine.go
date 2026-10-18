package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	copy "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/file"
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/observability"
	"github.com/pagrus7/kaferry/internal/plan"
)

type Engine interface {
	Run(context.Context, plan.Plan, config.Config, Shutdown) error
}
type KafkaFactory interface {
	Open(context.Context, plan.Endpoint) (kafkaport.Connection, error)
}
type FileFactory interface {
	OpenSource(plan.Plan, string, config.Worker) (file.Input, error)
	OpenOutput(plan.Plan, string, file.DestinationProgress, config.Worker) (file.OutputSink, error)
}
type Execution interface{ Run(context.Context) error }
type RunnerFactory interface {
	New(copy.Source, copy.Sink, copy.ProgressStore, plan.Plan, config.Worker, copy.StopObserver, copy.Observer) Execution
}

type MetricsFactory interface {
	Open(string) (observability.Session, error)
}

type runtimeEngine struct {
	kafka   KafkaFactory
	files   FileFactory
	runner  RunnerFactory
	log     *slog.Logger
	metrics MetricsFactory
}

func newEngine(kafka KafkaFactory, files FileFactory, runner RunnerFactory, logger *slog.Logger, metrics MetricsFactory) Engine {
	return &runtimeEngine{kafka: kafka, files: files, runner: runner, log: logger, metrics: metrics}
}

func (e *runtimeEngine) Run(ctx context.Context, p plan.Plan, cfg config.Config, stop Shutdown) (result error) {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if p.OneOff {
		p.Group = oneOffGroup()
		e.log.Info("one-off group generated", "group", p.Group)
	}
	var observer copy.Observer
	if cfg.Metrics.Enabled {
		session, err := e.metrics.Open(p.Group)
		if err != nil {
			return &copy.Exit{Err: err, Code: 2}
		}
		observer = session
		defer closeMetrics(session, stop)
	}
	limits := cfg.Worker
	stop.Stage("inspecting Kafka endpoints")
	sourceEndpoint, err := e.inspect(ctx, p.Source, stop)
	if err != nil {
		return err
	}
	defer func() { result = copy.CleanupResult(result, "close source connection", sourceEndpoint.close(stop)) }()
	destinationEndpoint, err := e.inspect(ctx, p.Destination, stop)
	if err != nil {
		return err
	}
	defer func() {
		result = copy.CleanupResult(result, "close destination connection", destinationEndpoint.close(stop))
	}()
	fp, err := p.Fingerprint(sourceEndpoint.clusterID, destinationEndpoint.clusterID)
	if err != nil {
		return &kafkaport.Conflict{Err: err}
	}
	input, progress, err := e.sourceProgress(ctx, p, fp, limits, sourceEndpoint.connection)
	if err != nil {
		return err
	}
	if input != nil {
		defer func() {
			stop.Begin()
			stop.Stage("closing file input")
			result = copy.CleanupResult(result, "close file input", input.Close())
		}()
	}
	var source copy.Source
	var store copy.ProgressStore
	var bindOutput func(copy.OutputProgress)
	if input != nil {
		source, store = input, input
		bindOutput = input.BindOutput
	} else {
		if p.Window.Upper != nil {
			e.log.Warn("bounded copy can omit late and out-of-order timestamps after a cutoff stop")
		}
		var sourceProgress kafkaport.SourceProgress
		source, sourceProgress, err = sourceEndpoint.connection.Source(ctx, p, fp, limits)
		if err != nil {
			return err
		}
		defer func() {
			stop.Begin()
			stop.Stage("closing Kafka input")
			result = copy.CleanupResult(result, "close Kafka input", source.Close())
		}()
		store, bindOutput = sourceProgress, sourceProgress.BindOutput
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	sink, output, err := e.openSink(ctx, p, fp, limits, destinationEndpoint.connection, progress)
	if err != nil {
		return err
	}
	defer func() {
		stop.Begin()
		stop.Stage("closing output")
		sink.Close()
	}()
	bindOutput(output)
	if err := e.runner.New(source, sink, store, p, limits, stop, observer).Run(ctx); err != nil {
		return err
	}
	if p.OneOff && input != nil {
		stop.Begin()
		stop.Stage("deleting one-off checkpoint")
		return input.DeleteCheckpoint()
	}
	return nil
}

func closeMetrics(session observability.Session, stop Shutdown) {
	stop.Begin()
	deadline := minTime(time.Now().Add(time.Second), stop.Deadline().Add(-50*time.Millisecond))
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	session.Close(ctx)
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func oneOffGroup() string {
	var id [16]byte
	rand.Read(id[:])
	return "kaferry-" + hex.EncodeToString(id[:])
}

type inspectedEndpoint struct {
	connection kafkaport.Connection
	clusterID  string
}

func (e *runtimeEngine) inspect(ctx context.Context, endpoint plan.Endpoint, stop copy.StopObserver) (inspectedEndpoint, error) {
	if endpoint.Kind != "TOPIC" {
		return inspectedEndpoint{}, nil
	}
	connection, err := e.kafka.Open(ctx, endpoint)
	if err != nil {
		return inspectedEndpoint{}, err
	}
	id, err := connection.ClusterID(ctx)
	if err != nil {
		opened := inspectedEndpoint{connection: connection}
		return inspectedEndpoint{}, copy.CleanupResult(err, "close Kafka connection", opened.close(stop))
	}
	return inspectedEndpoint{connection: connection, clusterID: id}, nil
}

func (e *inspectedEndpoint) close(stop copy.StopObserver) error {
	if e.connection != nil {
		stop.Begin()
		stop.Stage("closing Kafka connection")
		connection := e.connection
		e.connection = nil
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return connection.Close(ctx)
	}
	return nil
}

func (e *runtimeEngine) sourceProgress(ctx context.Context, p plan.Plan, fp string, limits config.Worker, connection kafkaport.Connection) (file.Input, file.DestinationProgress, error) {
	if p.Source.Kind == "FILE" {
		input, err := e.files.OpenSource(p, fp, limits)
		if err != nil {
			return nil, file.DestinationProgress{}, err
		}
		progress := input.Progress()
		e.log.Info("source progress inspected", "group", p.Group, "exists", progress.Exists)
		return input, progress, nil
	}
	if err := connection.CheckMembers(ctx, p.Group); err != nil {
		return nil, file.DestinationProgress{}, err
	}
	saved, err := connection.ReadProgress(ctx, p.Group, fp, p.Window, p.CutoffMS)
	if err != nil {
		return nil, file.DestinationProgress{}, err
	}
	e.log.Info("source progress inspected", "group", p.Group, "partitions_with_progress", len(saved))
	progress, err := destinationProgress(saved)
	return nil, progress, err
}

func destinationProgress(saved map[int32]kafkaport.Progress) (file.DestinationProgress, error) {
	id, extent, err := kafkaport.FileExtent(saved)
	return file.DestinationProgress{ID: id, Extent: extent, Exists: len(saved) > 0}, err
}

func (e *runtimeEngine) openSink(ctx context.Context, p plan.Plan, fp string, limits config.Worker, connection kafkaport.Connection, progress file.DestinationProgress) (copy.Sink, copy.OutputProgress, error) {
	if p.Destination.Kind == "FILE" {
		output, err := e.files.OpenOutput(p, fp, progress, limits)
		return output, output, err
	}
	sink, err := connection.Sink(ctx, limits)
	return sink, nil, err
}
