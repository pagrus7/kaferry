package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"time"

	"github.com/pagrus7/kaferry/internal/archiveio"
	"github.com/pagrus7/kaferry/internal/config"
	copy "github.com/pagrus7/kaferry/internal/copy"
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
	OpenSource(plan.Plan, string, config.Worker, int) (archiveio.Input, error)
	OpenOutput(plan.Endpoint, config.Worker) (archiveio.Output, error)
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
	partitionCount, err := validateRouting(ctx, p, sourceEndpoint.connection, destinationEndpoint.connection)
	if err != nil {
		return err
	}
	input, err := e.prepareSource(ctx, p, fp, limits, sourceEndpoint.connection, partitionCount)
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
	if input != nil {
		source, store = input, input
	} else {
		if p.Window.Upper != nil && !p.HasOffsetEnd() && (p.MaxStalenessMS >= 0 || p.DisorderMS() >= 0) {
			e.log.Warn("timestamp completion relies on configured arrival-delay and timestamp-disorder assumptions")
		}
		var sourceProgress copy.ProgressStore
		source, sourceProgress, err = sourceEndpoint.connection.Source(ctx, p, fp, limits, routingValidator(p, destinationEndpoint.connection))
		if err != nil {
			return err
		}
		defer func() {
			stop.Begin()
			stop.Stage("closing Kafka input")
			result = copy.CleanupResult(result, "close Kafka input", source.Close())
		}()
		store = sourceProgress
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	sink, err := e.openSink(ctx, p, limits, destinationEndpoint.connection)
	if err != nil {
		return err
	}
	defer func() {
		stop.Begin()
		stop.Stage("closing output")
		sink.Close()
	}()
	runErr := e.runner.New(source, sink, store, p, limits, stop, observer).Run(ctx)
	if finalizer, ok := sink.(interface{ Finish() error }); ok {
		stop.Begin()
		stop.Stage("publishing archive files")
		runErr = copy.CleanupResult(runErr, "publish archive files", finalizer.Finish())
	}
	if runErr != nil {
		return runErr
	}
	if p.OneOff && input != nil && p.WorkerStopAfterRecords == 0 {
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

func (e *runtimeEngine) prepareSource(ctx context.Context, p plan.Plan, fp string, limits config.Worker, connection kafkaport.Connection, partitionCount int) (archiveio.Input, error) {
	if p.Source.Kind == "FILE" {
		return e.files.OpenSource(p, fp, limits, partitionCount)
	}
	if err := connection.CheckMembers(ctx, p.Group, kafkaport.GroupProtocol(p, fp)); err != nil {
		return nil, err
	}
	saved, err := connection.ReadProgress(ctx, p.Group, fp, p.Window, p.MaxStalenessMS)
	if err != nil {
		return nil, err
	}
	e.log.Info("source progress inspected", "group", p.Group, "partitions_with_progress", len(saved))
	return nil, nil
}

func (e *runtimeEngine) openSink(ctx context.Context, p plan.Plan, limits config.Worker, connection kafkaport.Connection) (copy.Sink, error) {
	if p.Destination.Kind == "FILE" {
		return e.files.OpenOutput(p.Destination, limits)
	}
	return connection.Sink(ctx, limits)
}

func routingValidator(p plan.Plan, destination kafkaport.Connection) kafkaport.PartitionValidator {
	if p.Destination.PreservesPartitions() {
		return destination
	}
	return nil
}

func validateRouting(ctx context.Context, p plan.Plan, source, destination kafkaport.Connection) (int, error) {
	var ids []int32
	if p.Source.Kind == "TOPIC" && (p.Partitions != nil || p.Destination.PreservesPartitions()) {
		available, err := source.Partitions(ctx)
		if err != nil {
			return 0, err
		}
		ids, err = p.Partitions.Select(available)
		if err != nil {
			return 0, &kafkaport.Conflict{Err: err}
		}
	}
	if !p.Destination.PreservesPartitions() {
		return 0, nil
	}
	count, err := destination.DestinationPartitions(ctx)
	if err != nil {
		return 0, err
	}
	if p.Source.Kind == "TOPIC" {
		if err = plan.CheckPartitions(ids, count); err != nil {
			return 0, &kafkaport.Conflict{Err: err}
		}
	}
	return count, nil
}
