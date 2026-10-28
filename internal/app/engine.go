package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"github.com/pagrus7/kaferry/internal/delivery"
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
type KafkaFactory = kafkaport.Factory
type FileFactory interface {
	OpenSource(context.Context, *delivery.Session, config.Worker, int) (archiveio.Input, error)
	OpenOutput(*delivery.Session, config.Worker, delivery.Log) (archiveio.Output, error)
	Recover(context.Context, *delivery.Session, delivery.Log, copy.StopObserver) error
}
type Execution interface{ Run(context.Context) error }
type RunnerFactory interface {
	New(copy.Source, copy.Sink, copy.ProgressStore, plan.Plan, config.Worker, copy.StopObserver, copy.Observer) Execution
}

type MetricsFactory interface {
	Open(string) (observability.Session, error)
}

type runtimeEngine struct {
	sessions func(plan.Plan, config.Worker) *delivery.Session
	kafka    KafkaFactory
	files    FileFactory
	runner   RunnerFactory
	log      *slog.Logger
	metrics  MetricsFactory
}

func newEngine(kafka KafkaFactory, files FileFactory, runner RunnerFactory, logger *slog.Logger, metrics MetricsFactory) Engine {
	return &runtimeEngine{sessions: delivery.New, kafka: kafka, files: files, runner: runner, log: logger, metrics: metrics}
}

func (e *runtimeEngine) Run(ctx context.Context, p plan.Plan, cfg config.Config, stop Shutdown) (result error) {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if p.RecoverLocal && (p.OneOff || p.Source.Kind != "TOPIC" || p.Destination.Kind != "ARCHIVE") {
		return &copy.Exit{Code: 2, Err: errors.New("local recovery requires Kafka capture and the original named group")}
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
	session := e.sessions(p, limits)
	engine := *e
	engine.kafka = e.kafka.ForQuery(session)
	e = &engine
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
	defer func() {
		stop.Begin()
		stop.Stage("closing output transactions")
		result = copy.CleanupResult(result, "close output transactions", session.Close())
	}()
	fp, err := p.Fingerprint(sourceEndpoint.clusterID, destinationEndpoint.clusterID)
	if err != nil {
		return &kafkaport.Conflict{Err: err}
	}
	partitionCount, err := validateRouting(ctx, p, sourceEndpoint.connection, destinationEndpoint.connection)
	if err != nil {
		return err
	}
	session.Identity = delivery.Identity{Group: p.Group, Fingerprint: fp, SourceCluster: sourceEndpoint.clusterID, DestinationCluster: destinationEndpoint.clusterID}
	for _, item := range []struct {
		endpoint inspectedEndpoint
		target   *[16]byte
	}{{sourceEndpoint, &session.Identity.SourceTopic}, {destinationEndpoint, &session.Identity.DestinationTopic}} {
		if item.endpoint.connection != nil {
			connection, ok := item.endpoint.connection.(kafkaport.DeliveryConnection)
			if !ok {
				return errors.New("kafka endpoint cannot provide delivery authority")
			}
			id, err := connection.TopicID(ctx)
			if err != nil {
				return err
			}
			*item.target = id
		}
	}
	if p.Source.Kind == "TOPIC" && p.Destination.Kind == "TOPIC" && sourceEndpoint.clusterID == destinationEndpoint.clusterID && p.Source.Name == p.Destination.Name {
		return &kafkaport.Conflict{Err: errors.New("source and destination are the same topic")}
	}
	var log delivery.Log
	authority := destinationEndpoint.connection
	if p.Destination.Kind == "ARCHIVE" {
		authority = sourceEndpoint.connection
	}
	if authority != nil {
		if limits.ProgressTopic == p.Destination.Name || limits.ProgressTopic == p.Source.Name && (p.Destination.Kind != "TOPIC" || sourceEndpoint.clusterID == destinationEndpoint.clusterID) {
			return errors.New("progress topic must differ from data topics")
		}
		var err error
		log, err = authority.(kafkaport.DeliveryConnection).ProgressLog(ctx, session, limits.ProgressTopic)
		if err != nil {
			return err
		}
		if err = log.Ensure(ctx, nil); err != nil {
			return err
		}
	}
	if p.RecoverLocal {
		return e.files.Recover(ctx, session, log, stop)
	}
	sink, err := e.openSink(ctx, p, limits, destinationEndpoint.connection, session, log)
	if err != nil {
		return err
	}
	defer func() { stop.Begin(); stop.Stage("closing output"); sink.Close() }()
	var input archiveio.Input
	if p.Source.Kind == "ARCHIVE" {
		input, err = e.files.OpenSource(ctx, session, limits, partitionCount)
		if err != nil {
			return err
		}
		defer func() {
			stop.Begin()
			stop.Stage("closing archive input")
			result = copy.CleanupResult(result, "close output transactions", session.Close())
			result = copy.CleanupResult(result, "close archive input", input.Close())
		}()
	} else {
		if err = sourceEndpoint.connection.CheckMembers(ctx, p.Group, session.Protocol(kafkaport.GroupProtocol(p, fp))); err != nil {
			return err
		}
		saved, err := sourceEndpoint.connection.ReadProgress(ctx, p.Group, fp, p.Window, p.MaxStalenessMS)
		if err != nil {
			return err
		}
		e.log.Info("source progress inspected", "group", p.Group, "partitions_with_progress", len(saved))
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
			result = copy.CleanupResult(result, "close output transactions", session.Close())
			result = copy.CleanupResult(result, "close Kafka input", source.Close())
		}()
		store = sourceProgress
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
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

func (e *runtimeEngine) openSink(ctx context.Context, p plan.Plan, limits config.Worker, connection kafkaport.Connection, s *delivery.Session, log delivery.Log) (copy.Sink, error) {
	if p.Destination.Kind == "ARCHIVE" {
		return e.files.OpenOutput(s, limits, log)
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
