package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	copy "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/file"
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/plan"
)

type Engine interface {
	Run(context.Context, plan.Plan, config.Worker, copy.StopObserver) error
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
	New(copy.Source, copy.Sink, copy.ProgressStore, plan.Plan, config.Worker, copy.StopObserver) Execution
}

type runtimeEngine struct {
	kafka  KafkaFactory
	files  FileFactory
	runner RunnerFactory
	log    *slog.Logger
}

func newEngine(kafka KafkaFactory, files FileFactory, runner RunnerFactory, logger *slog.Logger) Engine {
	return &runtimeEngine{kafka: kafka, files: files, runner: runner, log: logger}
}

func (e *runtimeEngine) Run(ctx context.Context, p plan.Plan, limits config.Worker, stop copy.StopObserver) (result error) {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if p.OneOff {
		var err error
		p.Group, err = oneOffGroup()
		if err != nil {
			return err
		}
		e.log.Info("one-off group generated", "group", p.Group)
	}
	stop.Stage("inspecting Kafka endpoints")
	sourceEndpoint, err := e.inspect(ctx, p.Source, stop)
	if err != nil {
		return err
	}
	defer func() { result = cleanupResult(result, "close source connection", sourceEndpoint.close(stop)) }()
	destinationEndpoint, err := e.inspect(ctx, p.Destination, stop)
	if err != nil {
		return err
	}
	defer func() {
		result = cleanupResult(result, "close destination connection", destinationEndpoint.close(stop))
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
			result = cleanupResult(result, "close file input", input.Close())
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
			result = cleanupResult(result, "close Kafka input", source.Close())
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
	if err := e.runner.New(source, sink, store, p, limits, stop).Run(ctx); err != nil {
		return err
	}
	if p.OneOff && input != nil {
		stop.Begin()
		stop.Stage("deleting one-off checkpoint")
		return input.DeleteCheckpoint()
	}
	return nil
}

func oneOffGroup() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	return "kaferry-" + hex.EncodeToString(id[:]), nil
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
		return inspectedEndpoint{}, cleanupResult(err, "close Kafka connection", opened.close(stop))
	}
	return inspectedEndpoint{connection: connection, clusterID: id}, nil
}

// Cleanup failures use exit 1 without changing an earlier failure's exit code.
func cleanupResult(result error, stage string, cleanup error) error {
	if cleanup == nil {
		return result
	}
	if result == nil {
		return &copy.Exit{Code: 1, Err: fmt.Errorf("%s: %w", stage, cleanup)}
	}
	var coded interface{ ExitCode() int }
	if !errors.As(result, &coded) {
		result = &copy.Exit{Code: 1, Err: result}
	}
	return errors.Join(result, fmt.Errorf("%s: %w", stage, cleanup))
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
	progress := file.DestinationProgress{Exists: len(saved) > 0}
	for _, partition := range saved {
		if partition.FileID != "" {
			if progress.ID != "" && progress.ID != partition.FileID {
				return progress, &file.Conflict{Err: fmt.Errorf("commits name different destination files")}
			}
			progress.ID = partition.FileID
		}
		progress.Extent = max(progress.Extent, partition.Extent)
	}
	return progress, nil
}

func (e *runtimeEngine) openSink(ctx context.Context, p plan.Plan, fp string, limits config.Worker, connection kafkaport.Connection, progress file.DestinationProgress) (copy.Sink, copy.OutputProgress, error) {
	if p.Destination.Kind == "FILE" {
		output, err := e.files.OpenOutput(p, fp, progress, limits)
		return output, output, err
	}
	sink, err := connection.Sink(ctx, limits)
	return sink, nil, err
}
