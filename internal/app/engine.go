package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"

	"github.com/pagrus7/kaferry/internal/config"
	copy "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/file"
	"github.com/pagrus7/kaferry/internal/kafka"
	"github.com/pagrus7/kaferry/internal/plan"
)

type Engine interface {
	Run(context.Context, plan.Plan, config.Worker) error
}
type KafkaFactory interface {
	Open(plan.Endpoint) (kafka.Connection, error)
}
type FileFactory interface {
	OpenSource(plan.Plan, string, config.Worker) (file.Input, error)
	OpenOutput(plan.Plan, string, file.DestinationProgress, config.Worker) (file.OutputSink, error)
}
type Execution interface{ Run(context.Context) error }
type RunnerFactory interface {
	New(copy.Source, copy.Sink, copy.ProgressStore, plan.Plan, config.Worker) Execution
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

func (e *runtimeEngine) Run(ctx context.Context, p plan.Plan, limits config.Worker) error {
	if p.OneOff {
		var err error
		p.Group, err = oneOffGroup()
		if err != nil {
			return err
		}
		e.log.Info("one-off group generated", "group", p.Group)
	}
	sourceEndpoint, err := e.inspect(p.Source)
	if err != nil {
		return err
	}
	defer sourceEndpoint.close()
	destinationEndpoint, err := e.inspect(p.Destination)
	if err != nil {
		return err
	}
	defer destinationEndpoint.close()
	fp, err := p.Fingerprint(sourceEndpoint.clusterID, destinationEndpoint.clusterID)
	if err != nil {
		return &kafka.Conflict{Err: err}
	}
	input, progress, err := e.sourceProgress(p, fp, limits, sourceEndpoint.connection)
	if err != nil {
		return err
	}
	if input != nil {
		defer input.Close()
	}
	sink, output, err := e.openSink(p, fp, limits, destinationEndpoint.connection, progress)
	if err != nil {
		return err
	}
	defer sink.Close()
	var source copy.Source
	var store copy.ProgressStore
	if input != nil {
		input.BindOutput(output)
		source, store = input, input
	} else {
		if p.Window.Upper != nil {
			e.log.Warn("bounded copy can omit late and out-of-order timestamps after a cutoff stop")
		}
		source, store, err = sourceEndpoint.connection.Source(p, fp, limits, output)
		if err != nil {
			return err
		}
		defer source.Close()
	}
	if err := e.runner.New(source, sink, store, p, limits).Run(ctx); err != nil {
		return err
	}
	if p.OneOff && input != nil {
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
	connection kafka.Connection
	clusterID  string
}

func (e *runtimeEngine) inspect(endpoint plan.Endpoint) (inspectedEndpoint, error) {
	if endpoint.Kind != "TOPIC" {
		return inspectedEndpoint{}, nil
	}
	connection, err := e.kafka.Open(endpoint)
	if err != nil {
		return inspectedEndpoint{}, err
	}
	id, err := connection.ClusterID()
	if err != nil {
		connection.Close()
		return inspectedEndpoint{}, err
	}
	return inspectedEndpoint{connection: connection, clusterID: id}, nil
}
func (e inspectedEndpoint) close() {
	if e.connection != nil {
		e.connection.Close()
	}
}

func (e *runtimeEngine) sourceProgress(p plan.Plan, fp string, limits config.Worker, connection kafka.Connection) (file.Input, file.DestinationProgress, error) {
	if p.Source.Kind == "FILE" {
		input, err := e.files.OpenSource(p, fp, limits)
		if err != nil {
			return nil, file.DestinationProgress{}, err
		}
		progress := input.Progress()
		e.log.Info("source progress inspected", "group", p.Group, "exists", progress.Exists)
		return input, progress, nil
	}
	if err := connection.CheckMembers(p.Group); err != nil {
		return nil, file.DestinationProgress{}, err
	}
	saved, err := connection.ReadProgress(p.Group, fp, p.Window, p.CutoffMS)
	if err != nil {
		return nil, file.DestinationProgress{}, err
	}
	e.log.Info("source progress inspected", "group", p.Group, "partitions_with_progress", len(saved))
	progress, err := destinationProgress(saved)
	return nil, progress, err
}

func destinationProgress(saved map[int32]kafka.Progress) (file.DestinationProgress, error) {
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

func (e *runtimeEngine) openSink(p plan.Plan, fp string, limits config.Worker, connection kafka.Connection, progress file.DestinationProgress) (copy.Sink, copy.OutputProgress, error) {
	if p.Destination.Kind == "FILE" {
		output, err := e.files.OpenOutput(p, fp, progress, limits)
		return output, output, err
	}
	sink, err := connection.Sink(limits)
	return sink, nil, err
}
