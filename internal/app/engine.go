package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"kafka-fwd2/internal/config"
	copy "kafka-fwd2/internal/copy"
	"kafka-fwd2/internal/file"
	"kafka-fwd2/internal/kafka"
	"kafka-fwd2/internal/plan"
	"log/slog"
	"os"
)

type Engine interface {
	Run(context.Context, plan.Plan, config.Worker) error
}
type KafkaFactory struct {
	clock  copy.Clock
	logger *slog.Logger
}

func NewKafkaFactory(clock copy.Clock, logger *slog.Logger) *KafkaFactory {
	return &KafkaFactory{clock: clock, logger: logger}
}

type runtimeEngine struct {
	kafka   *KafkaFactory
	files   *FileFactory
	tracker *copy.Tracker
}

func newEngine(factory *KafkaFactory, files *FileFactory, tracker *copy.Tracker) Engine {
	return &runtimeEngine{kafka: factory, files: files, tracker: tracker}
}
func (e *runtimeEngine) Run(ctx context.Context, p plan.Plan, limits config.Worker) error {
	if p.OneOff {
		var id [16]byte
		if _, err := rand.Read(id[:]); err != nil {
			return err
		}
		p.Group = "kafka-fwd2-" + hex.EncodeToString(id[:])
		e.kafka.logger.Info("one-off group generated", "group", p.Group)
	}
	if p.Source.Kind == "FILE" && p.Destination.Kind == "FILE" {
		source, sourceErr := os.Stat(p.Source.Name)
		destination, destinationErr := os.Stat(p.Destination.Name)
		if sourceErr == nil && destinationErr == nil && os.SameFile(source, destination) {
			return &file.Conflict{Err: fmt.Errorf("source and destination are the same file")}
		}
	}
	var sourceInspector, destinationInspector *kafka.Inspector
	var sourceCluster, destinationCluster string
	var err error
	if p.Source.Kind == "TOPIC" {
		sourceInspector, err = kafka.NewInspector(p.Source.Properties, p.Source.Name, e.kafka.logger)
		if err != nil {
			return err
		}
		defer sourceInspector.Close()
		sourceCluster, err = sourceInspector.ClusterID()
		if err != nil {
			return err
		}
	}
	if p.Destination.Kind == "TOPIC" {
		destinationInspector, err = kafka.NewInspector(p.Destination.Properties, p.Destination.Name, e.kafka.logger)
		if err != nil {
			return err
		}
		defer destinationInspector.Close()
		destinationCluster, err = destinationInspector.ClusterID()
		if err != nil {
			return err
		}
	}
	fp, err := p.Fingerprint(sourceCluster, destinationCluster)
	if err != nil {
		return &kafka.Conflict{Err: err}
	}
	var source copy.Source
	var sink copy.Sink
	var store copy.ProgressStore
	var checkpoints *file.Checkpoints
	codec, err := file.NewCodec(limits.MaxFrameBytes)
	if err != nil {
		return err
	}
	var expectedID string
	var extent int64
	var existingProgress bool
	if p.Source.Kind == "FILE" {
		checkpoints, err = file.NewCheckpoints(limits.CheckpointDir, p.Group, e.files.writer)
		if err != nil {
			return err
		}
		defer checkpoints.Close()
		reader, err := file.NewSource(p.Source.Name, fp, p.Window, codec, checkpoints, e.files.clock)
		if err != nil {
			return err
		}
		source = reader
		defer reader.Close()
		store = checkpoints
		e.files.logger.Info("source progress inspected", "group", p.Group, "exists", checkpoints.Existing() != nil)
		if saved := checkpoints.Existing(); saved != nil {
			existingProgress = true
			expectedID, extent = saved.DestinationID, saved.Extent
		}
	} else {
		if err = sourceInspector.CheckMembers(p.Group); err != nil {
			return err
		}
		progress, err := sourceInspector.ReadProgress(p.Group, fp, p.Window, p.CutoffMS)
		if err != nil {
			return err
		}
		e.kafka.logger.Info("source progress inspected", "group", p.Group, "partitions_with_progress", len(progress))
		for _, saved := range progress {
			existingProgress = true
			if saved.FileID != "" {
				if expectedID != "" && expectedID != saved.FileID {
					return &file.Conflict{Err: fmt.Errorf("commits name different destination files")}
				}
				expectedID = saved.FileID
			}
			if saved.Extent > extent {
				extent = saved.Extent
			}
		}
	}
	var output copy.OutputProgress
	if p.Destination.Kind == "FILE" {
		if existingProgress && expectedID == "" {
			return &file.Conflict{Err: fmt.Errorf("existing progress has no destination file identity; use a new GROUP and output")}
		}
		destination, err := file.NewOutput(p.Destination.Name, p.Group, fp, expectedID, extent, codec, limits.RecordCap, e.files.logger)
		if err != nil {
			return err
		}
		sink = destination
		output = destination
		defer destination.Close()
	} else {
		destination, err := kafka.NewSink(destinationInspector, p.Destination, limits, e.kafka.clock, e.kafka.logger)
		if err != nil {
			return err
		}
		sink = destination
		defer destination.Close()
	}
	if p.Source.Kind == "TOPIC" {
		if p.Window.Upper != nil {
			e.kafka.logger.Warn("bounded copy can omit late and out-of-order timestamps after a cutoff stop")
		}
		reader, committer, err := kafka.NewSource(sourceInspector, p, fp, limits, e.kafka.clock, e.kafka.logger, output)
		if err != nil {
			return err
		}
		source = reader
		store = committer
		defer reader.Close()
	} else {
		checkpoints.BindOutput(output)
	}
	runner := copy.NewRunner(source, sink, store, e.kafka.clock, e.kafka.logger, p, limits, e.tracker)
	if err = runner.Run(ctx); err != nil {
		return err
	}
	if p.OneOff && checkpoints != nil {
		return checkpoints.Delete()
	}
	return nil
}
