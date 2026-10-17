package file

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/pagrus7/kaferry/internal/config"
	copy "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
)

type DestinationProgress struct {
	ID     string
	Extent int64
	Exists bool
}

// Input owns the source and its exclusively locked checkpoint store.
type Input interface {
	copy.Source
	copy.ProgressStore
	BindOutput(copy.OutputProgress)
	Progress() DestinationProgress
	DeleteCheckpoint() error
}

type OutputSink interface {
	copy.Sink
	copy.OutputProgress
}

type Factory struct {
	clock     copy.Clock
	logger    *slog.Logger
	writer    AtomicWriter
	publisher Publisher
}

func NewFactory(clock copy.Clock, logger *slog.Logger, writer AtomicWriter, publisher Publisher) *Factory {
	return &Factory{clock: clock, logger: logger, writer: writer, publisher: publisher}
}

func (f *Factory) OpenSource(p plan.Plan, fp string, limits config.Worker) (Input, error) {
	if p.Destination.Kind == "FILE" {
		source, sourceErr := os.Stat(p.Source.Name)
		destination, destinationErr := os.Stat(p.Destination.Name)
		if sourceErr == nil && destinationErr == nil && os.SameFile(source, destination) {
			return nil, conflict("source and destination are the same file")
		}
	}
	codec, err := NewCodec(limits.MaxFrameBytes)
	if err != nil {
		return nil, err
	}
	directory, err := checkpointDirectory(limits)
	if err != nil {
		return nil, err
	}
	checkpoints, err := NewCheckpoints(directory, p.Group, f.writer)
	if err != nil {
		return nil, err
	}
	source, err := NewSource(p.Source.Name, fp, p.Window, codec, checkpoints, f.clock)
	if err != nil {
		checkpoints.Close()
		return nil, err
	}
	return &input{Source: source, checkpoints: checkpoints}, nil
}

func checkpointDirectory(limits config.Worker) (string, error) {
	if limits.CheckpointDir != nil {
		return *limits.CheckpointDir, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", conflict("cannot resolve the default checkpoint directory; set HOME or worker.checkpoint_dir")
	}
	return filepath.Join(home, ".kaferry", "checkpoints"), nil
}

func (f *Factory) OpenOutput(p plan.Plan, fp string, progress DestinationProgress, limits config.Worker) (OutputSink, error) {
	if progress.Exists && progress.ID == "" {
		return nil, conflict("existing progress has no destination file identity; use a new GROUP and output")
	}
	codec, err := NewCodec(limits.MaxFrameBytes)
	if err != nil {
		return nil, err
	}
	return NewOutput(f.publisher, p.Destination.Name, p.Group, fp, progress.ID, progress.Extent, codec, limits.RecordCap, f.logger)
}

type input struct {
	copy.Source
	checkpoints CheckpointStore
}

func (i *input) Save(ctx context.Context, cursors []copy.Cursor) (map[int32]error, error) {
	return i.checkpoints.Save(ctx, cursors)
}
func (i *input) BindOutput(output copy.OutputProgress) { i.checkpoints.BindOutput(output) }
func (i *input) DeleteCheckpoint() error               { return i.checkpoints.Delete() }
func (i *input) Progress() DestinationProgress {
	if saved := i.checkpoints.Existing(); saved != nil {
		return DestinationProgress{ID: saved.DestinationID, Extent: saved.Extent, Exists: true}
	}
	return DestinationProgress{}
}
func (i *input) Close() error {
	defer i.checkpoints.Close()
	return i.Source.Close()
}
