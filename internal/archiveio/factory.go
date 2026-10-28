package archiveio

import (
	"context"
	"errors"
	"fmt"
	"github.com/pagrus7/kaferry/internal/archive/avro"
	"github.com/pagrus7/kaferry/internal/archive/replay"
	"github.com/pagrus7/kaferry/internal/checkpoint"
	"github.com/pagrus7/kaferry/internal/delivery"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
)

type Factory struct {
	clock  core.Clock
	writer checkpoint.AtomicWriter
	log    *slog.Logger
}

func NewFactory(clock core.Clock, writer checkpoint.AtomicWriter, log *slog.Logger) *Factory {
	return &Factory{clock: clock, writer: writer, log: log}
}
func (f *Factory) OpenSource(ctx context.Context, s *delivery.Session, w config.Worker, count int) (Input, error) {
	directory, err := checkpointDirectory(w)
	if err != nil {
		return nil, err
	}
	if err = separatePaths(s.Plan, directory); err != nil {
		return nil, err
	}
	// Validate fresh input headers and partition criteria before output can acquire files.
	if _, err = os.Stat(checkpoint.Path(directory, s.Plan.Group) + ".eos"); errors.Is(err, os.ErrNotExist) {
		codec, err := avro.New(w.ArchiveBlockMaxBytes)
		if err != nil {
			return nil, &Conflict{Err: err}
		}
		if _, err = replay.NewCatalog(s.Plan.Source.Name, codec, replay.NativeFiles{}).Inspect(s.Plan.Partitions); err != nil {
			return nil, &Conflict{Err: err}
		}
	}
	b := NewBinding(s, f.writer, f.clock, f.log)
	if err := b.Open(w); err != nil {
		return nil, &Conflict{Err: errors.Join(err, b.Close())}
	}
	source, store, err := b.Source(ctx, w, count)
	if err != nil {
		return nil, &Conflict{Err: errors.Join(err, s.Close(), b.Close())}
	}
	return &replayInput{Source: source, ProgressStore: store, binding: b}, nil
}
func (f *Factory) OpenOutput(s *delivery.Session, w config.Worker, log delivery.Log) (Output, error) {
	backend := NewCapture(s, f.clock, log, f.writer)
	s.Backend = backend
	sink, err := backend.Sink(w)
	if err != nil {
		return nil, err
	}
	return &captureSink{Sink: sink}, nil
}
func (f *Factory) Recover(ctx context.Context, s *delivery.Session, log delivery.Log, stop core.StopObserver) error {
	return NewCapture(s, f.clock, log, f.writer).Recover(ctx, stop)
}

type replayInput struct {
	core.Source
	core.ProgressStore
	binding *Binding
}

func (r *replayInput) Close() error            { return errors.Join(r.Source.Close(), r.binding.Close()) }
func (r *replayInput) DeleteCheckpoint() error { return r.binding.Delete() }

type captureSink struct{ core.Sink }

func (s *captureSink) Finish() error { return s.Sink.(*Sink).Finish() }

func checkpointDirectory(w config.Worker) (string, error) {
	var directory string
	if w.CheckpointDir != nil {
		directory = *w.CheckpointDir
	} else {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", &core.Exit{Code: 2, Err: errors.New("cannot resolve the default checkpoint directory; set HOME or worker.checkpoint_dir")}
		}
		directory = filepath.Join(home, ".kaferry", "checkpoints")
	}
	info, err := os.Stat(directory)
	if err != nil {
		return "", &Conflict{Err: fmt.Errorf("checkpoint directory must exist; create it for a new job or configure the existing directory to resume: %w", err)}
	}
	if !info.IsDir() {
		return "", &Conflict{Err: errors.New("checkpoint path must be a directory")}
	}
	return resolvedPath(directory)
}
func resolvedPath(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if errors.Is(err, os.ErrNotExist) {
		return absolute, nil
	}
	return resolved, err
}
func separatePaths(p plan.Plan, directory string) error {
	root, err := resolvedPath(p.Source.Name)
	if err != nil {
		return err
	}
	relative, err := filepath.Rel(root, directory)
	if err != nil {
		return err
	}
	if relative == "." || relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return &Conflict{Err: errors.New("checkpoint directory must be outside the source archive")}
	}
	if p.Destination.Kind == "ARCHIVE" {
		source, err1 := os.Stat(p.Source.Name)
		destination, err2 := os.Stat(p.Destination.Name)
		if err1 == nil && err2 == nil && os.SameFile(source, destination) {
			return &Conflict{Err: errors.New("source and destination are the same archive directory")}
		}
	}
	return nil
}
