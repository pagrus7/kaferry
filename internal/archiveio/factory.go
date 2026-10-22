package archiveio

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pagrus7/kaferry/internal/archive/avro"
	"github.com/pagrus7/kaferry/internal/archive/capture"
	"github.com/pagrus7/kaferry/internal/archive/replay"
	"github.com/pagrus7/kaferry/internal/checkpoint"
	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/file"
	"github.com/pagrus7/kaferry/internal/plan"
)

type LegacyFiles interface {
	OpenSource(plan.Plan, string, config.Worker) (file.Input, error)
	OpenOutput(plan.Plan, string, file.DestinationProgress, config.Worker) (file.OutputSink, error)
}
type Factory struct {
	legacy LegacyFiles
	clock  core.Clock
	writer file.AtomicWriter
	log    *slog.Logger
}

func NewFactory(legacy LegacyFiles, clock core.Clock, writer file.AtomicWriter, log *slog.Logger) *Factory {
	return &Factory{legacy: legacy, clock: clock, writer: writer, log: log}
}
func (f *Factory) OpenSource(p plan.Plan, fp string, w config.Worker) (file.Input, error) {
	if p.Source.Format != "avro" {
		return f.legacy.OpenSource(p, fp, w)
	}
	directory, err := checkpointDirectory(w)
	if err != nil {
		return nil, err
	}
	if err = separatePaths(p, directory); err != nil {
		return nil, err
	}
	codec, err := avro.New(w.MaxFrameBytes)
	if err != nil {
		return nil, err
	}
	catalog := replay.NewCatalog(p.Source.Name, codec, replay.NativeFiles{})
	pool, err := replay.NewPool(catalog, replay.Limits{OpenFiles: w.ArchiveReplayOpenFiles, Blocks: w.ArchiveReplayBlocks})
	if err != nil {
		return nil, err
	}
	journal, err := replay.NewJournal(directory, p.Group, replay.NativeGroupFiles{}, f.writer)
	if err != nil {
		_ = pool.Close()
		return nil, &file.Conflict{Err: err}
	}
	session, err := replay.NewSession(replay.Query{Group: p.Group, Fingerprint: fp, Window: p.Window}, f.clock, journal, catalog, pool, w.RecordCap)
	if err != nil {
		return nil, &file.Conflict{Err: errors.Join(err, pool.Close(), journal.Close())}
	}
	if session.PrivateFiles > 0 {
		f.log.Warn("archive snapshot excludes unfinished files", "private_files", session.PrivateFiles)
	}
	return NewSource(session, f.clock, checkpoint.Path(directory, p.Group)), nil
}
func (f *Factory) OpenOutput(p plan.Plan, fp string, progress file.DestinationProgress, w config.Worker) (file.OutputSink, error) {
	if p.Destination.Format != "avro" {
		return f.legacy.OpenOutput(p, fp, progress, w)
	}
	codec, err := avro.New(w.MaxFrameBytes)
	if err != nil {
		return nil, err
	}
	files := capture.NativeFiles{}
	store, err := capture.Open(p.Destination.Name, codec, files, capture.NewJournal(files, f.writer))
	if err != nil {
		return nil, err
	}
	writer, err := capture.New(store, f.clock, capture.Policy{RotateAfterSize: p.Destination.RotateAfterSize, RotateAfterAge: time.Duration(p.Destination.RotateAfterAgeMS) * time.Millisecond, MaxOpenFiles: w.ArchiveCaptureOpenFiles})
	if err != nil {
		return nil, errors.Join(err, store.Close())
	}
	return NewSink(writer, w.RecordCap), nil
}

func checkpointDirectory(w config.Worker) (string, error) {
	var directory string
	if w.CheckpointDir != nil {
		directory = *w.CheckpointDir
	} else {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		directory = filepath.Join(home, ".kaferry", "checkpoints")
	}
	info, err := os.Stat(directory)
	if err != nil {
		return "", &file.Conflict{Err: fmt.Errorf("checkpoint directory must exist; create it for a new job or configure the existing directory to resume: %w", err)}
	}
	if !info.IsDir() {
		return "", &file.Conflict{Err: errors.New("checkpoint path must be a directory")}
	}
	return filepath.EvalSymlinks(directory)
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
		return &file.Conflict{Err: errors.New("checkpoint directory must be outside the source archive")}
	}
	if p.Destination.Kind == "FILE" {
		source, err1 := os.Stat(p.Source.Name)
		destination, err2 := os.Stat(p.Destination.Name)
		if err1 == nil && err2 == nil && os.SameFile(source, destination) {
			return &file.Conflict{Err: errors.New("source and destination are the same archive directory")}
		}
	}
	return nil
}
