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
func (f *Factory) OpenSource(p plan.Plan, fp string, w config.Worker) (Input, error) {
	directory, err := checkpointDirectory(w)
	if err != nil {
		return nil, err
	}
	if err = separatePaths(p, directory); err != nil {
		return nil, err
	}
	codec, err := avro.New(w.ArchiveBlockMaxBytes)
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
		return nil, &Conflict{Err: err}
	}
	session, err := replay.NewSession(replay.Query{Group: p.Group, Fingerprint: fp, Window: p.Window}, f.clock, journal, catalog, pool, w.RecordCap)
	if err != nil {
		return nil, &Conflict{Err: errors.Join(err, pool.Close(), journal.Close())}
	}
	if session.PrivateFiles > 0 {
		f.log.Warn("archive snapshot excludes unfinished files", "private_files", session.PrivateFiles)
	}
	return NewSource(session, f.clock, checkpoint.Path(directory, p.Group)), nil
}
func (f *Factory) OpenOutput(endpoint plan.Endpoint, w config.Worker) (Output, error) {
	codec, err := avro.New(w.ArchiveBlockMaxBytes)
	if err != nil {
		return nil, err
	}
	files := capture.NativeFiles{}
	store, err := capture.Open(endpoint.Name, codec, files, capture.NewJournal(files, f.writer))
	if err != nil {
		return nil, err
	}
	writer, err := capture.New(store, f.clock, capture.Policy{RotateAfterSize: endpoint.RotateAfterSize, RotateAfterAge: time.Duration(endpoint.RotateAfterAgeMS) * time.Millisecond, MaxOpenFiles: w.ArchiveCaptureOpenFiles})
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
			return "", &Conflict{Err: errors.New("cannot resolve the default checkpoint directory; set HOME or worker.checkpoint_dir")}
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
	if p.Destination.Kind == "FILE" {
		source, err1 := os.Stat(p.Source.Name)
		destination, err2 := os.Stat(p.Destination.Name)
		if err1 == nil && err2 == nil && os.SameFile(source, destination) {
			return &Conflict{Err: errors.New("source and destination are the same archive directory")}
		}
	}
	return nil
}
