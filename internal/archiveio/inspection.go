package archiveio

import (
	"errors"
	"fmt"
	"os"

	"github.com/pagrus7/kaferry/internal/archive/avro"
	"github.com/pagrus7/kaferry/internal/archive/replay"
	"github.com/pagrus7/kaferry/internal/checkpoint"
	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
)

type Inspector struct {
	clock core.Clock
}

func NewInspector(clock core.Clock) *Inspector {
	return &Inspector{clock: clock}
}
func (i *Inspector) Source(p plan.Plan, fp string, w config.Worker) (result SourceInspection, err error) {
	directory, err := checkpointDirectory(w)
	if err != nil {
		return result, err
	}
	if err = separatePaths(p, directory); err != nil {
		return result, err
	}
	result.Exists = true
	var saved *replay.State
	if !p.OneOff {
		result.CheckpointPath = checkpoint.Path(directory, p.Group)
		saved, err = replay.ReadCheckpoint(directory, p.Group, replay.NativeGroupFiles{})
		if err != nil {
			return result, &Conflict{Err: err}
		}
	}
	codec, err := avro.New(w.ArchiveBlockMaxBytes)
	if err != nil {
		return result, err
	}
	catalog := replay.NewCatalog(p.Source.Name, codec, replay.NativeFiles{})
	var files []replay.File
	if saved != nil {
		if saved.Fingerprint != fp {
			return result, &Conflict{Err: errors.New("replay checkpoint query identity mismatch")}
		}
		if err = p.Window.ValidateStored(saved.Window, 0); err != nil {
			return result, &Conflict{Err: err}
		}
		result.Saved = true
		result.Position = saved.Position
		result.Window = saved.Window
		result.Stop = saved.Stop
		for _, f := range saved.Files {
			files = append(files, f.File)
		}
		if !saved.Complete {
			if err = catalog.ValidateHeaders(files); err != nil {
				return result, &Conflict{Err: err}
			}
		}
	} else {
		selection, err := catalog.Inspect()
		if err != nil {
			return result, err
		}
		files = selection.Files
		if selection.PrivateFiles > 0 {
			result.Warnings = append(result.Warnings, fmt.Sprintf("Snapshot excludes %d unfinished files.", selection.PrivateFiles))
		}
		result.Window, err = p.Window.Resolve(i.clock.Now())
		if err != nil {
			return result, &Conflict{Err: err}
		}
	}
	result.FileCount = len(files)
	for _, f := range files {
		result.Size += f.Size
	}
	return result, nil
}
func (i *Inspector) Destination(path string) (ArchiveInspection, error) {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return ArchiveInspection{}, nil
	}
	if err != nil {
		return ArchiveInspection{}, err
	}
	if !info.IsDir() {
		return ArchiveInspection{}, &Conflict{Err: errors.New("avro destination must be a directory")}
	}
	return ArchiveInspection{Exists: true}, nil
}
