package archiveio

import (
	"errors"
	"fmt"
	"os"
	"slices"

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
		if err = p.ValidateWindow(saved.Window); err != nil {
			return result, &Conflict{Err: err}
		}
		result.Saved = true
		result.Position = saved.Position
		result.Window = saved.Window
		result.Stop = saved.Stop
		for _, f := range saved.Files {
			if !p.Partitions.Matches(f.File.Identity.Source.Partition) {
				return result, &Conflict{Err: errors.New("replay selection contains an excluded original partition")}
			}
			files = append(files, f.File)
		}
		if !saved.Complete {
			if err = catalog.ValidateHeaders(files); err != nil {
				return result, &Conflict{Err: err}
			}
		}
	} else {
		selection, err := catalog.Inspect(p.Partitions)
		if err != nil {
			if errors.Is(err, plan.ErrMissingPartition) {
				return result, &core.Exit{Code: 2, Err: err}
			}
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
	if err = p.ValidateWindow(result.Window); err != nil {
		return result, &Conflict{Err: err}
	}
	result.FileCount = len(files)
	seen := map[int32]bool{}
	for _, f := range files {
		result.Size += f.Size
		id := f.Identity.Source.Partition
		if !seen[id] {
			result.Partitions = append(result.Partitions, id)
			seen[id] = true
		}
	}
	slices.Sort(result.Partitions)
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
