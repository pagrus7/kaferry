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
func (i *Inspector) Source(p plan.Plan, fp string, w config.Worker) (SourceInspection, error) {
	return i.source(p, fp, w, nil, false)
}
func (i *Inspector) SavedSource(p plan.Plan, fp string, w config.Worker, saved *replay.State) (SourceInspection, error) {
	return i.source(p, fp, w, saved, true)
}
func (i *Inspector) source(p plan.Plan, fp string, w config.Worker, saved *replay.State, authority bool) (result SourceInspection, err error) {
	directory, err := checkpointDirectory(w)
	if err != nil {
		return result, err
	}
	if err = separatePaths(p, directory); err != nil {
		return result, err
	}
	result.Exists = true
	if !p.OneOff {
		result.CheckpointPath = checkpoint.Path(directory, p.Group)
		if !authority {
			saved, err = replay.ReadCheckpoint(directory, p.Group, replay.NativeGroupFiles{})
			if err == nil && saved != nil {
				err = errors.New("replay checkpoint lacks delivery authority")
			}
		}
		if err != nil {
			return result, &Conflict{Err: err}
		}
	}
	codec, err := avro.New(w.ArchiveBlockMaxBytes)
	if err != nil {
		return result, err
	}
	inputs := replay.NewCatalog(p.Source.Name, codec, replay.NativeFiles{}).Preview()
	query := replayQuery(p, fp, 0)
	var files []replay.File
	if saved != nil {
		if err = query.CheckSaved(*saved, inputs); err != nil {
			return result, &Conflict{Err: err}
		}
		result.Saved = true
		result.Position = saved.Position
		result.Window = saved.Window
		result.Stop = saved.Stop
		for _, f := range saved.Files {
			files = append(files, f.File)
		}
	} else {
		selection, err := inputs.Select(p.Partitions)
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
		result.Window, err = query.NewWindow(i.clock.Now())
		if err != nil {
			return result, &Conflict{Err: err}
		}
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
