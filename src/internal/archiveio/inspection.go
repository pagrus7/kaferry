package archiveio

import (
	"errors"
	"fmt"
	"os"
	"slices"

	"github.com/pagrus7/kaferry/src/internal/archive/avro"
	"github.com/pagrus7/kaferry/src/internal/archive/replay"
	"github.com/pagrus7/kaferry/src/internal/checkpoint"
	"github.com/pagrus7/kaferry/src/internal/config"
	core "github.com/pagrus7/kaferry/src/internal/copy"
	"github.com/pagrus7/kaferry/src/internal/plan"
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
		files, err = inspectSavedFiles(p, fp, catalog, saved, &result)
	} else {
		files, err = i.inspectFreshFiles(p, catalog, &result)
	}
	if err != nil {
		return result, err
	}

	if saved == nil || saved.Fingerprint == fp {
		if err = p.ValidateWindow(result.Window); err != nil {
			return result, &Conflict{Err: err}
		}
	}
	result.summarizeFiles(files)
	result.Saved = saved != nil
	if saved != nil && saved.Fingerprint != fp {
		return result, &Conflict{Err: plan.ErrFingerprintMismatch}
	}
	return result, nil
}
func (r *SourceInspection) summarizeFiles(files []replay.File) {
	r.FileCount = len(files)
	seen := map[int32]bool{}
	for _, f := range files {
		r.Size += f.Size
		id := f.Identity.Source.Partition
		if !seen[id] {
			r.Partitions = append(r.Partitions, id)
			seen[id] = true
		}
	}
	slices.Sort(r.Partitions)
}

func inspectSavedFiles(p plan.Plan, fp string, catalog *replay.Catalog, saved *replay.State, result *SourceInspection) ([]replay.File, error) {
	var files []replay.File
	result.Fingerprint = saved.Fingerprint
	result.Position = saved.Position
	result.Window = saved.Window
	result.Stop = saved.Stop
	for _, f := range saved.Files {
		if saved.Fingerprint == fp && !p.Partitions.Matches(f.File.Identity.Source.Partition) {
			return nil, &Conflict{Err: errors.New("replay selection contains an excluded original partition")}
		}
		files = append(files, f.File)
	}
	if !saved.Complete {
		if err := catalog.ValidateHeaders(files); err != nil {
			return nil, &Conflict{Err: err}
		}
	}
	return files, nil
}
func (i *Inspector) inspectFreshFiles(p plan.Plan, catalog *replay.Catalog, result *SourceInspection) ([]replay.File, error) {

	selection, checkErr := catalog.Inspect(p.Partitions)
	if checkErr != nil {
		if errors.Is(checkErr, plan.ErrMissingPartition) {
			return nil, &core.Exit{Code: 2, Err: checkErr}
		}
		return nil, checkErr
	}
	files := selection.Files
	if selection.PrivateFiles > 0 {
		result.Warnings = append(result.Warnings, fmt.Sprintf("Snapshot excludes %d unfinished files.", selection.PrivateFiles))
	}
	result.Window, checkErr = p.Window.Resolve(i.clock.Now())
	if checkErr != nil {
		return nil, &Conflict{Err: checkErr}
	}
	return files, nil
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
