package file

import (
	"os"

	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
)

type ArchiveInspection struct {
	Exists       bool
	ID           string
	Size         int64
	RecordsStart int64
}

type SourceInspection struct {
	ArchiveInspection
	Position       int64
	Window         plan.Window
	Saved          bool
	CheckpointPath string
	Output         DestinationProgress
}

// Inspector reads archive headers and saved progress without opening locks.
type Inspector struct{ clock core.Clock }

func NewInspector(clock core.Clock) *Inspector { return &Inspector{clock: clock} }

type archiveReader struct {
	file   *os.File
	header Header
	info   os.FileInfo
	start  int64
}

func inspectArchive(path string, limit int64) (reader *archiveReader, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			f.Close()
		}
	}()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, conflict("archive must be a regular file")
	}
	codec, err := NewCodec(limit)
	if err != nil {
		return nil, err
	}
	header, start, err := codec.Header(f, info.Size())
	if err != nil {
		return nil, conflict("%v", err)
	}
	return &archiveReader{file: f, header: header, info: info, start: start}, nil
}

func (r *archiveReader) inspection() ArchiveInspection {
	return ArchiveInspection{Exists: true, ID: r.header.ID, Size: r.info.Size(), RecordsStart: r.start}
}

func (i *Inspector) Source(p plan.Plan, fingerprint string, limits config.Worker) (SourceInspection, error) {
	var result SourceInspection
	directory, err := checkpointDirectory(limits)
	if err != nil {
		return result, err
	}
	if err := validateCheckpointDirectory(directory); err != nil {
		return result, err
	}
	reader, err := inspectArchive(p.Source.Name, limits.MaxFrameBytes)
	if err != nil {
		return result, err
	}
	defer reader.file.Close()
	if p.Destination.Kind == "FILE" {
		other, err := os.Stat(p.Destination.Name)
		if err == nil && os.SameFile(reader.info, other) {
			return result, conflict("source and destination are the same file")
		}
	}
	result.ArchiveInspection, result.Position = reader.inspection(), reader.start
	var saved *Checkpoint
	if !p.OneOff {
		result.CheckpointPath = checkpointPath(directory, p.Group)
		saved, err = readCheckpoint(result.CheckpointPath)
		if err != nil {
			return result, err
		}
	}
	if saved == nil {
		result.Window, err = p.Window.Resolve(i.clock.Now())
		if err != nil {
			return result, conflict("%v", err)
		}
	} else {
		result.Position, result.Window, err = resumePoint(reader.file, reader.info.Size(), reader.start, reader.header.ID, saved, fingerprint, p.Window, limits.MaxFrameBytes)
		result.Saved = true
		result.Output = DestinationProgress{ID: saved.DestinationID, Extent: saved.Extent, Exists: true}
	}
	return result, err
}

func (i *Inspector) Destination(p plan.Plan, fingerprint string, progress DestinationProgress, limits config.Worker) (ArchiveInspection, error) {
	if progress.Exists && progress.ID == "" {
		return ArchiveInspection{}, conflict("existing progress has no destination file identity; use a new GROUP and output")
	}
	reader, err := inspectArchive(p.Destination.Name, limits.MaxFrameBytes)
	if os.IsNotExist(err) {
		if progress.ID != "" {
			return ArchiveInspection{}, conflict("output is missing while progress names it")
		}
		return ArchiveInspection{}, nil
	}
	if err != nil {
		return ArchiveInspection{}, err
	}
	defer reader.file.Close()
	_, err = outputIdentity(reader.header, reader.start, reader.info.Size(), false, p.Group, fingerprint, progress.ID, progress.Extent)
	return reader.inspection(), err
}
