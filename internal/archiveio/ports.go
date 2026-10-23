package archiveio

import (
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
)

// Input owns replay and its exclusively locked checkpoint store.
type Input interface {
	core.Source
	core.ProgressStore
	DeleteCheckpoint() error
}

type Output interface {
	core.Sink
	Finish() error
}

type ArchiveInspection struct {
	Exists    bool
	FileCount int
	Size      int64
}

type SourceInspection struct {
	ArchiveInspection
	Warnings       []string
	Position       int64
	Window         plan.Window
	Saved          bool
	CheckpointPath string
	Stop           *core.Stop
}

type Conflict struct{ Err error }

func (e *Conflict) Error() string { return e.Err.Error() }
func (e *Conflict) Unwrap() error { return e.Err }
func (*Conflict) ExitCode() int   { return 2 }
