package archiveio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/pagrus7/kaferry/internal/archive/replay"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/file"
)

type Replay interface {
	Next() (replay.Item, error)
	CheckpointStop(int64, *core.Stop) error
	Saved() replay.State
	Close() error
}

type Source struct {
	session       Replay
	clock         core.Clock
	path          string
	paused, ended bool
	position      int64
}

func NewSource(session Replay, clock core.Clock, path string) *Source {
	return &Source{session: session, clock: clock, path: path, position: session.Saved().Position}
}
func (s *Source) Start(life core.Lifecycle, _ core.Observer) error {
	saved := s.session.Saved()
	return life.Assign([]core.Assignment{{Cursor: core.Cursor{Partition: 0, Position: saved.Position}, Window: saved.Window}})
}
func (s *Source) Poll(timeout time.Duration) (core.Event, error) {
	if s.paused || s.ended {
		s.clock.Sleep(timeout)
		return core.Event{}, nil
	}
	item, err := s.session.Next()
	if errors.Is(err, replay.ErrPendingFull) {
		s.clock.Sleep(timeout)
		return core.Event{}, nil
	}
	if errors.Is(err, io.EOF) {
		s.ended = true
		return core.Event{EOF: &core.Cursor{Partition: 0, Position: s.position}}, nil
	}
	if err != nil {
		return core.Event{}, err
	}
	s.position = item.Next
	return core.Event{Input: &core.Input{Cursor: core.Cursor{Partition: 0, Position: item.Position}, Next: item.Next, Record: fromArchive(item.Record)}}, nil
}
func (s *Source) Save(ctx context.Context, cursors []core.Cursor) (map[int32]error, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(cursors) != 1 || cursors[0].Partition != 0 {
		return nil, fmt.Errorf("archive replay requires one virtual source cursor")
	}
	if err := s.session.CheckpointStop(cursors[0].Position, cursors[0].Stop); err != nil {
		return nil, err
	}
	return map[int32]error{0: nil}, nil
}
func (s *Source) Pause(_ int32, pause bool) error { s.paused = pause; return nil }
func (s *Source) HighWatermark(int32) (int64, error) {
	return 0, errors.New("archive replay has no Kafka high watermark")
}
func (s *Source) BindOutput(core.OutputProgress)     {}
func (s *Source) Progress() file.DestinationProgress { return file.DestinationProgress{Exists: true} }
func (s *Source) Close() error                       { return s.session.Close() }
func (s *Source) DeleteCheckpoint() error {
	if err := os.Remove(s.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	dir, err := os.Open(filepath.Dir(s.path))
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}
