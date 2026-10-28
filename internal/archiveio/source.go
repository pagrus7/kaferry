package archiveio

import (
	"errors"
	"io"
	"time"

	"github.com/pagrus7/kaferry/internal/archive/replay"
	core "github.com/pagrus7/kaferry/internal/copy"
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
func (s *Source) Pause(_ int32, pause bool) error { s.paused = pause; return nil }
func (s *Source) HighWatermark(int32) (int64, error) {
	return 0, errors.New("archive replay has no Kafka high watermark")
}
func (s *Source) Close() error { return s.session.Close() }
