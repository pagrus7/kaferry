package file

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	copy "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
)

type Source struct {
	file        sourceFile
	codec       ArchiveDecoder
	checkpoints SourceCheckpoints
	size        int64
	position    int64
	window      plan.Window
	paused      bool
	ended       bool
}

func NewSource(path, fp string, window plan.WindowSpec, codec ArchiveDecoder, checkpoints SourceCheckpoints, clock copy.Clock) (source *Source, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if source == nil {
			_ = f.Close()
		}
	}()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, conflict("file source must be a regular file")
	}
	header, start, err := codec.Header(f, info.Size())
	if err != nil {
		return nil, conflict("%v", err)
	}
	var resolved plan.Window
	position := start
	if saved := checkpoints.Existing(); saved != nil {
		position, resolved, err = resumePoint(f, info.Size(), start, header.ID, saved, fp, window, codec.FrameLimit())
		if err != nil {
			return nil, err
		}
		fresh := *saved
		fresh.OpeningSize, fresh.Stop = info.Size(), nil
		checkpoints.Initialize(fresh)
	} else {
		resolved, err = window.Resolve(clock.Now())
		if err != nil {
			return nil, conflict("%v", err)
		}
		checkpoints.Initialize(Checkpoint{Version: 2, OpeningSize: info.Size(), Fingerprint: fp, SourceID: header.ID, Position: start, Window: resolved})
	}
	return &Source{file: f, codec: codec, checkpoints: checkpoints, size: info.Size(), position: position, window: resolved}, nil
}

// resumePoint validates persisted file identity without owning the file handle.
func resumePoint(reader io.ReaderAt, size, start int64, sourceID string, saved *Checkpoint, fp string, window plan.WindowSpec, frameLimit int64) (int64, plan.Window, error) {
	if saved.Fingerprint != fp {
		return 0, plan.Window{}, conflict("checkpoint query fingerprint mismatch")
	}
	if saved.SourceID != sourceID || saved.Position > size {
		return 0, plan.Window{}, conflict("source file changed: saved=%s current=%s", saved.SourceID, sourceID)
	}
	if saved.Position < start {
		return 0, plan.Window{}, conflict("checkpoint precedes source records")
	}
	if saved.LastFrame != nil {
		_, next, crc, err := readFrame(reader, saved.LastFrame.Position, size, frameLimit)
		if err != nil || next != saved.Position || crc != saved.LastFrame.CRC {
			return 0, plan.Window{}, conflict("source file changed: last checkpointed frame differs")
		}
	} else if saved.Position != start {
		return 0, plan.Window{}, conflict("checkpoint has no source frame identity")
	}
	position := saved.Position
	resolved := saved.Window
	if err := window.ValidateStored(resolved, 0); err != nil {
		return 0, plan.Window{}, conflict("%v", err)
	}
	return position, resolved, nil
}
func (s *Source) Start(lifecycle copy.Lifecycle) error {
	cursor := copy.Cursor{Partition: 0, Position: s.position}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := s.checkpoints.Save(ctx, []copy.Cursor{cursor}); err != nil {
		return err
	}
	return lifecycle.Assign([]copy.Assignment{{Cursor: cursor, Window: s.window}})
}
func (s *Source) Poll(timeout time.Duration) (copy.Event, error) {
	if s.paused || s.ended {
		time.Sleep(timeout)
		return copy.Event{}, nil
	}
	position := s.position
	data, next, crc, err := readFrame(s.file, position, s.size, s.codec.FrameLimit())
	if errors.Is(err, io.EOF) {
		s.ended = true
		cursor := copy.Cursor{Partition: 0, Position: position}
		return copy.Event{EOF: &cursor}, nil
	}
	if err != nil {
		return copy.Event{}, err
	}
	record, err := s.codec.DecodeRecord(data)
	if err != nil {
		return copy.Event{}, fmt.Errorf("frame at %d: %w", position, err)
	}
	s.position = next
	s.checkpoints.RememberFrame(next, FrameIdentity{Position: position, CRC: crc})
	return copy.Event{Input: &copy.Input{Cursor: copy.Cursor{Partition: 0, Position: position}, Next: next, Record: record}}, nil
}
func (s *Source) Pause(_ int32, pause bool) error    { s.paused = pause; return nil }
func (s *Source) HighWatermark(int32) (int64, error) { return s.size, nil }
func (s *Source) Close() error                       { return s.file.Close() }
