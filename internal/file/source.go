package file

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	copy "kafka-fwd2/internal/copy"
	"kafka-fwd2/internal/plan"
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

func NewSource(path, fp string, window plan.WindowSpec, codec ArchiveDecoder, checkpoints SourceCheckpoints, clock copy.Clock) (*Source, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if !info.Mode().IsRegular() {
		f.Close()
		return nil, conflict("file source must be a regular file")
	}
	header, start, err := codec.Header(f, info.Size())
	if err != nil {
		f.Close()
		return nil, conflict("%v", err)
	}
	resolved, err := window.Resolve(clock.Now())
	if err != nil {
		f.Close()
		return nil, conflict("%v", err)
	}
	position := start
	if saved := checkpoints.Existing(); saved != nil {
		if saved.Fingerprint != fp {
			f.Close()
			return nil, conflict("checkpoint query fingerprint mismatch")
		}
		if saved.SourceID != header.ID || saved.Position > info.Size() {
			f.Close()
			return nil, conflict("source file changed: saved=%s current=%s", saved.SourceID, header.ID)
		}
		if saved.Position < start {
			f.Close()
			return nil, conflict("checkpoint precedes source records")
		}
		if saved.LastFrame != nil {
			_, next, crc, err := readFrame(f, saved.LastFrame.Position, info.Size(), codec.FrameLimit())
			if err != nil || next != saved.Position || crc != saved.LastFrame.CRC {
				f.Close()
				return nil, conflict("source file changed: last checkpointed frame differs")
			}
		} else if saved.Position != start {
			f.Close()
			return nil, conflict("checkpoint has no source frame identity")
		}
		position = saved.Position
		resolved = saved.Window
		if err := window.ValidateStored(resolved, 0); err != nil {
			f.Close()
			return nil, conflict("%v", err)
		}
	} else {
		checkpoints.Initialize(Checkpoint{Version: 1, Fingerprint: fp, SourceID: header.ID, Position: start, Window: resolved})
	}
	return &Source{file: f, codec: codec, checkpoints: checkpoints, size: info.Size(), position: position, window: resolved}, nil
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
