package replay

import (
	"errors"
	"io"
	"math"
	"time"

	"github.com/pagrus7/kaferry/internal/archive/avro"
	"github.com/pagrus7/kaferry/internal/plan"
)

type Clock interface{ Now() time.Time }
type Query struct {
	Group, Fingerprint string
	Window             plan.WindowSpec
}
type Item struct {
	Position, Next int64
	Record         avro.Record
}
type delta struct {
	file int
	next Position
}

var ErrPendingFull = errors.New("replay must checkpoint confirmed records before reading more")

// Session saves only confirmed prefixes. One event loop owns its mutable state.
type Session struct {
	store            Checkpoints
	blocks           Blocks
	reader           *Reader
	saved            State
	issued           int64
	pending          []delta
	limit            int
	readErr, saveErr error
	closed           bool
	PrivateFiles     int
}

// NewSession takes ownership of store and blocks only on success.
func NewSession(query Query, clock Clock, store Checkpoints, inputs Snapshot, blocks Blocks, maxPending int) (*Session, error) {
	if maxPending == 0 {
		maxPending = 20000
	}
	if maxPending < 1 {
		return nil, errors.New("replay pending-record cap must be positive")
	}
	s := &Session{store: store, blocks: blocks, limit: maxPending}
	saved, err := store.Load()
	if err != nil {
		return nil, err
	}
	if saved == nil {
		window, err := query.Window.Resolve(clock.Now())
		if err != nil {
			return nil, err
		}
		selection, err := inputs.Select()
		if err != nil {
			return nil, err
		}
		s.PrivateFiles = selection.PrivateFiles
		s.saved = State{Version: 1, Group: query.Group, Fingerprint: query.Fingerprint, Window: window, Complete: len(selection.Files) == 0, Files: make([]Progress, 0, len(selection.Files))}
		for _, file := range selection.Files {
			s.saved.Files = append(s.saved.Files, Progress{File: file, Next: initial(file)})
		}
		if err = s.saved.validate(); err != nil {
			return nil, err
		}
		if err = store.Save(s.saved.clone()); err != nil {
			return nil, err
		}
	} else {
		s.saved = saved.clone()
		if err = s.saved.validate(); err != nil {
			return nil, err
		}
		if s.saved.Group != query.Group || s.saved.Fingerprint != query.Fingerprint {
			return nil, errors.New("replay checkpoint query identity mismatch")
		}
		if err = query.Window.ValidateStored(s.saved.Window, 0); err != nil {
			return nil, err
		}
		if !s.saved.Complete {
			files := make([]File, len(s.saved.Files))
			for i, p := range s.saved.Files {
				files[i] = p.File
			}
			if err = inputs.Validate(files); err != nil {
				return nil, err
			}
		}
	}
	s.issued = s.saved.Position
	if !s.saved.Complete {
		s.reader, err = NewReader(s.saved.Files, blocks)
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *Session) Next() (item Item, err error) {
	if s.closed {
		return item, errors.New("archive replay session is closed")
	}
	if s.saveErr != nil {
		return item, s.saveErr
	}
	if s.readErr != nil {
		return item, s.readErr
	}
	if s.saved.Complete {
		return item, io.EOF
	}
	if len(s.pending) >= s.limit {
		return item, ErrPendingFull
	}
	if s.issued == math.MaxInt64 {
		return item, errors.New("replay sequence exhausted")
	}
	entry, err := s.reader.Next()
	if err != nil {
		if !errors.Is(err, io.EOF) {
			s.readErr = err
		}
		return item, err
	}
	s.pending = append(s.pending, delta{file: entry.File, next: entry.Next})
	item = Item{Position: s.issued, Next: s.issued + 1, Record: entry.Record}
	s.issued++
	return item, nil
}

func (s *Session) Checkpoint(position int64) error {
	if s.closed {
		return errors.New("archive replay session is closed")
	}
	if s.saveErr != nil {
		return s.saveErr
	}
	if position < s.saved.Position || position > s.issued {
		return errors.New("replay checkpoint is not a known confirmed prefix")
	}
	if position == s.saved.Position {
		return nil
	}
	next := s.saved.clone()
	count := int(position - s.saved.Position)
	for _, d := range s.pending[:count] {
		next.Files[d.file].Next = d.next
	}
	next.Position = position
	next.Complete = true
	for _, p := range next.Files {
		next.Complete = next.Complete && p.ended()
	}
	if err := next.validate(); err != nil {
		return err
	}
	if err := s.store.Save(next); err != nil {
		s.saveErr = err
		return err
	}
	s.saved = next
	copy(s.pending, s.pending[count:])
	s.pending = s.pending[:len(s.pending)-count]
	return nil
}

func (s *Session) Saved() State { return s.saved.clone() }
func (s *Session) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	var err error
	if s.reader != nil {
		err = s.reader.Close()
	} else {
		err = s.blocks.Close()
	}
	return errors.Join(err, s.store.Close())
}
