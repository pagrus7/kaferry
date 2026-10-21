package capture

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"

	"github.com/pagrus7/kaferry/internal/archive/avro"
)

type Segment struct {
	ID             string
	Bytes, Records int64
}

// Segments confirms records only after Checkpoint or Publish succeeds.
type Segments interface {
	Create(avro.Source, int64) (Segment, error)
	Append(string, avro.Record) (Segment, error)
	Checkpoint() error
	Size(string) int64
	Publish(string) error
	Close() error
}

type activeFile struct {
	file    DataFile
	writer  *avro.Writer
	changed bool
}

// Store owns one locked namespace. Its methods run on the caller's event loop.
type Store struct {
	root, namespace string
	files           Filesystem
	states          StateStore
	codec           *avro.Codec
	lock            io.Closer
	state           State
	active          map[string]*activeFile
	failed          error
}

func (s *Store) Create(source avro.Source, first int64) (Segment, error) {
	if s.failed != nil {
		return Segment{}, s.failed
	}
	identity := avro.Identity{Version: avro.Version, WriterID: s.state.WriterID, FileID: newID(), Source: source, FirstOffset: first}
	f, err := s.files.OpenFile(dataPath(s.root, identity, true), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return Segment{}, s.fail(err)
	}
	w, err := s.codec.NewWriter(f, identity)
	if err != nil {
		return Segment{}, s.fail(errors.Join(err, f.Close()))
	}
	s.active[identity.FileID] = &activeFile{file: f, writer: w}
	return segment(w), nil
}

func (s *Store) Append(id string, record avro.Record) (Segment, error) {
	if s.failed != nil {
		return Segment{}, s.failed
	}
	f, ok := s.active[id]
	if !ok {
		return Segment{}, s.fail(errors.New("archive segment is not open"))
	}
	if err := f.writer.Append(record); err != nil {
		var invalid *avro.InvalidRecord
		if errors.As(err, &invalid) || errors.Is(err, avro.ErrRotationRequired) {
			return segment(f.writer), err
		}
		return segment(f.writer), s.fail(err)
	}
	f.changed = true
	return segment(f.writer), nil
}

func (s *Store) Checkpoint() error {
	if s.failed != nil {
		return s.failed
	}
	next := s.state
	next.Files = maps.Clone(s.state.Files)
	changed := false
	for id, f := range s.active {
		if !f.changed {
			continue
		}
		if err := f.writer.Flush(); err != nil {
			return s.fail(err)
		}
		if err := f.file.Sync(); err != nil {
			return s.fail(err)
		}
		next.Files[id] = Boundary{Identity: f.writer.Identity(), Extent: f.writer.Size(), Records: f.writer.Count()}
		changed = true
	}
	if !changed {
		return nil
	}
	if err := s.files.SyncDirectory(s.root); err != nil {
		return s.fail(err)
	}
	if err := s.states.Save(s.statePath(), next); err != nil {
		return s.fail(err)
	}
	s.state = next
	for _, f := range s.active {
		f.changed = false
	}
	return nil
}

// Publish completes a nonempty file; an empty private file is discarded.
func (s *Store) Publish(id string) error {
	if s.failed != nil {
		return s.failed
	}
	f, ok := s.active[id]
	if !ok {
		return s.fail(errors.New("archive segment is not open"))
	}
	if f.writer.Count() > 0 {
		if err := s.Checkpoint(); err != nil {
			return err
		}
		info, err := f.file.Stat()
		if err != nil {
			return s.fail(err)
		}
		if info.Size() != s.state.Files[id].Extent {
			return s.fail(errors.New("archive length changed before publication"))
		}
	}
	handle := f.file
	f.file = nil
	if err := handle.Close(); err != nil {
		return s.fail(err)
	}
	identity := f.writer.Identity()
	private := dataPath(s.root, identity, true)
	if f.writer.Count() == 0 {
		if err := s.files.Remove(private); err != nil {
			return s.fail(err)
		}
	} else if err := s.files.RenameExclusive(private, dataPath(s.root, identity, false)); err != nil {
		return s.fail(err)
	}
	if err := s.files.SyncDirectory(s.root); err != nil {
		return s.fail(err)
	}
	if f.writer.Count() > 0 {
		next := s.state
		next.Files = maps.Clone(s.state.Files)
		delete(next.Files, id)
		if err := s.states.Save(s.statePath(), next); err != nil {
			return s.fail(err)
		}
		s.state = next
	}
	delete(s.active, id)
	return nil
}

// Close releases handles without publishing or acknowledging pending writes.
func (s *Store) Close() error {
	var result error
	for id, f := range s.active {
		if f.file != nil {
			result = errors.Join(result, f.file.Close())
		}
		delete(s.active, id)
	}
	if s.lock != nil {
		result = errors.Join(result, s.lock.Close())
		s.lock = nil
	}
	if s.failed == nil {
		s.failed = errors.New("archive store is closed")
	}
	return result
}

func (s *Store) fail(err error) error {
	if s.failed == nil {
		s.failed = fmt.Errorf("archive capture stopped: %w", err)
	}
	return s.failed
}

func (s *Store) statePath() string    { return filepath.Join(s.namespace, "state.json") }
func (s *Store) Size(id string) int64 { return s.active[id].writer.Size() }
func segment(w *avro.Writer) Segment {
	return Segment{ID: w.Identity().FileID, Bytes: w.Size(), Records: w.Count()}
}
func newID() string {
	var id [16]byte
	_, _ = rand.Read(id[:])
	return hex.EncodeToString(id[:])
}
func dataPath(root string, identity avro.Identity, private bool) string {
	name := identity.WriterID + "." + identity.FileID + ".avro"
	if private {
		name = "." + identity.WriterID + "." + identity.FileID + ".partial"
	}
	return filepath.Join(root, name)
}
