package replay

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/archive/avro"
	"github.com/pagrus7/kaferry/internal/checkpoint"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
)

var errDisk = errors.New("injected storage failure")

type clock struct{ now time.Time }

func (c clock) Now() time.Time { return c.now }
func record(offset int64) avro.Record {
	return avro.Record{Source: avro.Source{Cluster: "cluster-a", Topic: "orders", TopicID: avro.TopicID{1}}, Offset: offset, TimestampKind: "create_time", TimestampMS: new(offset * 10), Key: []byte{}, Value: []byte(fmt.Sprintf("payload-%d", offset)), Headers: []avro.Header{{Key: []byte{0xff, 0}, Value: nil}, {Key: []byte{0xff, 0}, Value: []byte{}}}}
}
func records(offsets ...int64) []avro.Record {
	var result []avro.Record
	for _, offset := range offsets {
		result = append(result, record(offset))
	}
	return result
}
func descriptor(id int, values []avro.Record) File {
	return File{Identity: avro.Identity{Version: 1, WriterID: strings.Repeat("1", 32), FileID: fmt.Sprintf("%032x", id), Source: values[0].Source, FirstOffset: values[0].Offset, BlockLimit: 8 << 20}, Start: 100, Size: 200, SHA256: strings.Repeat("0", 64)}
}

type memoryBlocks struct {
	values        map[string][]avro.Record
	reads, closes int
	fail          func(File, Position) error
}

func (m *memoryBlocks) Read(file File, pos Position) (avro.Record, Position, error) {
	m.reads++
	if m.fail != nil {
		if err := m.fail(file, pos); err != nil {
			return avro.Record{}, Position{}, err
		}
	}
	values := m.values[file.Identity.FileID]
	if pos.Block == file.Size {
		return avro.Record{}, Position{}, io.EOF
	}
	if pos.Record < 0 || pos.Record >= int64(len(values)) {
		return avro.Record{}, Position{}, errors.New("invalid test cursor")
	}
	r := values[pos.Record]
	next := Position{Block: file.Start, Record: pos.Record + 1, Previous: r.Offset}
	if next.Record == int64(len(values)) {
		next.Block, next.Record = file.Size, 0
	}
	return r, next, nil
}
func (m *memoryBlocks) Close() error { m.closes++; return nil }

type memorySnapshots struct {
	files                  []File
	validated              []File
	private                int
	selects, validates     int
	selectErr, validateErr error
}

func (m *memorySnapshots) Select(*plan.PartitionFilter) (Selection, error) {
	m.selects++
	return Selection{Files: append([]File{}, m.files...), PrivateFiles: m.private}, m.selectErr
}
func (m *memorySnapshots) Validate(files []File) error {
	m.validates++
	m.validated = append([]File{}, files...)
	return m.validateErr
}

type memoryCheckpoints struct {
	saved            *State
	writes           []State
	loadErr, saveErr error
	closes           int
}

func (m *memoryCheckpoints) Load() (*State, error) {
	if m.saved == nil {
		return nil, m.loadErr
	}
	s := m.saved.clone()
	return &s, m.loadErr
}
func (m *memoryCheckpoints) Save(s State) error {
	m.writes = append(m.writes, s.clone())
	if m.saveErr != nil {
		return m.saveErr
	}
	fresh := s.clone()
	m.saved = &fresh
	return nil
}
func (m *memoryCheckpoints) Close() error { m.closes++; return nil }
func query() Query                        { return Query{Group: "replay-test", Fingerprint: "query-fingerprint"} }
func session(t testing.TB, store Checkpoints, inputs Snapshot, blocks Blocks, limit int) *Session {
	t.Helper()
	s, err := NewSession(query(), clock{}, store, inputs, blocks, limit)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}
func drain(t testing.TB, s *Session, confirm bool) []avro.Record {
	t.Helper()
	var result []avro.Record
	for {
		item, err := s.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		result = append(result, item.Record)
		if confirm {
			require.NoError(t, s.Checkpoint(item.Next))
		}
	}
	return result
}
func codec(t testing.TB) *avro.Codec {
	t.Helper()
	c, err := avro.New(8 << 20)
	require.NoError(t, err)
	return c
}
func writeFile(t testing.TB, root string, id int, values []avro.Record, blockRecords int) File {
	t.Helper()
	meta := descriptor(id, values)
	f, err := os.Create(filepath.Join(root, meta.name()))
	require.NoError(t, err)
	w, err := codec(t).NewWriter(f, meta.Identity)
	require.NoError(t, err)
	for i, r := range values {
		require.NoError(t, w.Append(r))
		if blockRecords > 0 && (i+1)%blockRecords == 0 {
			require.NoError(t, w.Flush())
		}
	}
	require.NoError(t, w.Flush())
	require.NoError(t, f.Close())
	result, err := NewCatalog(root, codec(t), NativeFiles{}).describe(meta.name(), true, nil)
	require.NoError(t, err)
	return result
}
func diskSession(t testing.TB, root, checkpoints string, q Query, limits Limits, pending int) (*Session, *Pool) {
	t.Helper()
	catalog := NewCatalog(root, codec(t), NativeFiles{})
	pool, err := NewPool(catalog, limits)
	require.NoError(t, err)
	journal, err := NewJournal(checkpoints, q.Group, NativeGroupFiles{}, checkpoint.DurableWriter{})
	require.NoError(t, err)
	s, err := NewSession(q, clock{}, journal, catalog, pool, pending)
	if err != nil {
		require.NoError(t, pool.Close())
		require.NoError(t, journal.Close())
	}
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s, pool
}

func partialState() State {
	f := descriptor(1, records(42, 43))
	s := emptyState()
	s.Complete, s.Position = false, 1
	s.Files = []Progress{{File: f, Next: Position{Block: f.Start, Record: 1, Previous: 42}}}
	return s
}
