package capture

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/archive/avro"
	"github.com/stretchr/testify/require"
)

type memorySegments struct {
	files  map[string]Segment
	calls  []string
	errors map[string]error
	next   int
}

func (m *memorySegments) call(op string) error {
	m.calls = append(m.calls, op)
	return m.errors[op]
}
func (m *memorySegments) Create(avro.Source, int64) (Segment, error) {
	if err := m.call("create"); err != nil {
		return Segment{}, err
	}
	m.next++
	s := Segment{ID: strconv.Itoa(m.next)}
	m.files[s.ID] = s
	return s, nil
}
func (m *memorySegments) Append(id string, _ avro.Record) (Segment, error) {
	if err := m.call("append"); err != nil {
		return Segment{}, err
	}
	s := m.files[id]
	s.Bytes, s.Records = s.Bytes+100, s.Records+1
	m.files[id] = s
	return s, nil
}
func (m *memorySegments) Checkpoint() error    { return m.call("checkpoint") }
func (m *memorySegments) Size(id string) int64 { return m.files[id].Bytes }
func (m *memorySegments) Close() error         { return m.call("close") }
func (m *memorySegments) Publish(id string) error {
	if err := m.call("publish"); err != nil {
		return err
	}
	delete(m.files, id)
	return nil
}

func TestCaptureStopsAfterBackendFailure(t *testing.T) {
	for _, op := range []string{"create", "append", "checkpoint", "publish"} {
		t.Run(op, func(t *testing.T) {
			// Given a focused backend mock that fails one component boundary.
			store := &memorySegments{files: make(map[string]Segment), errors: map[string]error{op: errDisk}}
			c, err := New(store, &testClock{}, Policy{RotateAfterSize: 1})
			require.NoError(t, err)
			// When the operation fails, then capture cannot acknowledge or submit more work.
			err = c.Append(testRecord(42))
			if op == "checkpoint" {
				err = c.Flush()
			}
			require.ErrorIs(t, err, errDisk)
			store.calls = nil
			require.ErrorIs(t, c.Append(testRecord(43)), errDisk)
			require.ErrorIs(t, c.Finish(), errDisk)
			require.Empty(t, store.calls)
		})
	}
}

func TestSkipCannotHideFailedEmptyFileCleanup(t *testing.T) {
	// Given a rejected first record whose empty private file cannot be removed.
	store := &memorySegments{files: make(map[string]Segment), errors: map[string]error{
		"append": &avro.InvalidRecord{Err: errors.New("too large")}, "publish": errDisk,
	}}
	c, err := New(store, &testClock{}, Policy{})
	require.NoError(t, err)
	// When SQL would permit skipping the input, then storage failure still stops capture.
	err = c.Append(testRecord(42))
	require.ErrorIs(t, err, errDisk)
	var skippable *avro.InvalidRecord
	require.False(t, errors.As(err, &skippable))
	require.ErrorIs(t, c.Flush(), errDisk)
}

func TestCaptureCapsOpenFilesAndPublishesLeastRecentlyUsed(t *testing.T) {
	// Given two allowed open files and two original source partitions.
	root := t.TempDir()
	c, _ := newCapture(t, root, Policy{MaxOpenFiles: 2})
	a, b, d := testRecord(42), testRecord(50), testRecord(99)
	b.Source.Partition++
	d.Source.Partition += 2
	for _, r := range []avro.Record{a, b, a} {
		require.NoError(t, c.Append(r))
	}
	// When another partition needs a file, then the least recently used partition is completed.
	require.NoError(t, c.Append(d))
	paths := completeFiles(t, root)
	require.Len(t, paths, 1)
	require.Equal(t, []avro.Record{b}, readFile(t, paths[0]))
	require.Len(t, c.active, 2)
	require.NoError(t, c.Finish())
	require.Len(t, completeFiles(t, root), 3)
}

func TestActiveInputAlsoChecksElapsedAge(t *testing.T) {
	// Given a still-active partition whose first file reaches its elapsed-age target.
	root := t.TempDir()
	c, clock := newCapture(t, root, Policy{RotateAfterAge: time.Minute})
	require.NoError(t, c.Append(testRecord(42)))
	clock.now = clock.now.Add(time.Minute)
	// When input continues without an idle flush, then the previous file is completed first.
	require.NoError(t, c.Append(testRecord(43)))
	paths := completeFiles(t, root)
	require.Len(t, paths, 1)
	require.Equal(t, []avro.Record{testRecord(42)}, readFile(t, paths[0]))
	require.NoError(t, c.Finish())
	require.Len(t, completeFiles(t, root), 2)
}

func TestInvalidRotationPolicy(t *testing.T) {
	for _, policy := range []Policy{{RotateAfterAge: -1}, {RotateAfterSize: -1}, {MaxOpenFiles: -1}} {
		// Given an invalid policy, when constructing, then no backend operation occurs.
		store := &memorySegments{}
		_, err := New(store, &testClock{}, policy)
		require.Error(t, err)
		require.Empty(t, store.calls)
	}
}
