package capture

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/archive/avro"
	"github.com/pagrus7/kaferry/internal/checkpoint"
	"github.com/stretchr/testify/require"
)

type testClock struct{ now time.Time }

func (c *testClock) Now() time.Time { return c.now }

func testCodec(t testing.TB) *avro.Codec {
	t.Helper()
	c, err := avro.New(8 << 20)
	require.NoError(t, err)
	return c
}

func openStore(t testing.TB, root string, files Filesystem, writer AtomicWriter) *Store {
	t.Helper()
	s, err := Open(root, testCodec(t), files, NewJournal(files, writer))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

func testRecord(offset int64) avro.Record {
	return avro.Record{
		Source: avro.Source{Cluster: "cluster", Topic: "orders", TopicID: avro.TopicID{1}, Partition: 2},
		Offset: offset, TimestampMS: new(offset), TimestampKind: "create_time",
		Key: []byte("key"), Value: []byte{0, 1, 2},
		Headers: []avro.Header{{Key: []byte{255}, Value: nil}, {Key: []byte{255}, Value: []byte{}}},
	}
}

func completeFiles(t testing.TB, root string) []string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(root, "*.avro"))
	require.NoError(t, err)
	return paths
}

func readFile(t testing.TB, path string) []avro.Record {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	r, err := testCodec(t).Open(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	var records []avro.Record
	for position := r.Start; position < int64(len(data)); {
		block, err := r.Block(position)
		require.NoError(t, err)
		for range block.Count {
			record, err := block.Next()
			require.NoError(t, err)
			records = append(records, record)
		}
		position = block.End
	}
	return records
}

func newCapture(t testing.TB, root string, policy Policy) (*Capture, *testClock) {
	t.Helper()
	s := openStore(t, root, NativeFiles{}, checkpoint.DurableWriter{})
	clock := &testClock{now: time.Unix(1_000_000, 0)}
	c, err := New(s, clock, policy)
	require.NoError(t, err)
	return c, clock
}

func TestCapturePreservesPartitionsAndOrder(t *testing.T) {
	// Given interleaved original partitions, duplicate offsets and raw nullable bytes.
	root := t.TempDir()
	c, _ := newCapture(t, root, Policy{})
	first := testRecord(42)
	second := testRecord(43)
	second.Key, second.Value, second.TimestampMS = nil, []byte{}, nil
	other := testRecord(99)
	other.Source.Partition++
	// When a durable batch is saved, then private files remain unpublished.
	for _, record := range []avro.Record{first, other, first, second} {
		require.NoError(t, c.Append(record))
	}
	require.NoError(t, c.Flush())
	require.Empty(t, completeFiles(t, root))
	// When the worker finishes, then each partition's exact physical order survives.
	require.NoError(t, c.Finish())
	paths := completeFiles(t, root)
	require.Len(t, paths, 2)
	for _, path := range paths {
		got := readFile(t, path)
		if got[0].Source == first.Source {
			require.Equal(t, []avro.Record{first, first, second}, got)
		} else {
			require.Equal(t, []avro.Record{other}, got)
		}
	}
	privates, err := filepath.Glob(filepath.Join(root, "*.partial"))
	require.NoError(t, err)
	require.Empty(t, privates)
}

func TestRotationOnSizeAgeAndRewind(t *testing.T) {
	for _, mode := range []string{"size", "age", "rewind", "topic identity"} {
		t.Run(mode, func(t *testing.T) {
			// Given one source record and a configured size or elapsed-age rule.
			root := t.TempDir()
			policy := Policy{}
			if mode == "size" {
				policy.RotateAfterSize = 1
			}
			c, clock := newCapture(t, root, policy)
			first, next := testRecord(42), testRecord(43)
			require.NoError(t, c.Append(first))
			// When a full block crosses the size or time rule, or the source rewinds/changes.
			switch mode {
			case "size":
				require.Len(t, completeFiles(t, root), 1)
			case "age":
				clock.now = clock.now.Add(time.Hour)
				require.NoError(t, c.Flush(), "quiet input still closes aged files")
				require.Len(t, completeFiles(t, root), 1)
			case "rewind":
				next.Offset = 41
			case "topic identity":
				next.Source.TopicID[0]++
			}
			require.NoError(t, c.Append(next))
			require.NoError(t, c.Finish())
			// Then records belong to separate immutable files, without empty files.
			paths := completeFiles(t, root)
			require.Len(t, paths, 2)
			var offsets []int64
			for _, path := range paths {
				got := readFile(t, path)
				require.Len(t, got, 1)
				offsets = append(offsets, got[0].Offset)
			}
			require.ElementsMatch(t, []int64{first.Offset, next.Offset}, offsets)
		})
	}
}

func TestSizeRotationAfterCheckpointFlush(t *testing.T) {
	// Given a target above the header but below the first compressed block end.
	root := t.TempDir()
	c, _ := newCapture(t, root, Policy{RotateAfterSize: 2 << 10})
	record := testRecord(42)
	record.Value = make([]byte, 32<<10)
	for i := range record.Value {
		record.Value[i] = byte(i*17 + i/256)
	}
	require.NoError(t, c.Append(record))
	require.Empty(t, completeFiles(t, root))
	// When the batch flushes, then the actual compressed size triggers publication.
	require.NoError(t, c.Flush())
	paths := completeFiles(t, root)
	require.Len(t, paths, 1)
	require.Equal(t, []avro.Record{record}, readFile(t, paths[0]))
}

func TestSkippedFirstRecordDoesNotPinFirstOffset(t *testing.T) {
	// Given an oversized first record rejected before any record is written.
	root := t.TempDir()
	c, _ := newCapture(t, root, Policy{})
	bad := testRecord(42)
	bad.Value = make([]byte, 8<<20)
	var invalid *avro.InvalidRecord
	require.ErrorAs(t, c.Append(bad), &invalid)
	// When SQL permits skipping it, then the next record starts a fresh nonempty file.
	want := testRecord(43)
	require.NoError(t, c.Append(want))
	require.NoError(t, c.Finish())
	paths := completeFiles(t, root)
	require.Len(t, paths, 1)
	require.Equal(t, []avro.Record{want}, readFile(t, paths[0]))
}

func TestEmptyCaptureCreatesNoCompletedFile(t *testing.T) {
	// Given no records, when the worker finishes, then no data file is published.
	root := t.TempDir()
	c, _ := newCapture(t, root, Policy{})
	require.NoError(t, c.Finish())
	require.Empty(t, completeFiles(t, root))
}
