package replay

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/linkedin/goavro/v2"
	"github.com/pagrus7/kaferry/internal/archive/avro"
	"github.com/stretchr/testify/require"
)

func TestReplayRejectsOffsetsDecreasingBetweenBlocks(t *testing.T) {
	// Given a standard Avro writer that emits decreasing offsets in separate blocks.
	root := t.TempDir()
	meta := descriptor(1, records(42))
	f, err := os.Create(filepath.Join(root, meta.name()))
	require.NoError(t, err)
	metadata, err := json.Marshal(meta.Identity)
	require.NoError(t, err)
	w, err := goavro.NewOCFWriter(goavro.OCFConfig{W: f, Schema: avro.Schema(), CompressionName: "snappy", MetaData: map[string][]byte{"kaferry": metadata}})
	require.NoError(t, err)
	source := meta.Identity.Source
	for _, offset := range []int64{42, 44, 43} {
		native := map[string]any{"source": map[string]any{"cluster": source.Cluster, "topic": source.Topic, "topic_id": source.TopicID[:], "partition": source.Partition}, "offset": offset, "timestamp_ms": nil, "timestamp_kind": "unknown", "key": nil, "value": nil, "headers": []any{}}
		require.NoError(t, w.Append([]any{native}))
	}
	require.NoError(t, f.Close())
	catalog := NewCatalog(root, codec(t), NativeFiles{})
	selected, err := catalog.Select()
	require.NoError(t, err)
	pool, err := NewPool(catalog, Limits{})
	require.NoError(t, err)
	defer pool.Close()
	// When reading across blocks, then each offset must follow the previous block's last offset.
	selectedFile := selected.Files[0]
	pos := initial(selectedFile)
	for _, offset := range []int64{42, 44} {
		r, next, err := pool.Read(selectedFile, pos)
		require.NoError(t, err)
		require.Equal(t, offset, r.Offset)
		pos = next
	}
	_, _, err = pool.Read(selectedFile, pos)
	require.ErrorContains(t, err, "decreased")

}

func TestDifferentSourceIdentitiesDoNotMergeTheirOffsets(t *testing.T) {
	// Given reused offsets in different clusters, topics, topic IDs and partitions.
	root := t.TempDir()
	var expected []avro.Record
	for i := range 5 {
		values := records(42, 43)
		for j := range values {
			if i == 1 {
				values[j].Source.Partition = 1
			}
			if i == 2 {
				values[j].Source.TopicID[0] = 2
			}
			if i == 3 {
				values[j].Source.Topic = "payments"
			}
			if i == 4 {
				values[j].Source.Cluster = "cluster-b"
			}
		}
		writeFile(t, root, i+1, values, 0)
		expected = append(expected, values...)
	}
	s, _ := diskSession(t, root, t.TempDir(), query(), Limits{OpenFiles: 1, Blocks: 1}, 10)
	// When replayed, then each original identity retains its records and own offset order.
	require.Equal(t, expected, drain(t, s, true))
}

func TestBadCursorAndChangedHandleFailBeforeReturningRecords(t *testing.T) {
	// Given a valid selection and invalid physical positions or a later changed file header.
	root := t.TempDir()
	f := writeFile(t, root, 1, records(42, 43), 0)
	catalog := NewCatalog(root, codec(t), NativeFiles{})
	for _, pos := range []Position{{Block: f.Start - 1, Previous: -1}, {Block: f.Start, Record: -1, Previous: -1}, {Block: f.Size, Record: 1, Previous: 42}, {Block: f.Start, Record: 1, Previous: -1}} {
		_, err := NewReader([]Progress{{File: f, Next: pos}}, &memoryBlocks{})
		require.Error(t, err)
	}
	wrong := f
	wrong.Size++
	_, err := catalog.Open(wrong)
	require.ErrorContains(t, err, "changed")
	pool, err := NewPool(catalog, Limits{})
	require.NoError(t, err)
	defer pool.Close()
	_, _, err = pool.Read(f, Position{Block: f.Start, Record: 10, Previous: 42})
	require.ErrorContains(t, err, "count")
}

func TestPoolReleasesAllHandlesEvenWhenCloseFails(t *testing.T) {
	// Given multiple open input files and close failures.
	root := t.TempDir()
	a := writeFile(t, root, 1, records(42), 0)
	b := writeFile(t, root, 2, records(43), 0)
	meter := &measuredOpener{Opener: NewCatalog(root, codec(t), NativeFiles{})}
	pool, err := NewPool(meter, Limits{})
	require.NoError(t, err)
	for _, f := range []File{a, b} {
		_, _, err = pool.Read(f, initial(f))
		require.NoError(t, err)
	}
	meter.closeErr = errDisk
	// When closing, then every handle is released exactly once and the error survives.
	require.ErrorIs(t, pool.Close(), errDisk)
	require.Zero(t, meter.active)
	require.Equal(t, 2, meter.closes)
	require.NoError(t, pool.Close())
	_, _, err = pool.Read(a, initial(a))
	require.ErrorContains(t, err, "closed")
}
