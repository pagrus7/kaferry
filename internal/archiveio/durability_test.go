package archiveio

import (
	"path/filepath"
	"testing"

	"github.com/pagrus7/kaferry/internal/archive/avro"
	"github.com/pagrus7/kaferry/internal/archive/capture"
	"github.com/pagrus7/kaferry/internal/checkpoint"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/stretchr/testify/require"
)

type countingWriter struct {
	writes int
	checkpoint.DurableWriter
}

func (w *countingWriter) Replace(path string, data []byte) error {
	w.writes++
	return w.DurableWriter.Replace(path, data)
}
func TestCaptureSavesChecksummedStatePerChangedBatch(t *testing.T) {
	// Given real files with a counted atomic writer for the checksummed JSON state.
	files := capture.NativeFiles{}
	atomic := &countingWriter{}
	codec, err := avro.New(64 << 20)
	require.NoError(t, err)
	store, err := capture.Open(filepath.Join(t.TempDir(), "output"), codec, files, capture.NewJournal(files, atomic))
	require.NoError(t, err)
	writer, err := capture.New(store, core.RealClock{}, capture.Policy{})
	require.NoError(t, err)
	defer writer.Close()
	initialized := atomic.writes
	source := avro.Source{Cluster: "cluster", Topic: "orders", TopicID: avro.TopicID{1}}
	// When appending a hundred records, then no JSON state is saved until their durability flush.
	for i := range 100 {
		require.NoError(t, writer.Append(avro.Record{Source: source, Offset: int64(i), TimestampKind: "unknown", Headers: []avro.Header{}}))
	}
	require.Equal(t, initialized, atomic.writes, "record appends do not encode or hash recovery JSON")
	require.NoError(t, writer.Flush())
	require.Equal(t, initialized+1, atomic.writes, "one state checksum/write for the batch")
	// When idle flushes repeat, then unchanged recovery state is not serialized or hashed again.
	for range 10 {
		require.NoError(t, writer.Flush())
	}
	require.Equal(t, initialized+1, atomic.writes)
}
