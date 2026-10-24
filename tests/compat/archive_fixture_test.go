//go:build integration

package compat

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/archive/avro"
	"github.com/pagrus7/kaferry/internal/archive/capture"
	"github.com/pagrus7/kaferry/internal/archive/replay"
	"github.com/pagrus7/kaferry/internal/checkpoint"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
)

var archiveFixtureTimestamp = time.Now().Add(-time.Hour).UnixMilli()

func archiveLoadRecord(id int) core.Record {
	value := bytes.Repeat([]byte{byte(id % 251)}, 1024+id%4096)
	if os.Getenv("KAFERRY_ARCHIVE_PAYLOAD") == "random" {
		random := rand.New(rand.NewPCG(uint64(id), 42))
		for i := range value {
			value[i] = byte(random.Uint32())
		}
	}
	binary.BigEndian.PutUint32(value, uint32(id))
	stamp := archiveFixtureTimestamp
	key := []byte(fmt.Sprintf("key-%d", id%257))
	if id%7 == 0 {
		key = nil
	}
	return core.Record{Key: key, Value: value, TimestampMS: &stamp, TimestampType: "create_time", Source: &core.Provenance{Cluster: "fixture", TopicID: [16]byte{1}, Topic: "orders", Partition: int32(id % 4), Offset: int64(id / 4)}, Headers: []core.Header{{Name: "trace", Value: []byte{0, 255}}, {Name: "dup", Value: nil}, {Name: "dup", Value: []byte{}}}}
}

func archiveFixtureWriter(t *testing.T, path string) *capture.Capture {
	t.Helper()
	codec, err := avro.New(64 << 20)
	require.NoError(t, err)
	files := capture.NativeFiles{}
	store, err := capture.Open(path, codec, files, capture.NewJournal(files, checkpoint.DurableWriter{}))
	require.NoError(t, err)
	writer, err := capture.New(store, core.RealClock{}, capture.Policy{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, writer.Close()) })
	return writer
}

func fixtureRecord(record core.Record, offset int64) avro.Record {
	source := record.Source
	if source == nil {
		source = &core.Provenance{Cluster: "fixture", Topic: "orders", TopicID: [16]byte{1}, Offset: offset}
	}
	out := avro.Record{Source: avro.Source{Cluster: source.Cluster, Topic: source.Topic, TopicID: avro.TopicID(source.TopicID), Partition: source.Partition}, Offset: source.Offset, TimestampMS: record.TimestampMS, TimestampKind: record.TimestampType, Key: record.Key, Value: record.Value, Headers: make([]avro.Header, len(record.Headers))}
	for i, h := range record.Headers {
		out.Headers[i] = avro.Header{Key: []byte(h.Name), Value: h.Value}
	}
	return out
}

func writeArchiveRecords(t *testing.T, path string, records []core.Record) {
	t.Helper()
	writer := archiveFixtureWriter(t, path)
	for i, record := range records {
		require.NoError(t, writer.Append(fixtureRecord(record, int64(i))))
	}
	require.NoError(t, writer.Finish())
}

func seedArchive(t *testing.T, path string, count int) {
	t.Helper()
	writer := archiveFixtureWriter(t, path)
	for id := range count {
		require.NoError(t, writer.Append(fixtureRecord(archiveLoadRecord(id), int64(id))))
	}
	require.NoError(t, writer.Finish())
}

func scanArchive(t *testing.T, path string, visit func(core.Record)) {
	t.Helper()
	codec, err := avro.New(64 << 20)
	require.NoError(t, err)
	catalog := replay.NewCatalog(path, codec, replay.NativeFiles{})
	selection, err := catalog.Select(nil)
	require.NoError(t, err)
	require.Zero(t, selection.PrivateFiles, "finished copies leave no unfinished archive files")
	pool, err := replay.NewPool(catalog, replay.Limits{Blocks: 32})
	require.NoError(t, err)
	defer pool.Close()
	progress := make([]replay.Progress, len(selection.Files))
	for i, file := range selection.Files {
		progress[i] = replay.Progress{File: file, Next: replay.Position{Block: file.Start, Previous: -1}}
	}
	reader, err := replay.NewReader(progress, pool)
	require.NoError(t, err)
	for {
		entry, err := reader.Next()
		if err == io.EOF {
			return
		}
		require.NoError(t, err)
		record := entry.Record
		headers := make([]core.Header, len(record.Headers))
		for i, h := range record.Headers {
			headers[i] = core.Header{Name: string(h.Key), Value: h.Value}
		}
		visit(core.Record{Source: &core.Provenance{Cluster: record.Source.Cluster, Topic: record.Source.Topic, TopicID: [16]byte(record.Source.TopicID), Partition: record.Source.Partition, Offset: record.Offset}, TimestampMS: record.TimestampMS, TimestampType: record.TimestampKind, Key: record.Key, Value: record.Value, Headers: headers})
	}
}

func archiveRecords(t *testing.T, path string) []core.Record {
	t.Helper()
	var records []core.Record
	scanArchive(t, path, func(record core.Record) { records = append(records, record) })
	return records
}

func verifyArchive(t *testing.T, path string, count int, original *core.Provenance) {
	t.Helper()
	seen := make([]bool, count)
	verified := 0
	scanArchive(t, path, func(record core.Record) {
		t.Helper()
		require.GreaterOrEqual(t, len(record.Value), 4)
		id := int(binary.BigEndian.Uint32(record.Value))
		require.Less(t, id, count)
		require.False(t, seen[id], "duplicate output %d", id)
		seen[id] = true
		verified++
		expected := archiveLoadRecord(id)
		if original != nil {
			expected.Source.Cluster, expected.Source.TopicID, expected.Source.Topic = original.Cluster, original.TopicID, original.Topic
		}
		require.Equal(t, expected, record, "logical record %d retains bytes, timestamp, headers and source identity", id)
	})
	require.Equal(t, count, verified, "all logical records survive archive copy")
}

func archiveQuery(path string) string { return fmt.Sprintf("FILE avro('%s')", filepath.ToSlash(path)) }

func brokerSourceIdentity(t *testing.T, admin *kadm.Client, topic string) *core.Provenance {
	t.Helper()
	metadata, err := admin.Metadata(t.Context(), topic)
	require.NoError(t, err)
	require.NoError(t, metadata.Topics[topic].Err)
	require.NotEmpty(t, metadata.Cluster)
	id := [16]byte(metadata.Topics[topic].ID)
	require.NotEqual(t, [16]byte{}, id)
	return &core.Provenance{Cluster: metadata.Cluster, Topic: topic, TopicID: id}
}
