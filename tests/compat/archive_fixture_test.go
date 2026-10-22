//go:build integration

package compat

import (
	"bufio"
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
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/file"
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
func seedArchive(t *testing.T, path, format string, count int) {
	t.Helper()
	if format == "cbor" {
		codec, err := file.NewCodec(64 << 20)
		require.NoError(t, err)
		header, err := codec.EncodeHeader(file.Header{Version: 1, ID: "fixture", Group: "fixture", Fingerprint: "fixture"})
		require.NoError(t, err)
		f, err := os.Create(path)
		require.NoError(t, err)
		defer f.Close()
		buffer := bufio.NewWriter(f)
		_, err = buffer.Write(header)
		require.NoError(t, err)
		for id := range count {
			data, err := codec.EncodeRecord(archiveLoadRecord(id))
			require.NoError(t, err)
			_, err = buffer.Write(data)
			require.NoError(t, err)
		}
		require.NoError(t, buffer.Flush())
		require.NoError(t, f.Close())
		return
	}
	codec, err := avro.New(64 << 20)
	require.NoError(t, err)
	files := capture.NativeFiles{}
	store, err := capture.Open(path, codec, files, capture.NewJournal(files, file.DurableWriter{}))
	require.NoError(t, err)
	writer, err := capture.New(store, core.RealClock{}, capture.Policy{})
	require.NoError(t, err)
	defer writer.Close()
	for id := range count {
		r := archiveLoadRecord(id)
		headers := make([]avro.Header, len(r.Headers))
		for i, h := range r.Headers {
			headers[i] = avro.Header{Key: []byte(h.Name), Value: h.Value}
		}
		require.NoError(t, writer.Append(avro.Record{Source: avro.Source{Cluster: r.Source.Cluster, Topic: r.Source.Topic, TopicID: avro.TopicID(r.Source.TopicID), Partition: r.Source.Partition}, Offset: r.Source.Offset, TimestampMS: r.TimestampMS, TimestampKind: r.TimestampType, Key: r.Key, Value: r.Value, Headers: headers}))
	}
	require.NoError(t, writer.Finish())
}

func verifyArchive(t *testing.T, path, format string, count int, original *core.Provenance) {
	t.Helper()
	seen := make([]bool, count)
	verified := 0
	check := func(record core.Record) {
		t.Helper()
		require.GreaterOrEqual(t, len(record.Value), 4)
		id := int(binary.BigEndian.Uint32(record.Value))
		require.Less(t, id, count)
		require.False(t, seen[id], "duplicate output %d", id)
		seen[id] = true
		verified++
		expected := archiveLoadRecord(id)
		require.Equal(t, expected.Key, record.Key)
		require.Equal(t, expected.Value, record.Value)
		require.Equal(t, expected.TimestampMS, record.TimestampMS)
		require.Equal(t, expected.TimestampType, record.TimestampType)
		require.Equal(t, expected.Headers, record.Headers)
		require.NotNil(t, record.Source)
		if original != nil {
			expected.Source.Cluster = original.Cluster
			expected.Source.TopicID = original.TopicID
			expected.Source.Topic = original.Topic
		}
		if format == "cbor" {
			expected.Source.Cluster = ""
			expected.Source.TopicID = [16]byte{}
		}
		require.Equal(t, expected.Source, record.Source, "original Kafka identity and offset")
	}
	if format == "cbor" {
		codec, err := file.NewCodec(64 << 20)
		require.NoError(t, err)
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		_, position, err := codec.Header(bytes.NewReader(data), int64(len(data)))
		require.NoError(t, err)
		for position < int64(len(data)) {
			require.GreaterOrEqual(t, int64(len(data))-position, int64(8))
			n := int64(binary.BigEndian.Uint32(data[position:]))
			require.LessOrEqual(t, n+8, int64(len(data))-position)
			record, err := codec.DecodeRecord(data[position+4 : position+4+n])
			require.NoError(t, err)
			check(record)
			position += n + 8
		}
	} else {
		codec, err := avro.New(64 << 20)
		require.NoError(t, err)
		catalog := replay.NewCatalog(path, codec, replay.NativeFiles{})
		selection, err := catalog.Select()
		require.NoError(t, err)
		require.Zero(t, selection.PrivateFiles)
		pool, err := replay.NewPool(catalog, replay.Limits{Blocks: 32})
		require.NoError(t, err)
		defer pool.Close()
		progress := make([]replay.Progress, len(selection.Files))
		for i, f := range selection.Files {
			progress[i] = replay.Progress{File: f, Next: replay.Position{Block: f.Start, Previous: -1}}
		}
		reader, err := replay.NewReader(progress, pool)
		require.NoError(t, err)
		for {
			entry, err := reader.Next()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			r := entry.Record
			headers := make([]core.Header, len(r.Headers))
			for i, h := range r.Headers {
				headers[i] = core.Header{Name: string(h.Key), Value: h.Value}
			}
			check(core.Record{Source: &core.Provenance{Cluster: r.Source.Cluster, TopicID: [16]byte(r.Source.TopicID), Topic: r.Source.Topic, Partition: r.Source.Partition, Offset: r.Offset}, TimestampMS: r.TimestampMS, TimestampType: r.TimestampKind, Key: r.Key, Value: r.Value, Headers: headers})
		}
	}
	require.Equal(t, count, verified, "all logical records survive archive copy")
}

func archiveQuery(format, path string) string {
	return fmt.Sprintf("FILE %s('%s')", format, filepath.ToSlash(path))
}

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
