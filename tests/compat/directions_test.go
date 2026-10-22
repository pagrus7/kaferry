//go:build integration

package compat

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/app"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func execute(t *testing.T, statement, config string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if os.Getenv("KAFERRY_TEST_BINARY") != "" {
		result := binaryWorker(t, ctx, statement, config)
		require.Zero(t, result.code, result.detail)
		return
	}
	var output bytes.Buffer
	code := app.Execute(ctx, []string{"run", "--sql", statement, "--config", config}, nil, io.Discard, &output)
	require.NoError(t, ctx.Err(), "copy must finish before its timeout: %s", output.String())
	require.Zero(t, code, output.String())
}
func configuration(t *testing.T, workerSettings ...string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	text := fmt.Sprintf("[connections.default]\n'bootstrap.servers' = %q\n[worker]\ncheckpoint_dir = %q\n", broker(t), t.TempDir()) + strings.Join(workerSettings, "\n")
	require.NoError(t, os.WriteFile(path, []byte(text), 0600))
	return path
}
func assertTopicRecords(t *testing.T, name string, partitions int, expected []core.Record) {
	t.Helper()
	reader := consumer(t)
	assign(t, reader, name, partitions)
	var actual, wanted []core.Record
	for _, record := range expected {
		record.Source = nil // Kafka output does not publish source provenance.
		wanted = append(wanted, record)
		message := read(t, reader)
		require.Equal(t, int8(0), message.Attrs.TimestampType(), "destination timestamp type")
		stamp := message.Timestamp.UnixMilli()
		got := core.Record{Key: message.Key, Value: message.Value, TimestampMS: &stamp, TimestampType: "create_time"}
		for _, header := range message.Headers {
			got.Headers = append(got.Headers, core.Header{Name: header.Key, Value: header.Value})
		}
		actual = append(actual, got)
	}
	require.ElementsMatch(t, wanted, actual, "copied records retain bytes, nulls, headers and timestamps")
}

func TestAllFileDirectionsAndResume(t *testing.T) {
	// Given
	a := admin(t)
	src := topic(t, a, 1)
	dst := topic(t, a, 3)
	p := producer(t)
	identity := brokerSourceIdentity(t, a, src)
	stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	var expected []core.Record
	for i := range 24 {
		key, value := []byte{0, byte(i)}, []byte{255, byte(i)}
		if i%3 == 0 {
			key = nil
		}
		if i%3 == 1 {
			key = []byte{}
		}
		if i%4 == 0 {
			value = nil
		}
		if i%4 == 1 {
			value = []byte{}
		}
		headers := []kgo.RecordHeader{{Key: "dup", Value: nil}, {Key: "dup", Value: []byte{}}, {Key: "binary", Value: []byte{0, 255}}}
		publish(t, p, &kgo.Record{Topic: src, Partition: 0, Key: key, Value: value, Headers: headers, Timestamp: stamp})
		ms := stamp.UnixMilli()
		expected = append(expected, core.Record{Key: key, Value: value, Headers: []core.Header{{Name: "dup", Value: nil}, {Name: "dup", Value: []byte{}}, {Name: "binary", Value: []byte{0, 255}}}, TimestampMS: &ms, TimestampType: "create_time", Source: &core.Provenance{Cluster: identity.Cluster, TopicID: identity.TopicID, Topic: src, Partition: 0, Offset: int64(i)}})
	}
	dir := t.TempDir()
	archive, clone := filepath.Join(dir, "archive.avro"), filepath.Join(dir, "clone.avro")
	cfg := configuration(t)
	statement := fmt.Sprintf("INSERT INTO FILE avro('%s') SELECT * FROM TOPIC '%s' WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' OPTIONS(GROUP='%s',CUTOFF=INTERVAL '0' SECOND)", archive, src, stamp.Add(time.Second).Format(time.RFC3339Nano), src+"-archive")

	// When
	execute(t, statement, cfg)

	// Then
	require.Equal(t, expected, archiveRecords(t, archive), "topic-to-file preserves full logical records")
	execute(t, statement, cfg)
	require.Len(t, archiveRecords(t, archive), 24, "completed archive was recopied")
	statement = fmt.Sprintf("INSERT INTO FILE avro('%s') SELECT * FROM FILE avro('%s') OPTIONS(GROUP='clone')", clone, archive)
	execute(t, statement, cfg)
	execute(t, statement, cfg)
	require.Equal(t, expected, archiveRecords(t, clone), "file-to-file preserves records without recopying saved output")
	statement = fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM FILE avro('%s') OPTIONS(GROUP='restore')", dst, archive)
	execute(t, statement, cfg)
	execute(t, statement, cfg)
	assertTopicRecords(t, dst, 3, expected)

	require.Equal(t, int64(24), topicRecordCount(t, a, dst, 3), "resumed file→topic produced extra records")
}
