//go:build integration

package compat

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestAvroCaptureAndReplayCLI(t *testing.T) {
	// Given a Kafka source and a fresh archive directory with full record fidelity.
	a := admin(t)
	source, destination := topic(t, a, 4), topic(t, a, 5)
	p := producer(t)
	run := func(statement, settings string) workerResult {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		cfg := ""
		if settings != "" {
			cfg = filepath.Join(t.TempDir(), "config.toml")
			require.NoError(t, os.WriteFile(cfg, []byte(settings), 0600))
		}
		return binaryWorker(t, ctx, statement, cfg)
	}
	for id := range 32 {
		r := archiveLoadRecord(id)
		headers := make([]kgo.RecordHeader, len(r.Headers))
		for i, h := range r.Headers {
			headers[i] = kgo.RecordHeader{Key: h.Name, Value: h.Value}
		}
		publish(t, p, &kgo.Record{Topic: source, Partition: r.Source.Partition, Key: r.Key, Value: r.Value, Timestamp: time.UnixMilli(*r.TimestampMS), Headers: headers})
	}
	root := t.TempDir()
	archive := filepath.Join(root, "archive")
	prefix := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s') ", broker(t))
	capture := prefix + fmt.Sprintf("INSERT INTO ARCHIVE '%s' SELECT * FROM TOPIC '%s' WITH (GROUP='%s', START=EARLIEST) OPTIONS (WORKER_STOP_AFTER_RECORDS=32)", archive, source, source+"-archive")
	// When capturing through the public command, then completed files retain every record.
	result := run(capture, "")
	require.Zero(t, result.code, result.detail)
	verifyArchive(t, archive, 32, brokerSourceIdentity(t, a, source))
	// When replaying through the public command, then destination routing uses keys and preserves bytes.
	replay := prefix + fmt.Sprintf("INSERT INTO TOPIC '%s' WITH (PARTITIONING = KEY) SELECT * FROM ARCHIVE '%s' WITH (GROUP='restore') ", destination, archive)
	settings := fmt.Sprintf("[worker]\ncheckpoint_dir=%q\n", root)
	result = run(replay, settings)
	require.Zero(t, result.code, result.detail)
	c := consumer(t)
	assign(t, c, destination, 5)
	seen := make([]bool, 32)
	for range 32 {
		r := read(t, c)
		require.GreaterOrEqual(t, len(r.Value), 4)
		id := int(binary.BigEndian.Uint32(r.Value))
		require.Less(t, id, 32)
		require.False(t, seen[id])
		seen[id] = true
		expected := archiveLoadRecord(id)
		require.True(t, bytes.Equal(expected.Value, r.Value))
		require.Equal(t, expected.Key, r.Key)
		require.EqualValues(t, *expected.TimestampMS, r.Timestamp.UnixMilli())
		require.Len(t, r.Headers, len(expected.Headers))
		for i, h := range expected.Headers {
			require.Equal(t, h.Name, r.Headers[i].Key)
			require.Equal(t, h.Value, r.Headers[i].Value)
		}
	}
	result = run(replay, settings)
	require.Zero(t, result.code, result.detail)
	require.EqualValues(t, 32, topicRecordCount(t, a, destination, 5), "completed snapshot must not replay again")
}
