//go:build integration

package compat

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/checkpoint"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestPreservedPartitionsAndExplicitKeyRouting(t *testing.T) {
	// Given source records spread over three partitions, including null and empty keys.
	a := admin(t)
	source := topic(t, a, 3)
	p := producer(t)
	stamp := time.Now().Add(-10 * time.Minute).Truncate(time.Millisecond)
	keys := [][]byte{nil, {}, {0, 255}, nil, {}, {3, 0}}
	for id, key := range keys {
		publish(t, p, &kgo.Record{Topic: source, Partition: int32(id % 3), Key: key, Value: []byte{byte(id)}, Timestamp: stamp})
	}
	config := configuration(t)
	verify := func(t *testing.T, destination string, parts int, preserve bool) {
		t.Helper()
		reader := consumer(t)
		assign(t, reader, destination, parts)
		seen := make(map[byte]bool)
		for range keys {
			r := read(t, reader)
			require.Len(t, r.Value, 1)
			id := r.Value[0]
			require.Less(t, int(id), len(keys))
			require.False(t, seen[id])
			seen[id] = true
			require.Equal(t, keys[id], r.Key)
			if preserve {
				require.EqualValues(t, int(id)%3, r.Partition)
			} else {
				require.Zero(t, r.Partition)
			}
		}
	}
	t.Run("default preserves into a larger topic", func(t *testing.T) {
		// When copying with the default, then each original ID remains unchanged despite different counts.
		destination := topic(t, a, 5)
		query := fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' WHERE RECORD_TIMESTAMP < TIMESTAMP '%s'", destination, source, stamp.Add(time.Second).Format(time.RFC3339Nano))
		execute(t, query, config)
		verify(t, destination, 5, true)
		// When bare SQL is rerun, then its fresh group copies the source again.
		execute(t, query, config)
		require.EqualValues(t, 2*len(keys), topicRecordCount(t, a, destination, 5))
	})
	t.Run("missing partition is fatal before progress", func(t *testing.T) {
		// When the destination is too small, then ON_ERROR=SKIP cannot suppress the layout error or create progress.
		destination := topic(t, a, 2)
		group := source + "-reject"
		query := fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' WITH(GROUP='%s',START=EARLIEST) OPTIONS(ON_ERROR=SKIP)", destination, source, group)
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		result := binaryWorker(t, ctx, query, config)
		require.Equal(t, 2, result.code)
		require.Contains(t, result.detail, "original partition 2")
		require.Zero(t, topicRecordCount(t, a, destination, 2))
		offsets, err := a.FetchOffsets(t.Context(), group)
		if err != nil {
			require.ErrorIs(t, err, kerr.GroupIDNotFound)
		} else {
			require.Empty(t, offsets)
		}
	})
	t.Run("explicit key routing permits a smaller topic", func(t *testing.T) {
		// When KEY is explicit, then original keys route through the destination partitioner.
		destination := topic(t, a, 1)
		query := fmt.Sprintf("INSERT INTO TOPIC '%s' WITH(PARTITIONING=KEY) SELECT * FROM TOPIC '%s' WITH(GROUP='%s-key',START=EARLIEST) OPTIONS(WORKER_STOP_AFTER_RECORDS=6)", destination, source, source)
		execute(t, query, config)
		verify(t, destination, 1, false)
	})
	t.Run("archive replay preserves provenance", func(t *testing.T) {
		// Given capture files retaining each record's original partition.
		root := t.TempDir()
		archive := filepath.Join(root, "archive")
		cfg := filepath.Join(root, "worker.toml")
		require.NoError(t, os.WriteFile(cfg, []byte(fmt.Sprintf("[connections.default]\n'bootstrap.servers'=%q\n[worker]\ncheckpoint_dir=%q\n", broker(t), root)), 0600))
		capture := fmt.Sprintf("INSERT INTO FILE avro('%s') SELECT * FROM TOPIC '%s' OPTIONS(WORKER_STOP_AFTER_RECORDS=6)", archive, source)
		execute(t, capture, cfg)
		destination := topic(t, a, 5)
		query := fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM FILE avro('%s')", destination, archive)
		// When replaying, then routing uses original partitions rather than the file's virtual cursor.
		execute(t, query, cfg)
		verify(t, destination, 5, true)
		// When replaying into too few partitions, then reject before saving the fixed file selection.
		small := topic(t, a, 2)
		query = fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM FILE avro('%s') WITH(GROUP='reject-file') OPTIONS(ON_ERROR=SKIP)", small, archive)
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		result := binaryWorker(t, ctx, query, cfg)
		require.Equal(t, 2, result.code)
		require.Contains(t, result.detail, "original partition 2")
		_, err := os.Stat(checkpoint.Path(root, "reject-file"))
		require.ErrorIs(t, err, os.ErrNotExist)
		require.Zero(t, topicRecordCount(t, a, small, 2))
	})
}
