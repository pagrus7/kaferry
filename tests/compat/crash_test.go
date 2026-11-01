//go:build integration && fault

package compat

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestCrashResumeReconcilesRetainedRecords(t *testing.T) {
	// Given a following copy and deterministic records retained throughout recovery.
	a := admin(t)
	src, dst := topic(t, a, 1), topic(t, a, 1)
	group := src + "-crash"
	producer := producer(t)
	reader := consumer(t)
	assign(t, reader, dst, 1)
	stamp := time.Now().Truncate(time.Millisecond)
	publishRange := func(first, end int) {
		for id := first; id < end; id++ {
			value := make([]byte, 8)
			binary.BigEndian.PutUint64(value, uint64(id))
			publish(t, producer, &kgo.Record{Topic: src, Key: []byte{0, 255}, Value: value, Timestamp: stamp,
				Headers: []kgo.RecordHeader{{Key: "dup", Value: nil}, {Key: "dup", Value: []byte{}}}})
		}
	}
	publishRange(0, 16)
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	statement := fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' WITH (GROUP='%s', START=EARLIEST)", dst, src, group)
	config := configuration(t)
	process := startCLI(t, ctx, testBinary(t), "", "run", "-e", statement, "-c", config)
	waitEnd(t, a, dst, 1)
	// When the process dies without a drain/save and a fresh process resumes.
	require.NoError(t, process.command.Process.Kill())
	require.Error(t, process.wait())
	publishRange(16, 64)
	resumed := startCLI(t, ctx, testBinary(t), "", "run", "-e", statement, "-c", config)
	seen, duplicates := make(map[uint64]bool), 0
	for len(seen) < 64 {
		record := read(t, reader)
		require.Len(t, record.Value, 8)
		id := binary.BigEndian.Uint64(record.Value)
		require.Less(t, id, uint64(64))
		require.Equal(t, []byte{0, 255}, record.Key)
		require.Equal(t, stamp, record.Timestamp)
		require.Equal(t, []kgo.RecordHeader{{Key: "dup", Value: nil}, {Key: "dup", Value: []byte{}}}, record.Headers)
		if seen[id] {
			duplicates++
		}
		seen[id] = true
	}
	require.NoError(t, resumed.signal(os.Interrupt))
	require.NoError(t, resumed.wait(), resumed.logs())
	// Then every retained input survives; replay duplicates respect at-least-once delivery.
	progress := committedPartitions(t, a, group, src, 1)
	require.Equal(t, int64(64), progress[0].At)
	t.Logf("unique_records=%d replay_duplicates=%d missing=0 corrupted=0", len(seen), duplicates)
}
