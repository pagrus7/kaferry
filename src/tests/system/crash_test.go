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
	oracle := startFaultOracle(t, "fault/crash", broker(t), broker(t), src, dst, group, 1, false)
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
	statement := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s','session.timeout.ms'='%d','heartbeat.interval.ms'='1000') INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' WITH (GROUP='%s', START=EARLIEST)",
		broker(t), oracle.timeouts["session.timeout.ms"].Milliseconds(), dst, src, group)
	config := configuration(t, fmt.Sprintf("delivery_timeout=%q\nshutdown_timeout=%q\n", oracle.timeouts["delivery_timeout"], oracle.timeouts["shutdown_timeout"]))
	process := startCLI(t, ctx, testBinary(t), "", "run", "-e", statement, "-c", config)
	oracle.writer(process)
	waitEnd(t, a, dst, 1)
	// When the process dies without a drain/save and a fresh process resumes.
	window := oracle.begin(t, "process-kill", map[string]any{"worker": "original", "pid": process.pid()}, false,
		oracle.timeouts,
		"destination partition 0 has at least one record")
	require.NoError(t, process.command.Process.Kill())
	require.Error(t, process.wait())
	oracle.injected(t, window, "process-kill", map[string]int{"pid": process.pid()}, "original worker exited after kill")
	publishRange(16, 64)
	waitMembers(t, a, group, 0, 0, nil)
	resumed := startCLI(t, ctx, testBinary(t), "", "run", "-e", statement, "-c", config)
	oracle.writer(resumed)
	waitMembers(t, a, group, 1, 1, nil)
	oracle.healed(t, window, "replacement owns the source partition")
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
	oracle.finish(t)
}
