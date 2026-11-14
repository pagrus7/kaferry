//go:build integration

package compat

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/src/internal/archive/capture"
	"github.com/pagrus7/kaferry/src/internal/checkpoint"
	core "github.com/pagrus7/kaferry/src/internal/copy"
	"github.com/pagrus7/kaferry/src/internal/inspect"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestArchiveCaptureCrashRecoversAcknowledgedData(t *testing.T) {
	// Given a following capture with rotated files and an acknowledged private tail.
	a := admin(t)
	src := topic(t, a, 2)
	group := src + "-archive-crash"
	p := producer(t)
	identity := brokerSourceIdentity(t, a, src)
	stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond).UnixMilli()
	var expected []core.Record
	publishRange := func(first, end int) {
		t.Helper()
		for id := first; id < end; id++ {
			size := 2048
			if id < 8 {
				size = 32768 // Each initial record exceeds the rotation size, even after compression.
			}
			record := archiveLoadRecordPayload(id, 2, [2]int{size, size}, true, 42)
			record.Key, record.TimestampMS = []byte{0, 255}, &stamp
			record.Source.Cluster, record.Source.TopicID, record.Source.Topic = identity.Cluster, identity.TopicID, src
			message := &kgo.Record{Topic: src, Partition: record.Source.Partition, Key: record.Key, Value: record.Value, Timestamp: time.UnixMilli(stamp)}
			for _, header := range record.Headers {
				message.Headers = append(message.Headers, kgo.RecordHeader{Key: header.Name, Value: header.Value})
			}
			publish(t, p, message)
			record.Source.Offset = message.Offset
			expected = append(expected, record)
		}
	}
	publishRange(0, 8)
	root := filepath.Join(t.TempDir(), "archive")
	config, binaryPath := configuration(t), testBinary(t)
	query := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s', 'session.timeout.ms'='6000', 'heartbeat.interval.ms'='1000') "+
		"INSERT INTO ARCHIVE '%s' WITH (ROTATE_AFTER_SIZE = 16k) SELECT * FROM TOPIC '%s' WITH (GROUP = '%s', START = EARLIEST)", broker(t), root, src, group)
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	process := startCLI(t, ctx, binaryPath, "", "run", "-e", query, "-c", config)
	waitMembers(t, a, group, 1, 2, nil)
	waitSaved := func(want int64) {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
		for {
			parts := committedPartitions(t, a, group, src, 2)
			if parts[0].At == want && parts[1].At == want {
				return
			}
			require.False(t, process.finished(), "capture exited before saving offset %d", want)
			require.True(t, time.Now().Before(deadline), "capture did not save offset %d: %+v", want, parts)
			time.Sleep(20 * time.Millisecond)
		}
	}
	waitSaved(4)
	completed, err := filepath.Glob(filepath.Join(root, "*.avro"))
	require.NoError(t, err)
	require.NotEmpty(t, completed, "rotation must publish files before the kill")
	publishRange(8, 10)
	waitSaved(5)
	private, err := filepath.Glob(filepath.Join(root, ".*.partial"))
	require.NoError(t, err)
	require.Len(t, private, 2, "both source partitions must have private files before the kill")
	states, err := filepath.Glob(filepath.Join(root, ".kaferry", "writers", "*", "state.json"))
	require.NoError(t, err)
	require.Len(t, states, 1)
	journal := capture.NewJournal(capture.NativeFiles{}, checkpoint.DurableWriter{})
	before, err := journal.Load(states[0])
	require.NoError(t, err)
	require.Len(t, before.Files, 2, "private tails must be durably acknowledged before the kill")
	saved := committedPartitions(t, a, group, src, 2)
	for _, boundary := range before.Files {
		require.Less(t, boundary.Identity.FirstOffset, saved[boundary.Identity.Source.Partition].At)
		require.Positive(t, boundary.Records)
	}
	t.Logf("pre-kill saved offsets: %+v; completed=%d private=%d", saved, len(completed), len(private))

	// When SIGKILL interrupts capture, more input arrives and the same query restarts.
	completed, err = filepath.Glob(filepath.Join(root, "*.avro"))
	require.NoError(t, err)
	require.NoError(t, process.signal(os.Kill))
	require.Error(t, process.wait())
	waitStatus, ok := process.command.ProcessState.Sys().(syscall.WaitStatus)
	require.True(t, ok)
	require.Equal(t, syscall.SIGKILL, waitStatus.Signal())
	publishRange(10, 26)
	waitMembers(t, a, group, 0, 0, nil)
	process = startCLI(t, ctx, binaryPath, "", "run", "-e", query, "-c", config)
	waitMembers(t, a, group, 1, 2, nil)
	waitSaved(13)
	require.NoError(t, process.signal(os.Interrupt))
	require.NoError(t, process.wait(), process.logs())
	require.Zero(t, process.command.ProcessState.ExitCode(), "SIGINT must stop a Kafka follow run successfully")
	waitMembers(t, a, group, 0, 0, nil)

	// Then recovery publishes the old files, independently of copying from saved offsets.
	for _, path := range private {
		published := filepath.Join(root, strings.TrimSuffix(strings.TrimPrefix(filepath.Base(path), "."), ".partial")+".avro")
		require.FileExists(t, published, "startup recovery must publish the old acknowledged private file %s", filepath.Base(path))
		completed = append(completed, published)
	}
	remaining, err := filepath.Glob(filepath.Join(root, ".*.partial"))
	require.NoError(t, err)
	require.Empty(t, remaining, "recovery and graceful shutdown must leave no private files")
	after, err := journal.Load(states[0])
	require.NoError(t, err)
	require.Equal(t, before.WriterID, after.WriterID)
	require.Empty(t, after.Files, "the old writer must have no pending recovery entries")
	entries, err := os.ReadDir(filepath.Dir(states[0]))
	require.NoError(t, err)
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	require.ElementsMatch(t, []string{"lock", "state.json"}, names, "only the documented stable namespace files persist")

	// Read only files that existed before the kill, including the recovered private tails.
	acknowledged := t.TempDir()
	for _, path := range completed {
		require.NoError(t, os.Link(path, filepath.Join(acknowledged, filepath.Base(path))))
	}
	seenAcknowledged := make(map[int]bool)
	for _, record := range archiveRecords(t, acknowledged) {
		require.GreaterOrEqual(t, len(record.Value), 4)
		id := int(binary.BigEndian.Uint32(record.Value))
		require.Less(t, id, len(expected))
		require.Equal(t, expected[id], record, "acknowledged record %d retains its full source record", id)
		seenAcknowledged[id] = true
	}
	for id, record := range expected {
		if record.Source.Offset < saved[record.Source.Partition].At {
			require.True(t, seenAcknowledged[id], "acknowledged partition %d offset %d missing from the old completed files; startup recovery was required", record.Source.Partition, record.Source.Offset)
		}
	}

	// Replay into a fresh archive/group so reconciliation also retains original routing and offsets.
	clone := filepath.Join(t.TempDir(), "replayed")
	replayQuery := fmt.Sprintf("INSERT INTO ARCHIVE '%s' SELECT * FROM ARCHIVE '%s' WITH (GROUP = 'crash-reconcile')", clone, root)
	replay := startCLI(t, ctx, binaryPath, "", "run", "-e", replayQuery, "-c", config)
	require.NoError(t, replay.wait(), replay.logs())
	counts, duplicates := make(map[int]int), 0
	for _, record := range archiveRecords(t, clone) {
		require.GreaterOrEqual(t, len(record.Value), 4)
		id := int(binary.BigEndian.Uint32(record.Value))
		require.Less(t, id, len(expected), "replay must not invent records")
		require.Equal(t, expected[id], record, "record %d retains key, value, ordered headers, timestamp and source identity", id)
		if counts[id] > 0 {
			duplicates++
		}
		counts[id]++
	}
	for id := range expected {
		require.Positive(t, counts[id], "source record %d missing after crash/restart/replay", id)
	}
	t.Logf("unique_records=%d replay_duplicates=%d missing=0 corrupted=0", len(counts), duplicates)

	status := startCLI(t, ctx, binaryPath, "", "status", "-e", query, "-c", config, "--json")
	require.Error(t, status.wait())
	require.Equal(t, 4, status.command.ProcessState.ExitCode(), status.logs())
	var report inspect.StatusReport
	require.NoError(t, json.Unmarshal([]byte(status.logs()), &report))
	require.Equal(t, "incomplete", report.Verdict, "a stopped Kafka follow run has no saved completion rule")
	require.Len(t, report.Partitions, 2)
	for _, partition := range report.Partitions {
		require.NotNil(t, partition.Position)
		require.EqualValues(t, 13, *partition.Position)
		require.Nil(t, partition.Stop)
	}
	require.NoError(t, ctx.Err(), "crash/restart/replay/status must finish before the deadline")
}
