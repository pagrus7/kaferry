package app

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/archive/avro"
	"github.com/pagrus7/kaferry/internal/archive/replay"
	"github.com/pagrus7/kaferry/internal/config"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/pagrus7/kaferry/internal/sql"
	"github.com/stretchr/testify/require"
)

func archiveInput(t *testing.T, root string) map[string]int {
	t.Helper()
	expected := map[string]int{}
	codec, err := avro.New(64 << 20)
	require.NoError(t, err)
	for file, example := range []struct {
		part    int32
		offsets []int64
	}{{0, []int64{0, 1, 3}}, {0, []int64{1, 2}}, {1, []int64{0, 1, 2}}} {
		source := avro.Source{Cluster: "archive-fixture", Topic: "orders", TopicID: avro.TopicID{1}, Partition: example.part}
		writerID := strings.Repeat("a", 32)
		fileID := fmt.Sprintf("%032x", file+1)
		output, err := os.Create(filepath.Join(root, writerID+"."+fileID+".avro"))
		require.NoError(t, err)
		writer, err := codec.NewWriter(output, avro.Identity{Version: 1, WriterID: writerID, FileID: fileID, Source: source, FirstOffset: example.offsets[0]})
		require.NoError(t, err)
		for _, offset := range example.offsets {
			value := fmt.Sprintf("%d:%d", example.part, offset)
			stamp := time.Now().Add(-time.Minute).UnixMilli()
			record := avro.Record{Source: source, Offset: offset, Key: []byte(fmt.Sprintf("key-%d", offset)), Value: []byte(value), TimestampMS: &stamp, TimestampKind: "create_time"}
			require.NoError(t, writer.Append(record))
			expected[value]++
		}
		require.NoError(t, writer.Flush())
		require.NoError(t, output.Close())
	}
	return expected
}
func archiveValues(t *testing.T, root string) map[string]int {
	t.Helper()
	values, private, err := scanArchive(root)
	require.NoError(t, err)
	require.Zero(t, private)
	return values
}
func scanArchiveRecords(root string) ([]avro.Record, int, error) {
	codec, err := avro.New(64 << 20)
	if err != nil {
		return nil, 0, err
	}
	catalog := replay.NewCatalog(root, codec, replay.NativeFiles{})
	selection, err := catalog.Select(nil)
	if err != nil {
		return nil, 0, err
	}
	progress := make([]replay.Progress, 0, len(selection.Files))
	for _, file := range selection.Files {
		progress = append(progress, replay.Progress{File: file, Next: replay.Position{Block: file.Start, Previous: -1}})
	}
	pool, err := replay.NewPool(catalog, replay.Limits{OpenFiles: 1, Blocks: 1})
	if err != nil {
		return nil, 0, err
	}
	reader, err := replay.NewReader(progress, pool)
	if err != nil {
		_ = pool.Close()
		return nil, 0, err
	}
	defer reader.Close()
	records := []avro.Record{}
	for {
		entry, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, 0, err
		}
		records = append(records, entry.Record)
	}
	return records, selection.PrivateFiles, nil
}

func scanArchive(root string) (map[string]int, int, error) {
	records, private, err := scanArchiveRecords(root)
	if err != nil {
		return nil, private, err
	}
	values := map[string]int{}
	for _, record := range records {
		values[string(record.Value)]++
	}
	return values, private, nil
}

func TestStreamingAllEndpointsAndArchiveRotation(t *testing.T) {
	for _, inputKind := range []string{"TOPIC", "ARCHIVE"} {
		for _, outputKind := range []string{"TOPIC", "ARCHIVE"} {
			t.Run(inputKind+"_to_"+outputKind, func(t *testing.T) {
				// Given bounded work batches, overlapping archive input and an open-file budget of one.
				source, destination := clusters(t, 2)
				sourceEndpoint := "TOPIC 'input@src'"
				targetEndpoint := "TOPIC 'output@dst'"
				expected := map[string]int{}
				if inputKind == "TOPIC" {
					seed(t, source, 2, 40)
					for part := range 2 {
						for offset := range 40 {
							expected[fmt.Sprintf("%d:%d", part, offset)] = 1
						}
					}
				} else {
					path := t.TempDir()
					expected = archiveInput(t, path)
					sourceEndpoint = "ARCHIVE '" + path + "'"
				}
				outputRoot := ""
				if outputKind == "ARCHIVE" {
					outputRoot = t.TempDir()
					targetEndpoint = "ARCHIVE '" + outputRoot + "' WITH (ROTATE_AFTER_SIZE = 1000, ROTATE_AFTER_AGE = INTERVAL '1' SECOND)"
				}
				statement := fmt.Sprintf("INSERT INTO %s SELECT * FROM %s WITH (GROUP = 'matrix-%d') WHERE RECORD_OFFSET < LATEST;", targetEndpoint, sourceEndpoint, time.Now().UnixNano())
				ast, err := sql.NewParser().Parse(statement)
				require.NoError(t, err)
				p, err := plan.NewPlanner(environment{}).Build(ast, plan.Profiles{"src": {"bootstrap.servers": strings.Join(source, ",")}, "dst": {"bootstrap.servers": strings.Join(destination, ",")}})
				require.NoError(t, err)
				cfg := config.Defaults("")
				checkpointDirectory := t.TempDir()
				cfg.Worker.CheckpointDir = &checkpointDirectory
				cfg.Worker.RecordCap = 7
				cfg.Worker.PartitionRecordCap = 5
				cfg.Worker.ArchiveCaptureOpenFiles = 1
				cfg.Worker.ArchiveReplayOpenFiles = 1
				cfg.Worker.ArchiveReplayBlocks = 1
				// When records are checkpointed, rotated and replayed through a completed rerun.
				require.NoError(t, Run(testContext(t), p, cfg, Options{ProgressTopic: "progress"}))
				require.NoError(t, Run(testContext(t), p, cfg, Options{ProgressTopic: "progress"}))
				// Then ordinary archive readers or read_committed Kafka readers see each input occurrence once.
				got := map[string]int{}
				if outputKind == "ARCHIVE" {
					got = archiveValues(t, outputRoot)
				} else {
					got = output(t, destination, 2)
				}
				require.Equal(t, expected, got)
			})
		}
	}
}
func TestArchiveFilterAndWorkerStopResume(t *testing.T) {
	// Given repeated original offsets in overlapping archive files and a physical replay cursor.
	source, destination := clusters(t, 2)
	_ = source
	root := t.TempDir()
	archiveInput(t, root)
	statement := fmt.Sprintf("INSERT INTO TOPIC 'output@dst' WITH (PARTITIONING = KEY) SELECT * FROM ARCHIVE '%s' WITH (GROUP = 'archive-ranges') WHERE PARTITION IN (0) AND RECORD_OFFSET BETWEEN 1 AND 2 OPTIONS (WORKER_STOP_AFTER_RECORDS = 1);", root)
	ast, err := sql.NewParser().Parse(statement)
	require.NoError(t, err)
	p, err := plan.NewPlanner(environment{}).Build(ast, plan.Profiles{"dst": {"bootstrap.servers": strings.Join(destination, ",")}})
	require.NoError(t, err)
	cfg := config.Defaults("")
	checkpointDirectory := t.TempDir()
	cfg.Worker.CheckpointDir = &checkpointDirectory
	cfg.Worker.RecordCap = 2
	// When a capped invocation resumes, then duplicate physical occurrences survive the filters.
	require.NoError(t, Run(testContext(t), p, cfg, Options{ProgressTopic: "progress"}))
	p.WorkerStopAfterRecords = 0
	require.NoError(t, Run(testContext(t), p, cfg, Options{ProgressTopic: "progress"}))
	require.Equal(t, map[string]int{"0:1": 2, "0:2": 1}, output(t, destination, 2))
}

func TestCaptureFileBudgetAppliesAcrossSourcePartitions(t *testing.T) {
	// Given three Kafka source partitions and a worker-wide capture file budget of one.
	source, _ := clusters(t, 3)
	seed(t, source, 3, 4)
	root := t.TempDir()
	p, _ := capturePlan(t, source, root, fmt.Sprintf("file-budget-%d", time.Now().UnixNano()), false)
	cfg := config.Defaults("")
	cfg.Worker.ArchiveCaptureOpenFiles = 1
	cfg.Worker.RecordCap = 2
	cfg.Worker.PartitionRecordCap = 1
	open, peak := 0, 0
	// When each source partition creates output, then publication frees a file before another opens.
	require.NoError(t, Run(testContext(t), p, cfg, Options{Hook: func(point string, _ int32) {
		if point == "file-open" {
			open++
			peak = max(peak, open)
		}
		if point == "file-close" {
			open--
		}
	}}))
	require.Equal(t, 1, peak)
	require.Zero(t, open)
	got := archiveValues(t, root)
	require.Len(t, got, 12)
	for _, count := range got {
		require.Equal(t, 1, count)
	}
}
