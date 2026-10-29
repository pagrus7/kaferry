package app

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/archive/avro"
	"github.com/pagrus7/kaferry/internal/archive/replay"
	"github.com/pagrus7/kaferry/internal/checkpoint"
	"github.com/pagrus7/kaferry/internal/config"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/pagrus7/kaferry/internal/sql"
	"github.com/stretchr/testify/require"
)

func TestReplayCheckpointLargerThanKafkaRecordLimit(t *testing.T) {
	// Given a legitimate pinned selection whose checkpoint exceeds a Kafka record.
	_, destination := clusters(t, 1)
	root := t.TempDir()
	codec, err := avro.New(64 << 20)
	require.NoError(t, err)
	source := avro.Source{Cluster: strings.Repeat("cluster", 30), Topic: strings.Repeat("topic", 35), TopicID: avro.TopicID{7}, Partition: 0}
	const count = 1600
	stamp := time.Now().Add(-time.Minute).UnixMilli()
	for i := range count {
		identity := avro.Identity{Version: 1, WriterID: strings.Repeat("c", 32), FileID: fmt.Sprintf("%032x", i+1), Source: source, FirstOffset: int64(i)}
		file, err := os.Create(filepath.Join(root, identity.WriterID+"."+identity.FileID+".avro"))
		require.NoError(t, err)
		writer, err := codec.NewWriter(file, identity)
		require.NoError(t, err)
		require.NoError(t, writer.Append(avro.Record{Source: source, Offset: int64(i), Value: []byte(fmt.Sprint(i)), TimestampMS: &stamp, TimestampKind: "create_time"}))
		require.NoError(t, writer.Flush())
		require.NoError(t, file.Close())
	}
	catalog := replay.NewCatalog(root, codec, replay.NativeFiles{})
	selected, err := catalog.Select(nil)
	require.NoError(t, err)
	encoded, err := checkpoint.Encode(selected, 64<<20)
	require.NoError(t, err)
	require.Greater(t, len(encoded), 1<<20)
	text := fmt.Sprintf("INSERT INTO TOPIC 'output@dst' SELECT * FROM ARCHIVE '%s' WITH (GROUP = 'large-selection') OPTIONS (WORKER_STOP_AFTER_RECORDS = 3);", root)
	ast, err := sql.NewParser().Parse(text)
	require.NoError(t, err)
	p, err := plan.NewPlanner(environment{}).Build(ast, plan.Profiles{"dst": {"bootstrap.servers": strings.Join(destination, ",")}})
	require.NoError(t, err)
	cfg := config.Defaults("")
	checkpointDirectory := t.TempDir()
	cfg.Worker.CheckpointDir = &checkpointDirectory
	cfg.Worker.RecordCap = 256
	cfg.Worker.ArchiveReplayOpenFiles = 2
	cfg.Worker.ArchiveReplayBlocks = 2
	// When the large physical cursor is committed and restored, allow its bounded file validation and serialization to complete.
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	require.NoError(t, Run(ctx, p, cfg, Options{ProgressTopic: "progress"}))
	require.Len(t, output(t, destination, 1), 3)
	p.WorkerStopAfterRecords = 0
	require.NoError(t, Run(ctx, p, cfg, Options{ProgressTopic: "progress"}))
	// Then the same pinned selection completes without treating chunks as output records.
	got := output(t, destination, 1)
	require.Len(t, got, count)
	for i := range count {
		require.Equal(t, 1, got[fmt.Sprint(i)])
	}
}
