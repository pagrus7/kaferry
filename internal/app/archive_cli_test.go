package app

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/pagrus7/kaferry/internal/archive/avro"
	"github.com/pagrus7/kaferry/internal/archive/replay"
	"github.com/pagrus7/kaferry/internal/checkpoint"
	"github.com/stretchr/testify/require"
)

func archiveFixture(t *testing.T, root string, id int, records []avro.Record) {
	t.Helper()
	require.NoError(t, os.MkdirAll(root, 0700))
	codec, err := avro.New(64 << 20)
	require.NoError(t, err)
	identity := avro.Identity{Version: 1, WriterID: fmt.Sprintf("%032x", id), FileID: fmt.Sprintf("%032x", id), Source: records[0].Source, FirstOffset: records[0].Offset}
	f, err := os.Create(filepath.Join(root, identity.WriterID+"."+identity.FileID+".avro"))
	require.NoError(t, err)
	w, err := codec.NewWriter(f, identity)
	require.NoError(t, err)
	for _, r := range records {
		require.NoError(t, w.Append(r))
	}
	require.NoError(t, w.Flush())
	require.NoError(t, f.Close())
}
func archiveRecords(t *testing.T, root string) []avro.Record {
	t.Helper()
	codec, err := avro.New(64 << 20)
	require.NoError(t, err)
	catalog := replay.NewCatalog(root, codec, replay.NativeFiles{})
	selection, err := catalog.Select()
	require.NoError(t, err)
	pool, err := replay.NewPool(catalog, replay.Limits{})
	require.NoError(t, err)
	defer pool.Close()
	progress := make([]replay.Progress, len(selection.Files))
	for i, f := range selection.Files {
		progress[i] = replay.Progress{File: f, Next: replay.Position{Block: f.Start, Previous: -1}}
	}
	reader, err := replay.NewReader(progress, pool)
	require.NoError(t, err)
	var result []avro.Record
	for {
		entry, err := reader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		result = append(result, entry.Record)
	}
	return result
}
func TestArchiveCLIResumeUsesFixedSelectionAndSavesStopReason(t *testing.T) {
	// Given a portable archive with duplicate offsets, nulls, empty bytes and binary headers.
	root := t.TempDir()
	src, dst := filepath.Join(root, "input"), filepath.Join(root, "output")
	source := avro.Source{Cluster: "cluster", Topic: "orders", TopicID: avro.TopicID{1}}
	stamp := int64(1000)
	var records []avro.Record
	for i := range 5 {
		records = append(records, avro.Record{Source: source, Offset: int64(i / 2), TimestampMS: &stamp, TimestampKind: "create_time", Key: []byte{}, Value: []byte{byte(i)}, Headers: []avro.Header{{Key: []byte{255}, Value: nil}, {Key: []byte{255}, Value: []byte{}}}})
	}
	archiveFixture(t, src, 1, records)
	cfg := filepath.Join(root, "worker.toml")
	require.NoError(t, os.WriteFile(cfg, []byte(fmt.Sprintf("[worker]\ncheckpoint_dir=%q\nrecord_cap=2\npartition_record_cap=2\n", root)), 0600))
	sql := fmt.Sprintf("INSERT INTO FILE avro('%s', ROTATE_AFTER_SIZE=1m, ROTATE_AFTER_AGE=INTERVAL '2' HOUR) SELECT * FROM FILE avro('%s') OPTIONS(GROUP='resume'", dst, src)
	execute := func(command, statement string) (int, string) {
		t.Helper()
		var out bytes.Buffer
		code := Execute(t.Context(), []string{command, "-e", statement, "-c", cfg}, nil, &out, &out)
		return code, out.String()
	}
	// When the worker stops at its requested cap, then status reports that stop.
	code, logs := execute("run", sql+",WORKER_STOP_AFTER_RECORDS=2)")
	require.Zero(t, code, logs)
	state, err := replay.ReadCheckpoint(root, "resume", replay.NativeGroupFiles{})
	require.NoError(t, err)
	require.EqualValues(t, 2, state.Position)
	require.False(t, state.Complete)
	require.Equal(t, "worker_record_cap", state.Stop.Reason)
	code, logs = execute("status", sql+")")
	require.Zero(t, code, logs)
	require.Contains(t, logs, "worker record threshold reached")
	require.Contains(t, logs, "input records 2")
	extra := records[4]
	extra.Offset = 10
	archiveFixture(t, src, 2, []avro.Record{extra})
	// When the same group resumes, then it finishes only the originally selected file.
	code, logs = execute("run", sql+")")
	require.Zero(t, code, logs)
	require.Equal(t, records, archiveRecords(t, dst), "portable records and physical duplicates survive resume")
	state, err = replay.ReadCheckpoint(root, "resume", replay.NativeGroupFiles{})
	require.NoError(t, err)
	require.True(t, state.Complete)
	require.Equal(t, "eof", state.Stop.Reason)
	code, logs = execute("run", sql+")")
	require.Zero(t, code, logs)
	require.Equal(t, records, archiveRecords(t, dst), "completed group copies nothing more")
	// When explaining, then positions are records and no lock or data file changes are made.
	before, err := os.ReadFile(checkpoint.Path(root, "resume"))
	require.NoError(t, err)
	code, logs = execute("explain", sql+")")
	require.Zero(t, code, logs)
	require.Contains(t, logs, "Input position: 5 records")
	after, err := os.ReadFile(checkpoint.Path(root, "resume"))
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestArchiveCLIAllFilteredRecordsCanExceedPendingCap(t *testing.T) {
	// Given more filtered inputs than the worker's pending-record capacity.
	root := t.TempDir()
	src, dst := filepath.Join(root, "input"), filepath.Join(root, "output")
	source := avro.Source{Cluster: "cluster", Topic: "orders", TopicID: avro.TopicID{1}}
	var records []avro.Record
	for i := range 19 {
		ms := int64(i)
		records = append(records, avro.Record{Source: source, Offset: int64(i), TimestampMS: &ms, TimestampKind: "create_time", Headers: []avro.Header{}})
	}
	archiveFixture(t, src, 1, records)
	cfg := filepath.Join(root, "worker.toml")
	require.NoError(t, os.WriteFile(cfg, []byte(fmt.Sprintf("[worker]\ncheckpoint_dir=%q\nrecord_cap=2\npartition_record_cap=2\n", root)), 0600))
	query := fmt.Sprintf("INSERT INTO FILE avro('%s') SELECT * FROM FILE avro('%s') WHERE RECORD_TIMESTAMP >= TIMESTAMP '2000-01-01T00:00:00Z' OPTIONS(GROUP='filtered')", dst, src)
	// When copying through the real engine, then it checkpoints filtered inputs and reaches EOF.
	var out bytes.Buffer
	require.Zero(t, Execute(t.Context(), []string{"run", "-e", query, "-c", cfg}, nil, &out, &out), out.String())
	state, err := replay.ReadCheckpoint(root, "filtered", replay.NativeGroupFiles{})
	require.NoError(t, err)
	require.EqualValues(t, len(records), state.Position)
	require.True(t, state.Complete)
	require.Equal(t, "eof", state.Stop.Reason)
	require.Empty(t, archiveRecords(t, dst), "filtered capture creates no empty data files")
}
