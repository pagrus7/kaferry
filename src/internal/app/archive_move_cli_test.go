package app

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/pagrus7/kaferry/src/internal/archive/avro"
	"github.com/pagrus7/kaferry/src/internal/archive/replay"
	"github.com/pagrus7/kaferry/src/internal/checkpoint"
	"github.com/pagrus7/kaferry/src/internal/inspect"
	"github.com/stretchr/testify/require"
)

func TestArchiveCLIMoveReplayResumesSavedProgressWithoutDuplicates(t *testing.T) {
	// Given a replay stopped successfully after copying only part of its input.
	root, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	original := filepath.Join(root, "original")
	src, dst, progress := filepath.Join(original, "input"), filepath.Join(original, "output"), filepath.Join(original, "checkpoints")
	source := avro.Source{Cluster: "cluster", Topic: "orders", TopicID: avro.TopicID{1}}
	var records []avro.Record
	for offset := range int64(5) {
		records = append(records, avro.Record{Source: source, Offset: offset, TimestampKind: "unknown", Value: []byte(fmt.Sprint(offset)), Headers: []avro.Header{}})
	}
	archiveFixture(t, src, 1, records)
	require.NoError(t, os.MkdirAll(progress, 0700))
	cfg := filepath.Join(root, "worker.toml")
	configure := func(directory string) {
		t.Helper()
		require.NoError(t, os.WriteFile(cfg, []byte(fmt.Sprintf("[worker]\ncheckpoint_dir=%q\n", directory)), 0600))
	}
	query := func(input, output string) string {
		return fmt.Sprintf("INSERT INTO ARCHIVE '%s' SELECT * FROM ARCHIVE '%s' WITH(GROUP='move-replay')", output, input)
	}
	execute := func(command, statement string, asJSON bool) (int, []byte, string) {
		t.Helper()
		var out, diagnostics bytes.Buffer
		args := []string{command, "-e", statement, "-c", cfg}
		if asJSON {
			args = append(args, "--json")
		}
		code := Execute(t.Context(), args, nil, &out, &diagnostics)
		return code, out.Bytes(), diagnostics.String()
	}
	configure(progress)
	const stopAfter = 2
	code, _, logs := execute("run", query(src, dst)+fmt.Sprintf(" OPTIONS(WORKER_STOP_AFTER_RECORDS=%d)", stopAfter), false)
	require.Zero(t, code, logs)
	require.Equal(t, records[:stopAfter], archiveRecords(t, dst))
	state, err := replay.ReadCheckpoint(progress, "move-replay", replay.NativeGroupFiles{})
	require.NoError(t, err)
	require.EqualValues(t, stopAfter, state.Position)
	require.False(t, state.Complete)
	writerLocks, err := filepath.Glob(filepath.Join(dst, ".kaferry", "writers", "*", "lock"))
	require.NoError(t, err)
	require.NotEmpty(t, writerLocks, "the output includes hidden capture state and its lock")
	checkpointLocks, err := filepath.Glob(filepath.Join(progress, "*.lock"))
	require.NoError(t, err)
	require.NotEmpty(t, checkpointLocks)

	// When the entire input, output and checkpoint directories move, including hidden files and locks.
	relocated := filepath.Join(root, "relocated")
	newSrc, newDst, newProgress := filepath.Join(relocated, "archive-input"), filepath.Join(relocated, "archive-output"), filepath.Join(relocated, "saved-progress")
	for _, directory := range [][2]string{{src, newSrc}, {dst, newDst}, {progress, newProgress}} {
		require.NoError(t, os.CopyFS(directory[1], os.DirFS(directory[0])))
	}
	require.NoError(t, os.RemoveAll(original))
	require.NoDirExists(t, original)
	configure(newProgress)
	relocatedQuery := query(newSrc, newDst)

	// Then explain finds the relocated saved position before the replay resumes.
	code, data, logs := execute("explain", relocatedQuery, true)
	require.Zero(t, code, logs)
	var report inspect.Report
	require.NoError(t, json.Unmarshal(data, &report))
	require.NotNil(t, report.Source.File)
	require.NotNil(t, report.Source.File.CheckpointExists)
	require.True(t, *report.Source.File.CheckpointExists)
	require.Equal(t, checkpoint.Path(newProgress, "move-replay"), report.Source.File.Checkpoint)
	require.NotNil(t, report.Source.File.PositionRecords)
	require.EqualValues(t, stopAfter, *report.Source.File.PositionRecords)

	// When rerunning at the new paths, then every input record appears exactly once in the moved output.
	code, _, logs = execute("run", relocatedQuery, false)
	require.Zero(t, code, logs)
	require.Equal(t, records, archiveRecords(t, newDst))
	state, err = replay.ReadCheckpoint(newProgress, "move-replay", replay.NativeGroupFiles{})
	require.NoError(t, err)
	require.True(t, state.Complete)
	require.EqualValues(t, len(records), state.Position)
	code, _, logs = execute("run", relocatedQuery, false)
	require.Zero(t, code, logs)
	require.Equal(t, records, archiveRecords(t, newDst), "a completed replay remains complete after relocation")
}
