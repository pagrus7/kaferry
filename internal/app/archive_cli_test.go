package app

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/archive/avro"
	"github.com/pagrus7/kaferry/internal/archive/replay"
	"github.com/pagrus7/kaferry/internal/checkpoint"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/inspect"
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
func writeArchiveRecords(t *testing.T, root string, records []core.Record) {
	t.Helper()
	if len(records) == 0 {
		require.NoError(t, os.MkdirAll(root, 0700))
		return
	}
	values := make([]avro.Record, len(records))
	for index, record := range records {
		source := &core.Provenance{Cluster: "fixture", Topic: "orders", TopicID: [16]byte{1}, Offset: int64(index)}
		if record.Source != nil {
			source = record.Source
		}
		values[index] = avro.Record{Source: avro.Source{Cluster: source.Cluster, Topic: source.Topic, TopicID: avro.TopicID(source.TopicID), Partition: source.Partition}, Offset: source.Offset, Key: record.Key, Value: record.Value, TimestampMS: record.TimestampMS, TimestampKind: record.TimestampType, Headers: make([]avro.Header, len(record.Headers))}
		for i, header := range record.Headers {
			values[index].Headers[i] = avro.Header{Key: []byte(header.Name), Value: header.Value}
		}
	}
	archiveFixture(t, root, 1, values)
}

func archiveRecords(t *testing.T, root string) []avro.Record {
	t.Helper()
	codec, err := avro.New(64 << 20)
	require.NoError(t, err)
	catalog := replay.NewCatalog(root, codec, replay.NativeFiles{})
	selection, err := catalog.Select(nil)
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
	sql := fmt.Sprintf("INSERT INTO FILE avro('%s') WITH(ROTATE_AFTER_SIZE=1m, ROTATE_AFTER_AGE=INTERVAL '2' HOUR) SELECT * FROM FILE avro('%s') WITH(GROUP='resume')", dst, src)
	execute := func(command, statement string) (int, string) {
		t.Helper()
		var out bytes.Buffer
		code := Execute(t.Context(), []string{command, "-e", statement, "-c", cfg}, nil, &out, &out)
		return code, out.String()
	}
	// When the worker stops at its requested cap, then status reports that stop.
	code, logs := execute("run", sql+" OPTIONS(WORKER_STOP_AFTER_RECORDS=2)")
	require.Zero(t, code, logs)
	state, err := replay.ReadCheckpoint(root, "resume", replay.NativeGroupFiles{})
	require.NoError(t, err)
	require.EqualValues(t, 2, state.Position)
	require.False(t, state.Complete)
	require.Equal(t, "worker_record_cap", state.Stop.Reason)
	code, logs = execute("status", sql)
	require.Zero(t, code, logs)
	require.Contains(t, logs, "worker record threshold reached")
	require.Contains(t, logs, "input records 2")
	extra := records[4]
	extra.Offset = 10
	archiveFixture(t, src, 2, []avro.Record{extra})
	// When more files appear, then status still describes the saved selection.
	code, logs = execute("status", sql)
	require.Zero(t, code, logs)
	require.Contains(t, logs, fmt.Sprintf("Snapshot: 1 completed files, %d encoded bytes.", state.Files[0].File.Size))
	// When the same group resumes, then it finishes only the originally selected file.
	code, logs = execute("run", sql)
	require.Zero(t, code, logs)
	require.Equal(t, records, archiveRecords(t, dst), "portable records and physical duplicates survive resume")
	state, err = replay.ReadCheckpoint(root, "resume", replay.NativeGroupFiles{})
	require.NoError(t, err)
	require.True(t, state.Complete)
	require.Equal(t, "eof", state.Stop.Reason)
	code, logs = execute("run", sql)
	require.Zero(t, code, logs)
	require.Equal(t, records, archiveRecords(t, dst), "completed group copies nothing more")
	// When explaining, then positions are records and no lock or data file changes are made.
	before, err := os.ReadFile(checkpoint.Path(root, "resume"))
	require.NoError(t, err)
	code, logs = execute("explain", sql)
	require.Zero(t, code, logs)
	require.Contains(t, logs, "Input position: 5 records")
	require.Contains(t, logs, "end of this group's fixed selection")
	after, err := os.ReadFile(checkpoint.Path(root, "resume"))
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestArchivePartitionCLIInspectsCopiesAndResumesOnlyItsSelectedFiles(t *testing.T) {
	for _, predicate := range []string{"PARTITION IN (5)", "PARTITION NOT IN (0)"} {
		t.Run(predicate, func(t *testing.T) {
			// Given two original partitions; the excluded one has no timestamps.
			root := t.TempDir()
			src, dst := filepath.Join(root, "input"), filepath.Join(root, "output")
			source := avro.Source{Cluster: "cluster", Topic: "orders", TopicID: avro.TopicID{1}}
			archiveFixture(t, src, 1, []avro.Record{{Source: source, TimestampKind: "unknown"}})
			source.Partition = 5
			stamp := int64(1000)
			want := []avro.Record{
				{Source: source, Offset: 10, TimestampMS: &stamp, TimestampKind: "create_time", Value: []byte("first"), Headers: []avro.Header{}},
				{Source: source, Offset: 11, TimestampMS: &stamp, TimestampKind: "create_time", Value: []byte("second"), Headers: []avro.Header{}},
			}
			archiveFixture(t, src, 2, want)
			cfg := filepath.Join(root, "worker.toml")
			require.NoError(t, os.WriteFile(cfg, []byte(fmt.Sprintf("[worker]\ncheckpoint_dir=%q\n", root)), 0600))
			query := fmt.Sprintf("INSERT INTO FILE avro('%s') SELECT * FROM FILE avro('%s') WITH(GROUP='selected') WHERE %s AND RECORD_TIMESTAMP < TIMESTAMP '2000-01-01T00:00:00Z'", dst, src, predicate)
			call := func(command, statement string, asJSON bool) (int, []byte, string) {
				t.Helper()
				var out, diagnostics bytes.Buffer
				args := []string{command, "-e", statement, "-c", cfg}
				if asJSON {
					args = append(args, "--json")
				}
				code := Execute(t.Context(), args, nil, &out, &diagnostics)
				return code, out.Bytes(), diagnostics.String()
			}
			// When inspecting, then the public report describes only matching files and preserves the predicate.
			code, data, logs := call("explain", query, true)
			require.Zero(t, code, logs)
			var preview inspect.Report
			require.NoError(t, json.Unmarshal(data, &preview))
			require.Equal(t, 1, preview.Source.File.FileCount)
			require.NotNil(t, preview.Source.PartitionFilter)
			require.Equal(t, strings.Contains(predicate, "NOT"), preview.Source.PartitionFilter.Exclude)
			// When stopping at one accepted write, then the checkpoint contains only partition 5 while its cursor counts records.
			code, _, logs = call("run", query+" OPTIONS(WORKER_STOP_AFTER_RECORDS=1)", false)
			require.Zero(t, code, logs)
			state, err := replay.ReadCheckpoint(root, "selected", replay.NativeGroupFiles{})
			require.NoError(t, err)
			require.Len(t, state.Files, 1)
			require.EqualValues(t, 5, state.Files[0].File.Identity.Source.Partition)
			require.EqualValues(t, 1, state.Position)
			require.False(t, state.Complete)
			extra := want[1]
			extra.Offset = 12
			archiveFixture(t, src, 3, []avro.Record{extra})
			// When resuming after additions, then copy the original selection and report its EOF.
			code, _, logs = call("run", query, false)
			require.Zero(t, code, logs)
			require.Equal(t, want, archiveRecords(t, dst))
			code, data, logs = call("status", query, true)
			require.Zero(t, code, logs)
			var status inspect.StatusReport
			require.NoError(t, json.Unmarshal(data, &status))
			require.Equal(t, "stopped", status.Verdict)
			require.Equal(t, 1, status.Source.File.FileCount)
			require.EqualValues(t, 0, status.Partitions[0].ID)
			require.EqualValues(t, 2, *status.Partitions[0].Position)
			// A different predicate cannot reuse the saved group or change its checkpoint.
			path := checkpoint.Path(root, "selected")
			before, err := os.ReadFile(path)
			require.NoError(t, err)
			code, _, logs = call("run", strings.Replace(query, predicate, "PARTITION IN (0)", 1), false)
			require.Equal(t, 2, code)
			require.Contains(t, logs, "query identity mismatch")
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
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
	query := fmt.Sprintf("INSERT INTO FILE avro('%s') SELECT * FROM FILE avro('%s') WITH (GROUP='filtered') WHERE RECORD_TIMESTAMP >= TIMESTAMP '2000-01-01T00:00:00Z' ", dst, src)
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

func TestArchivePartitionMissingIDsFailBeforeCheckpointOrOutput(t *testing.T) {
	for _, command := range []string{"run", "explain", "status"} {
		// Given a listed partition absent from all completed headers.
		root := t.TempDir()
		src, dst := filepath.Join(root, "input"), filepath.Join(root, "output")
		archiveFixture(t, src, 1, []avro.Record{{Source: avro.Source{Cluster: "cluster", Topic: "orders", TopicID: avro.TopicID{1}}, TimestampKind: "unknown"}})
		cfg := filepath.Join(root, "worker.toml")
		require.NoError(t, os.WriteFile(cfg, []byte(fmt.Sprintf("[worker]\ncheckpoint_dir=%q\n", root)), 0600))
		query := fmt.Sprintf("INSERT INTO FILE avro('%s') SELECT * FROM FILE avro('%s') WITH(GROUP='missing') WHERE PARTITION NOT IN (7)", dst, src)
		var out bytes.Buffer
		// When executing or inspecting, then the same validation classification applies and no selection is pinned.
		require.Equal(t, 2, Execute(t.Context(), []string{command, "-e", query, "-c", cfg}, nil, &out, &out), out.String())
		require.Contains(t, out.String(), "absent from source metadata")
		_, err := os.Stat(checkpoint.Path(root, "missing"))
		require.ErrorIs(t, err, os.ErrNotExist)
		_, err = os.Stat(dst)
		require.ErrorIs(t, err, os.ErrNotExist)
	}
}

func TestArchiveCLIFreshPreviewDoesNotCreateRunState(t *testing.T) {
	// Given a completed file, an unfinished file and a query with a relative time window.
	root := t.TempDir()
	src, dst := filepath.Join(root, "input"), filepath.Join(root, "output")
	archiveFixture(t, src, 1, []avro.Record{{Source: avro.Source{Cluster: "cluster", Topic: "orders", TopicID: avro.TopicID{1}}, TimestampKind: "unknown"}})
	require.NoError(t, os.WriteFile(filepath.Join(src, "capture.partial"), nil, 0600))
	cfg := filepath.Join(root, "worker.toml")
	require.NoError(t, os.WriteFile(cfg, []byte(fmt.Sprintf("[worker]\ncheckpoint_dir=%q\n", root)), 0600))
	query := fmt.Sprintf("INSERT INTO FILE avro('%s') SELECT * FROM FILE avro('%s') WITH (GROUP='preview') WHERE RECORD_TIMESTAMP < NOW() ", dst, src)
	before := time.Now().UnixMilli()
	var stdout, diagnostics bytes.Buffer
	// When explaining a query that has never run, then describe the current files without pinning them.
	require.Zero(t, Execute(t.Context(), []string{"explain", "-e", query, "-c", cfg, "--json"}, nil, &stdout, &diagnostics), diagnostics.String())
	var report inspect.Report
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &report))
	info := report.Source.File
	require.Equal(t, 1, info.FileCount)
	require.Positive(t, info.SizeBytes)
	require.False(t, *info.CheckpointExists)
	require.Equal(t, "resolved", info.WindowOrigin)
	require.GreaterOrEqual(t, *info.Window.Upper, before)
	require.LessOrEqual(t, *info.Window.Upper, time.Now().UnixMilli())
	require.Contains(t, report.Warnings, "Snapshot excludes 1 unfinished files.")
	require.False(t, report.Destination.File.Exists)
	// When checking status, then an absent checkpoint means unknown progress and creates nothing.
	stdout.Reset()
	diagnostics.Reset()
	require.Equal(t, 5, Execute(t.Context(), []string{"status", "-e", query, "-c", cfg, "--json"}, nil, &stdout, &diagnostics), diagnostics.String())
	var status inspect.StatusReport
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &status))
	require.Equal(t, "unknown", status.Verdict)
	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	require.Len(t, entries, 2, "preview creates neither checkpoint, lock nor destination")
}

func TestArchiveCLIInspectionRejectsChangedSavedInputs(t *testing.T) {
	for _, change := range []string{"query", "missing", "resized"} {
		t.Run(change, func(t *testing.T) {
			// Given a partially replayed group with a fixed selection.
			root := t.TempDir()
			src, dst := filepath.Join(root, "input"), filepath.Join(root, "output")
			record := avro.Record{Source: avro.Source{Cluster: "cluster", Topic: "orders", TopicID: avro.TopicID{1}}, TimestampKind: "unknown"}
			archiveFixture(t, src, 1, []avro.Record{record, record})
			cfg := filepath.Join(root, "worker.toml")
			require.NoError(t, os.WriteFile(cfg, []byte(fmt.Sprintf("[worker]\ncheckpoint_dir=%q\n", root)), 0600))
			query := fmt.Sprintf("INSERT INTO FILE avro('%s') SELECT * FROM FILE avro('%s') WITH (GROUP='saved') OPTIONS (WORKER_STOP_AFTER_RECORDS=1)", dst, src)
			var output bytes.Buffer
			require.Zero(t, Execute(t.Context(), []string{"run", "-e", query, "-c", cfg}, nil, &output, &output), output.String())
			files, err := filepath.Glob(filepath.Join(src, "*.avro"))
			require.NoError(t, err)
			require.Len(t, files, 1)
			message := "selected archive input changed"
			switch change {
			case "query":
				query = fmt.Sprintf("INSERT INTO FILE avro('%s') SELECT * FROM FILE avro('%s') WITH (GROUP='saved') WHERE RECORD_TIMESTAMP < NOW() ", dst, src)
				message = "query identity mismatch"
			case "missing":
				require.NoError(t, os.Remove(files[0]))
				message = "no such file"
			case "resized":
				info, err := os.Stat(files[0])
				require.NoError(t, err)
				require.NoError(t, os.Truncate(files[0], info.Size()+1))
			}
			// When inspecting, then fail rather than reporting the altered selection as valid.
			output.Reset()
			require.NotZero(t, Execute(t.Context(), []string{"explain", "-e", query, "-c", cfg}, nil, &output, &output))
			require.Contains(t, output.String(), message)
		})
	}
}

func TestArchiveCLIOneOffDeletesOnlyCompletedProgress(t *testing.T) {
	for _, capped := range []bool{false, true} {
		t.Run(fmt.Sprintf("capped=%v", capped), func(t *testing.T) {
			// Given a fresh one-off replay with two records.
			root := t.TempDir()
			src, dst := filepath.Join(root, "input"), filepath.Join(root, "output")
			record := avro.Record{Source: avro.Source{Cluster: "cluster", Topic: "orders", TopicID: avro.TopicID{1}}, TimestampKind: "unknown"}
			archiveFixture(t, src, 1, []avro.Record{record, record})
			cfg := filepath.Join(root, "worker.toml")
			require.NoError(t, os.WriteFile(cfg, []byte(fmt.Sprintf("[worker]\ncheckpoint_dir=%q\n", root)), 0600))
			options := ""
			if capped {
				options = " OPTIONS (WORKER_STOP_AFTER_RECORDS=1)"
			}
			query := fmt.Sprintf("INSERT INTO FILE avro('%s') SELECT * FROM FILE avro('%s') WITH (GROUP=ONE_OFF)%s", dst, src, options)
			var output bytes.Buffer
			// When previewing, then no checkpoint path is invented for the generated group.
			require.Zero(t, Execute(t.Context(), []string{"explain", "--json", "-e", query, "-c", cfg}, nil, &output, &output), output.String())
			var preview inspect.Report
			require.NoError(t, json.Unmarshal(output.Bytes(), &preview))
			require.NotNil(t, preview.Source.File)
			require.Empty(t, preview.Source.File.Checkpoint)
			output.Reset()
			// When running, then completed one-off progress is deleted; capped progress remains resumable.
			require.Zero(t, Execute(t.Context(), []string{"run", "-e", query, "-c", cfg}, nil, &output, &output), output.String())
			checkpoints, err := filepath.Glob(filepath.Join(root, "*.json"))
			require.NoError(t, err)
			count := 0
			if capped {
				count = 1
			}
			require.Len(t, checkpoints, count)
			require.Len(t, archiveRecords(t, dst), 2-count)
			locks, err := filepath.Glob(filepath.Join(root, "*.lock"))
			require.NoError(t, err)
			require.Len(t, locks, 1, "stable lock inode remains after JSON deletion")
		})
	}
}
