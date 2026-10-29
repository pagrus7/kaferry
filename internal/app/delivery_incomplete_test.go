package app

import (
	"fmt"
	"github.com/pagrus7/kaferry/internal/checkpoint"
	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/pagrus7/kaferry/internal/sql"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
)

func TestCaptureCannotForkAnInitializedDiskAfterLosingItsWriterBinding(t *testing.T) {
	// Given committed capture bytes and a missing local writer binding.
	source, _ := clusters(t, 1)
	seed(t, source, 1, 4)
	root := t.TempDir()
	p, _ := capturePlan(t, source, root, "missing-writer", true)
	p.WorkerStopAfterRecords = 2
	cfg := config.Defaults("")
	require.NoError(t, Run(testContext(t), p, cfg, Options{ProgressTopic: "progress"}))
	paths, err := filepath.Glob(filepath.Join(root, ".kaferry", "eos-leases", "*.owner"))
	require.NoError(t, err)
	require.Len(t, paths, 1)
	require.NoError(t, os.Remove(paths[0]))
	before := captureDiskSnapshot(t, root)
	// When the same job resumes, then it refuses without creating another writer or changing output.
	err = Run(testContext(t), p, cfg, Options{ProgressTopic: "progress"})
	require.ErrorContains(t, err, "writer binding missing")
	require.Equal(t, before, captureDiskSnapshot(t, root))
}
func TestReplayCannotInventAJobAfterLosingBothBindingAndMirror(t *testing.T) {
	// Given a replay checkpoint directory retaining its ownership file after authority identity is lost.
	source, target, directory := t.TempDir(), t.TempDir(), t.TempDir()
	archiveInput(t, source)
	ast, err := sql.NewParser().Parse(fmt.Sprintf("INSERT INTO ARCHIVE '%s' SELECT * FROM ARCHIVE '%s' WITH(GROUP='missing-replay') OPTIONS(WORKER_STOP_AFTER_RECORDS=1)", target, source))
	require.NoError(t, err)
	p, err := plan.NewPlanner(environment{}).Build(ast, nil)
	require.NoError(t, err)
	cfg := config.Defaults("")
	cfg.Worker.CheckpointDir = &directory
	require.NoError(t, Run(testContext(t), p, cfg, Options{}))
	path := checkpoint.Path(directory, p.Group)
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.Remove(path+".eos"))
	before := captureDiskSnapshot(t, target)
	// When the job resumes, then it fails before touching output rather than guessing a fresh replay identity.
	err = Run(testContext(t), p, cfg, Options{})
	require.ErrorContains(t, err, "replay binding missing")
	require.Equal(t, before, captureDiskSnapshot(t, target))
}

func TestReplayAndLocalCaptureRejectTamperedStateBeforeOutput(t *testing.T) {
	for _, test := range []struct {
		name          string
		binding       bool
		inspect       bool
		alterBinding  func(*replayBinding)
		alterEnvelope func(*archiveEnvelope)
	}{
		{name: "binding malformed", binding: true, inspect: true},
		{name: "binding unsupported version", binding: true, inspect: true, alterBinding: func(b *replayBinding) { b.Version++ }},
		{name: "binding format", binding: true, inspect: true, alterBinding: func(b *replayBinding) { b.Format = "unknown" }},
		{name: "binding missing job", binding: true, inspect: true, alterBinding: func(b *replayBinding) { b.Identity.ReplayJob = "" }},
		{name: "binding query", binding: true, inspect: true, alterBinding: func(b *replayBinding) { b.Identity.Fingerprint = "another query" }},
		{name: "binding destination", binding: true, inspect: true, alterBinding: func(b *replayBinding) { b.Identity.DestinationTopic = [16]byte{9} }},
		{name: "envelope unsupported version", inspect: true, alterEnvelope: func(e *archiveEnvelope) { e.Version++ }},
		{name: "envelope identity", inspect: true, alterEnvelope: func(e *archiveEnvelope) { e.Identity.Fingerprint = "another query" }},
		{name: "envelope partition", inspect: true, alterEnvelope: func(e *archiveEnvelope) { e.Partition++ }},
		{name: "capture version", alterEnvelope: func(e *archiveEnvelope) { e.Capture.Version++ }},
		{name: "capture writer", alterEnvelope: func(e *archiveEnvelope) { e.Capture.WriterID = "another writer" }},
		{name: "capture incomplete files", alterEnvelope: func(e *archiveEnvelope) { e.Capture.Files = nil }},
		{name: "frame partition", inspect: true, alterEnvelope: func(e *archiveEnvelope) { e.Frame.Cursor.Partition++ }},
		{name: "frame identity", inspect: true, alterEnvelope: func(e *archiveEnvelope) { e.Frame.Identity.Fingerprint = "another query" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Given an initialized replay and local capture checkpoint with one acknowledged occurrence.
			source, target, directory := t.TempDir(), t.TempDir(), t.TempDir()
			expected := archiveInput(t, source)
			ast, err := sql.NewParser().Parse(fmt.Sprintf("INSERT INTO ARCHIVE '%s' SELECT * FROM ARCHIVE '%s' WITH(GROUP='tampered-state') OPTIONS(WORKER_STOP_AFTER_RECORDS=1)", target, source))
			require.NoError(t, err)
			p, err := plan.NewPlanner(environment{}).Build(ast, nil)
			require.NoError(t, err)
			cfg := config.Defaults("")
			cfg.Worker.CheckpointDir = &directory
			require.NoError(t, Run(testContext(t), p, cfg, Options{}))
			path := checkpoint.Path(directory, p.Group) + ".eos"
			if !test.binding {
				paths, err := filepath.Glob(filepath.Join(target, ".kaferry", "writers", "*", "state.json"))
				require.NoError(t, err)
				require.Len(t, paths, 1)
				path = paths[0]
			}
			original, err := os.ReadFile(path)
			require.NoError(t, err)
			changed := []byte("malformed checkpoint")
			if test.alterBinding != nil {
				b, err := checkpoint.Decode[replayBinding](original, 32<<10)
				require.NoError(t, err)
				test.alterBinding(&b)
				changed, err = checkpoint.Encode(b, 32<<10)
				require.NoError(t, err)
			} else if test.alterEnvelope != nil {
				e, err := checkpoint.Decode[archiveEnvelope](original, 96<<20)
				require.NoError(t, err)
				test.alterEnvelope(&e)
				changed, err = checkpoint.Encode(e, 96<<20)
				require.NoError(t, err)
			}
			require.NoError(t, os.WriteFile(path, changed, 0600))
			beforeOutput, beforeState := captureDiskSnapshot(t, target), captureDiskSnapshot(t, directory)
			// When execution or authority inspection sees unusable saved state, then it refuses before changing output or authority.
			require.Error(t, Run(testContext(t), p, cfg, Options{}))
			if test.inspect {
				_, err = (Inspection{}).Explain(testContext(t), p, cfg.Worker, core.NoopStopObserver{})
				require.Error(t, err)
			}
			require.Equal(t, beforeOutput, captureDiskSnapshot(t, target))
			require.Equal(t, beforeState, captureDiskSnapshot(t, directory))
			// Then restoring the state resumes every physical occurrence exactly once.
			require.NoError(t, os.WriteFile(path, original, 0600))
			p.WorkerStopAfterRecords = 0
			require.NoError(t, Run(testContext(t), p, cfg, Options{}))
			require.Equal(t, expected, archiveValues(t, target))
		})
	}
}
