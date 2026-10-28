package app

import (
	"fmt"
	"github.com/pagrus7/kaferry/internal/checkpoint"
	"github.com/pagrus7/kaferry/internal/config"
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
