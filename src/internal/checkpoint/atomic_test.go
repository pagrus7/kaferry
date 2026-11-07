package checkpoint

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

type checkpointFaultFS struct {
	nativeReplacement
	stage   string
	failure error
}

func (f checkpointFaultFS) CreateTemp(dir, pattern string) (*os.File, error) {
	if f.stage == "create" {
		return nil, f.failure
	}
	return f.nativeReplacement.CreateTemp(dir, pattern)
}
func (f checkpointFaultFS) Write(file *os.File, data []byte) error {
	if f.stage == "write" {
		_, _ = file.Write(data[:len(data)/2])
		return f.failure
	}
	return f.nativeReplacement.Write(file, data)
}
func (f checkpointFaultFS) Sync(file *os.File) error {
	if f.stage == "sync" {
		return f.failure
	}
	return f.nativeReplacement.Sync(file)
}
func (f checkpointFaultFS) Rename(old, new string) error {
	if f.stage == "rename" {
		return f.failure
	}
	return f.nativeReplacement.Rename(old, new)
}
func (f checkpointFaultFS) SyncDirectory(path string) error {
	if f.stage == "directory sync" {
		return f.failure
	}
	return f.nativeReplacement.SyncDirectory(path)
}

func TestCheckpointReplacementFailuresPreserveAtomicPublication(t *testing.T) {
	for _, stage := range []string{"create", "write", "sync", "rename", "directory sync"} {
		t.Run(stage, func(t *testing.T) {
			// Given an existing checkpoint and a failure at one replacement boundary.
			path := filepath.Join(t.TempDir(), "checkpoint.json")
			before, after := []byte("old checkpoint"), []byte("new checkpoint")
			require.NoError(t, os.WriteFile(path, before, 0600))
			failure := errors.New("checkpoint I/O failed")

			// When attempting the atomic replacement.
			err := replaceCheckpoint(checkpointFaultFS{stage: stage, failure: failure}, path, after)

			// Then failures before rename retain the old bytes; failed directory sync
			// leaves the complete new file visible but does not certify its durability.
			want := before
			if stage == "directory sync" {
				want = after
			}
			got, readErr := os.ReadFile(path)
			temporary, globErr := filepath.Glob(filepath.Join(filepath.Dir(path), ".checkpoint-*"))
			require.ErrorIs(t, err, failure)
			require.NoError(t, readErr)
			require.Equal(t, want, got, "replacement retains complete old or new bytes")
			require.NoError(t, globErr)
			require.Empty(t, temporary, "failed replacement cleans up temporary files")
		})
	}
}
