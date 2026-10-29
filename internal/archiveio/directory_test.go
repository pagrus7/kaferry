package archiveio

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExistingDirectoryStillRequiresDurableParent(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses the parent read-permission failure")
	}
	// Given creation is allowed but opening the parent for sync is denied.
	parent := t.TempDir()
	path := filepath.Join(parent, "archive")
	require.NoError(t, os.Chmod(parent, 0300))
	t.Cleanup(func() { require.NoError(t, os.Chmod(parent, 0700)) })
	require.ErrorIs(t, mkdirDurable(path), os.ErrPermission)
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.True(t, info.IsDir(), "creation must have happened before the failed parent sync")
	// When retried against that existing directory, then the same sync failure still stops preparation.
	require.ErrorIs(t, mkdirDurable(path), os.ErrPermission)
	// When the parent can be synced, then preparation succeeds.
	require.NoError(t, os.Chmod(parent, 0700))
	require.NoError(t, mkdirDurable(path))
}
