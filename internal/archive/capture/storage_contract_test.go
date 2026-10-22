package capture

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pagrus7/kaferry/internal/checkpoint"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestArchiveAcceptsASymlinkedParent(t *testing.T) {
	// Given a path through a symlinked mount or macOS /tmp-style parent.
	parent := t.TempDir()
	alias := filepath.Join(t.TempDir(), "mount")
	require.NoError(t, os.Symlink(parent, alias))
	root := filepath.Join(alias, "archive")
	// When capture opens and finishes, then it writes through the resolved parent.
	c, _ := newCapture(t, root, Policy{})
	require.NoError(t, c.Append(testRecord(42)))
	require.NoError(t, c.Finish())
	require.Len(t, completeFiles(t, filepath.Join(parent, "archive")), 1)
}

func TestRecoveryStreamsRootOnceAcrossMultipleNamespaces(t *testing.T) {
	// Given released writer namespaces and more directory entries than one page.
	root := t.TempDir()
	first, _ := savedPrefix(t, root)
	second, _ := savedPrefix(t, root)
	require.NoError(t, first.Close())
	require.NoError(t, second.Close())
	for i := range 600 {
		require.NoError(t, os.WriteFile(filepath.Join(root, fmt.Sprintf("external-%04d", i)), nil, 0600))
	}
	var private []string
	for _, writer := range []*Store{first, second} {
		for i := range 150 {
			path := filepath.Join(root, fmt.Sprintf(".%s.%032x.partial", writer.state.WriterID, i+1))
			require.NoError(t, os.WriteFile(path, nil, 0600))
			private = append(private, path)
		}
	}
	fs := &observedFiles{Filesystem: NativeFiles{}}
	scanClosed := false
	fs.hook = func(op, _ string) error {
		if op == "close-directory" {
			scanClosed = true
		}
		if op == "remove" {
			require.True(t, scanClosed, "finish the scan before deletion can reorder later entries")
		}
		if op == "open-directory" || op == "read-directory-batch" {
			for _, namespace := range []string{first.namespace, second.namespace} {
				lock, err := (NativeFiles{}).TryLock(filepath.Join(namespace, "lock"))
				if lock != nil {
					require.NoError(t, lock.Close())
				}
				require.ErrorIs(t, err, ErrBusy, "both owners stay locked throughout cleanup")
			}
		}
		return nil
	}
	// When startup recovers both owners, then one paged root scan serves both.
	_ = openStore(t, root, fs, checkpoint.DurableWriter{})
	joined := strings.Join(fs.events, "\n")
	require.Equal(t, 1, strings.Count(joined, "open-directory:"))
	require.Greater(t, strings.Count(joined, "read-directory-batch:"), 2)
	require.Len(t, completeFiles(t, root), 2)
	for _, path := range private {
		require.NoFileExists(t, path)
	}
}

func TestPublishDoesNotCheckpointOtherPartitions(t *testing.T) {
	// Given two partitions, with one saved prefix and newer unsaved input on the other.
	root := t.TempDir()
	fs := &observedFiles{Filesystem: NativeFiles{}}
	s := openStore(t, root, fs, checkpoint.DurableWriter{})
	b := testRecord(42)
	b.Source.Partition++
	bFile, err := s.Create(b.Source, b.Offset)
	require.NoError(t, err)
	_, err = s.Append(bFile.ID, b)
	require.NoError(t, err)
	require.NoError(t, s.Checkpoint())
	b.Offset++
	_, err = s.Append(bFile.ID, b)
	require.NoError(t, err)
	a := testRecord(42)
	aFile, err := s.Create(a.Source, a.Offset)
	require.NoError(t, err)
	_, err = s.Append(aFile.ID, a)
	require.NoError(t, err)
	fs.events = nil
	// When only A rotates, then B's saved boundary and pending data remain untouched.
	require.NoError(t, s.Publish(aFile.ID))
	require.EqualValues(t, 1, s.state.Files[bFile.ID].Records)
	bPath := filepath.Base(dataPath(root, s.state.Files[bFile.ID].Identity, true))
	require.NotContains(t, fs.events, "sync:"+bPath)
	require.NotContains(t, fs.events, "write:"+bPath)
}

func TestDefaultBudgetDoesNotFragmentA200PartitionAssignment(t *testing.T) {
	// Given an ordinary 200-partition assignment and the default policy.
	store := &memorySegments{files: make(map[string]Segment)}
	c, err := New(store, &testClock{}, Policy{})
	require.NoError(t, err)
	// When every partition receives input, then no file is prematurely published.
	for i := range 200 {
		r := testRecord(42)
		r.Source.Partition = int32(i)
		require.NoError(t, c.Append(r))
	}
	require.NotContains(t, store.calls, "publish")
	require.Len(t, store.files, 200)
}

func TestStorageCapabilityDiagnostics(t *testing.T) {
	for _, cause := range []error{unix.EINVAL, unix.ENOTSUP, unix.EOPNOTSUPP, unix.ENOSYS, unix.ENOLCK} {
		// Given a missing filesystem capability, when classified, then its name and errno survive.
		err := storageError("exclusive locking", cause)
		require.ErrorIs(t, err, ErrUnsupportedStorage)
		require.ErrorIs(t, err, cause)
		require.ErrorContains(t, err, "exclusive locking")
	}
	err := storageError("exclusive locking", os.ErrPermission)
	require.ErrorIs(t, err, os.ErrPermission)
	require.NotErrorIs(t, err, ErrUnsupportedStorage)
	require.NoError(t, storageError("exclusive locking", nil))
}
