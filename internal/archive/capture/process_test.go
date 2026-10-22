package capture

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/archive/avro"
	"github.com/pagrus7/kaferry/internal/checkpoint"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestArchiveWriterProcess(t *testing.T) {
	if os.Getenv("KAFERRY_ARCHIVE_PROCESS") != "1" {
		return
	}
	s, boundary := savedPrefix(t, os.Getenv("KAFERRY_ARCHIVE_PATH"))
	late := testRecord(43)
	late.Value = bytes.Repeat([]byte{1}, 300<<10)
	_, err := s.Append(boundary.Identity.FileID, late)
	require.NoError(t, err)
	fmt.Println("READY")
	_, _ = bufio.NewReader(os.Stdin).ReadByte()
}

func TestKilledWorkerReleasesLockAndRetainsOnlySavedPrefix(t *testing.T) {
	// Given a live worker with a durable prefix and an unacknowledged complete tail block.
	root := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestArchiveWriterProcess$", "-test.timeout=15s")
	command.Env = append(os.Environ(), "KAFERRY_ARCHIVE_PROCESS=1", "KAFERRY_ARCHIVE_PATH="+root)
	command.Stderr = os.Stderr
	input, err := command.StdinPipe()
	require.NoError(t, err)
	output, err := command.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, command.Start())
	defer func() { _ = input.Close(); _ = command.Process.Kill(); _ = command.Wait() }()
	ready, err := bufio.NewReader(output).ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "READY\n", ready)
	other := openStore(t, root, NativeFiles{}, checkpoint.DurableWriter{})
	require.Empty(t, completeFiles(t, root), "another process skips the live owner's namespace")
	// When the worker is killed, then a new process owner recovers its acknowledged prefix.
	require.NoError(t, command.Process.Kill())
	require.Error(t, command.Wait())
	require.NoError(t, other.Close())
	_ = openStore(t, root, NativeFiles{}, checkpoint.DurableWriter{})
	paths := completeFiles(t, root)
	require.Len(t, paths, 1)
	require.Equal(t, []avro.Record{testRecord(42)}, readFile(t, paths[0]))
}

func TestNamespaceIsLockedAndInitializedBeforePublication(t *testing.T) {
	// Given an initialization observer at the exact directory-publication boundary.
	root := t.TempDir()
	fs := &observedFiles{Filesystem: NativeFiles{}}
	checked := false
	fs.hook = func(op, path string) error {
		if op == "rename" && validID(filepath.Base(path)) {
			id := filepath.Base(path)
			temporary := filepath.Join(filepath.Dir(path), ".init-"+id)
			require.FileExists(t, filepath.Join(temporary, "state.json"))
			lock, err := (NativeFiles{}).TryLock(filepath.Join(temporary, "lock"))
			require.ErrorIs(t, err, ErrBusy, "the stable lock is already held before publication")
			if lock != nil {
				require.NoError(t, lock.Close())
			}
			checked = true
		}
		return nil
	}
	// When the namespace becomes visible, then competitors cannot acquire its lock.
	_ = openStore(t, root, fs, checkpoint.DurableWriter{})
	require.True(t, checked)
}

func TestPublishedLockIsNeverRecreatedOrFollowed(t *testing.T) {
	for _, kind := range []string{"missing", "symlink", "fifo"} {
		t.Run(kind, func(t *testing.T) {
			// Given a published namespace whose stable lock is damaged or replaced.
			root := t.TempDir()
			s := openStore(t, root, NativeFiles{}, checkpoint.DurableWriter{})
			require.NoError(t, s.Close())
			lock := filepath.Join(s.namespace, "lock")
			require.NoError(t, os.Remove(lock))
			switch kind {
			case "symlink":
				target := filepath.Join(t.TempDir(), "target")
				require.NoError(t, os.WriteFile(target, nil, 0600))
				require.NoError(t, os.Symlink(target, lock))
			case "fifo":
				require.NoError(t, unix.Mkfifo(lock, 0600))
			}
			// When opening the archive, then startup fails rather than creating a competing lock.
			_, err := Open(root, testCodec(t), NativeFiles{}, NewJournal(NativeFiles{}, checkpoint.DurableWriter{}))
			require.Error(t, err)
			if kind == "missing" {
				require.NoFileExists(t, lock)
			}
		})
	}
}

func TestRequiredStateNeverFollowsASymlinkOrBlocksOnAFIFO(t *testing.T) {
	for _, kind := range []string{"symlink", "fifo"} {
		t.Run(kind, func(t *testing.T) {
			// Given an invalid required-state file type.
			path := filepath.Join(t.TempDir(), "state.json")
			if kind == "symlink" {
				target := filepath.Join(t.TempDir(), "target")
				require.NoError(t, os.WriteFile(target, stateWire(emptyPayload), 0600))
				require.NoError(t, os.Symlink(target, path))
			} else {
				require.NoError(t, unix.Mkfifo(path, 0600))
			}
			// When loaded, then it is rejected before reading or waiting for a writer.
			_, err := NewJournal(NativeFiles{}, checkpoint.DurableWriter{}).Load(path)
			require.Error(t, err)
		})
	}
}

func TestOpenRejectsInvalidArchiveDirectories(t *testing.T) {
	for _, kind := range []string{"missing parent", "regular root", "symlink root", "non-directory namespace"} {
		t.Run(kind, func(t *testing.T) {
			// Given an unavailable parent or a path with the wrong filesystem type.
			root := filepath.Join(t.TempDir(), "archive")
			switch kind {
			case "missing parent":
				root = filepath.Join(root, "missing", "archive")
			case "regular root":
				require.NoError(t, os.WriteFile(root, nil, 0600))
			case "symlink root":
				require.NoError(t, os.Symlink(t.TempDir(), root))
			case "non-directory namespace":
				writerDir := filepath.Join(root, ".kaferry", "writers")
				require.NoError(t, os.MkdirAll(writerDir, 0700))
				require.NoError(t, os.WriteFile(filepath.Join(writerDir, strings.Repeat("a", 32)), nil, 0600))
			}
			// When opening, then it fails without claiming usable archive storage.
			_, err := Open(root, testCodec(t), NativeFiles{}, NewJournal(NativeFiles{}, checkpoint.DurableWriter{}))
			require.Error(t, err)
		})
	}
}
