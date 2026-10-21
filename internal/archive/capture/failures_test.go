package capture

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pagrus7/kaferry/internal/archive/avro"
	"github.com/pagrus7/kaferry/internal/file"
	"github.com/stretchr/testify/require"
)

var errDisk = errors.New("injected storage failure")

type observedFiles struct {
	Filesystem
	events    []string
	hook      func(string, string) error
	readError error
}

func (f *observedFiles) call(op, path string) error {
	f.events = append(f.events, op+":"+filepath.Base(path))
	if f.hook != nil {
		return f.hook(op, path)
	}
	return nil
}
func (f *observedFiles) OpenFile(path string, flags int, mode os.FileMode) (DataFile, error) {
	if err := f.call("open", path); err != nil {
		return nil, err
	}
	handle, err := f.Filesystem.OpenFile(path, flags, mode)
	if err != nil {
		return nil, err
	}
	return &observedFile{DataFile: handle, owner: f, path: path}, nil
}
func (f *observedFiles) SyncDirectory(path string) error {
	if err := f.call("sync-directory", path); err != nil {
		return err
	}
	return f.Filesystem.SyncDirectory(path)
}
func (f *observedFiles) RenameExclusive(old, new string) error {
	if err := f.call("rename", new); err != nil {
		return err
	}
	return f.Filesystem.RenameExclusive(old, new)
}
func (f *observedFiles) Remove(path string) error {
	if err := f.call("remove", path); err != nil {
		return err
	}
	return f.Filesystem.Remove(path)
}
func (f *observedFiles) TryLock(path string) (io.Closer, error) {
	if err := f.call("lock", path); err != nil {
		return nil, err
	}
	return f.Filesystem.TryLock(path)
}
func (f *observedFiles) Mkdir(path string, mode os.FileMode) error {
	if err := f.call("mkdir", path); err != nil {
		return err
	}
	return f.Filesystem.Mkdir(path, mode)
}
func (f *observedFiles) Lstat(path string) (os.FileInfo, error) {
	if err := f.call("lstat", path); err != nil {
		return nil, err
	}
	return f.Filesystem.Lstat(path)
}
func (f *observedFiles) ReadDir(path string) ([]os.DirEntry, error) {
	if err := f.call("read-directory", path); err != nil {
		return nil, err
	}
	return f.Filesystem.ReadDir(path)
}

type observedFile struct {
	DataFile
	owner *observedFiles
	path  string
}

func (f *observedFile) Write(data []byte) (int, error) {
	if err := f.owner.call("write", f.path); err != nil {
		return 0, err
	}
	return f.DataFile.Write(data)
}
func (f *observedFile) Sync() error {
	if err := f.owner.call("sync", f.path); err != nil {
		return err
	}
	return f.DataFile.Sync()
}
func (f *observedFile) Truncate(size int64) error {
	if err := f.owner.call("truncate", f.path); err != nil {
		return err
	}
	return f.DataFile.Truncate(size)
}
func (f *observedFile) Close() error {
	return errors.Join(f.owner.call("close", f.path), f.DataFile.Close())
}
func (f *observedFile) Stat() (os.FileInfo, error) {
	if err := f.owner.call("stat", f.path); err != nil {
		return nil, err
	}
	return f.DataFile.Stat()
}
func (f *observedFile) ReadAt(data []byte, position int64) (int, error) {
	if f.owner.readError != nil {
		return 0, f.owner.readError
	}
	return f.DataFile.ReadAt(data, position)
}

type observedWriter struct {
	AtomicWriter
	files *observedFiles
	after func() error
}

func (w *observedWriter) Replace(path string, data []byte) error {
	if err := w.files.call("save-state", path); err != nil {
		return err
	}
	if err := w.AtomicWriter.Replace(path, data); err != nil {
		return err
	}
	if w.after != nil {
		return w.after()
	}
	return nil
}

func TestCheckpointDurabilityOrderAndQuietFlush(t *testing.T) {
	// Given one changed file and injected operation tracing around real storage.
	root := filepath.Join(t.TempDir(), "archive")
	fs := &observedFiles{Filesystem: NativeFiles{}}
	w := &observedWriter{AtomicWriter: file.DurableWriter{}, files: fs}
	s := openStore(t, root, fs, w)
	r := testRecord(42)
	segment, err := s.Create(r.Source, r.Offset)
	require.NoError(t, err)
	_, err = s.Append(segment.ID, r)
	require.NoError(t, err)
	fs.events = nil
	// When a checkpoint succeeds, then bytes precede data sync, directory sync and state save.
	require.NoError(t, s.Checkpoint())
	joined := strings.Join(fs.events, "\n")
	write := strings.Index(joined, "write:")
	dataSync := strings.Index(joined, "sync:")
	directorySync := strings.Index(joined, "sync-directory:archive")
	state := strings.Index(joined, "save-state:state.json")
	require.NotEqual(t, -1, write)
	require.Less(t, write, dataSync)
	require.Less(t, dataSync, directorySync)
	require.Less(t, directorySync, state)
	fs.events = nil
	require.NoError(t, s.Checkpoint())
	require.Empty(t, fs.events, "quiet batches do not repeat sync or state writes")
}

func TestCheckpointFailuresRemainTerminal(t *testing.T) {
	for _, fail := range []string{"write", "sync", "sync-directory", "save-state"} {
		t.Run(fail, func(t *testing.T) {
			// Given a record accepted into a private file before a durable batch.
			root := t.TempDir()
			fs := &observedFiles{Filesystem: NativeFiles{}}
			w := &observedWriter{AtomicWriter: file.DurableWriter{}, files: fs}
			s := openStore(t, root, fs, w)
			r := testRecord(42)
			part, err := s.Create(r.Source, r.Offset)
			require.NoError(t, err)
			_, err = s.Append(part.ID, r)
			require.NoError(t, err)
			fs.hook = func(op, path string) error {
				if op == fail {
					return errDisk
				}
				return nil
			}
			// When any durability step fails, then retry cannot turn that batch into success.
			err = s.Checkpoint()
			require.ErrorIs(t, err, errDisk)
			fs.hook, fs.events = nil, nil
			require.ErrorIs(t, s.Checkpoint(), err)
			_, again := s.Append(part.ID, testRecord(43))
			require.ErrorIs(t, again, err)
			require.ErrorIs(t, s.Publish(part.ID), err)
			require.Empty(t, fs.events, "a failed writer performs no more I/O")
			require.NoError(t, s.Close())
			// Then recovery discards this never-acknowledged file, including fully written blocks.
			recovered := openStore(t, root, NativeFiles{}, file.DurableWriter{})
			require.Empty(t, recovered.state.Files)
			require.Empty(t, completeFiles(t, root))
		})
	}
}

func TestPublicationFailureKeepsRecoverableState(t *testing.T) {
	for _, failure := range []string{"close", "rename", "sync-directory", "save-state"} {
		t.Run(failure, func(t *testing.T) {
			// Given an already durable private prefix.
			root := t.TempDir()
			fs := &observedFiles{Filesystem: NativeFiles{}}
			s := openStore(t, root, fs, &observedWriter{AtomicWriter: file.DurableWriter{}, files: fs})
			r := testRecord(42)
			part, err := s.Create(r.Source, r.Offset)
			require.NoError(t, err)
			_, err = s.Append(part.ID, r)
			require.NoError(t, err)
			require.NoError(t, s.Checkpoint())
			fs.hook = func(op, path string) error {
				if op == failure {
					return errDisk
				}
				return nil
			}
			// When publication stops at any step, then no retry or further acknowledgement succeeds.
			err = s.Publish(part.ID)
			require.ErrorIs(t, err, errDisk)
			fs.hook = nil
			require.ErrorIs(t, s.Checkpoint(), err)
			require.NoError(t, s.Close())
			// Then a new owner recovers exactly the protected record from either filename.
			_ = openStore(t, root, NativeFiles{}, file.DurableWriter{})
			paths := completeFiles(t, root)
			require.Len(t, paths, 1)
			require.Equal(t, []avro.Record{r}, readFile(t, paths[0]))
		})
	}
}

func runLifecycle(root string, fs *observedFiles, codec *avro.Codec) (acknowledged bool, result error) {
	w := &observedWriter{AtomicWriter: file.DurableWriter{}, files: fs}
	s, err := Open(root, codec, fs, NewJournal(fs, w))
	if err != nil {
		return false, err
	}
	defer func() { result = errors.Join(result, s.Close()) }()
	r := testRecord(42)
	part, err := s.Create(r.Source, r.Offset)
	if err != nil {
		return false, err
	}
	if _, err = s.Append(part.ID, r); err != nil {
		return false, err
	}
	if err = s.Checkpoint(); err != nil {
		return false, err
	}
	return true, s.Publish(part.ID)
}

func TestEveryCaptureIOCutPreservesAcknowledgedRecords(t *testing.T) {
	// Given a complete reference lifecycle identifying each storage operation.
	baseline := &observedFiles{Filesystem: NativeFiles{}}
	_, err := runLifecycle(filepath.Join(t.TempDir(), "archive"), baseline, testCodec(t))
	require.NoError(t, err)
	for index, event := range baseline.events {
		t.Run(fmt.Sprintf("%02d_%s", index, event[:strings.IndexByte(event, ':')]), func(t *testing.T) {
			// When that operation fails once, then the caller receives an error instead of an acknowledgement.
			root := filepath.Join(t.TempDir(), "archive")
			fs := &observedFiles{Filesystem: NativeFiles{}}
			calls := 0
			fs.hook = func(string, string) error {
				calls++
				if calls == index+1 {
					return errDisk
				}
				return nil
			}
			acknowledged, err := runLifecycle(root, fs, testCodec(t))
			require.ErrorIs(t, err, errDisk)
			// Then healthy restart preserves every record acknowledged before the cut.
			_ = openStore(t, root, NativeFiles{}, file.DurableWriter{})
			paths := completeFiles(t, root)
			if acknowledged {
				require.Len(t, paths, 1)
				require.Equal(t, []avro.Record{testRecord(42)}, readFile(t, paths[0]))
			} else {
				require.Empty(t, paths)
			}
		})
	}
}

func TestEveryRecoveryIOCutCanBeRetriedByANewOwner(t *testing.T) {
	baselineRoot := t.TempDir()
	s, _ := savedPrefix(t, baselineRoot)
	require.NoError(t, s.Close())
	baseline := &observedFiles{Filesystem: NativeFiles{}}
	recovered := openStore(t, baselineRoot, baseline, &observedWriter{AtomicWriter: file.DurableWriter{}, files: baseline})
	for index, event := range baseline.events {
		t.Run(fmt.Sprintf("%02d_%s", index, event[:strings.IndexByte(event, ':')]), func(t *testing.T) {
			// Given an acknowledged private prefix and one failed recovery operation.
			root := t.TempDir()
			s, _ := savedPrefix(t, root)
			require.NoError(t, s.Close())
			fs := &observedFiles{Filesystem: NativeFiles{}}
			calls := 0
			fs.hook = func(string, string) error {
				calls++
				if calls == index+1 {
					return errDisk
				}
				return nil
			}
			// When recovery fails, then a later owner still restores the protected data.
			_, err := Open(root, testCodec(t), fs, NewJournal(fs, &observedWriter{AtomicWriter: file.DurableWriter{}, files: fs}))
			require.ErrorIs(t, err, errDisk)
			_ = openStore(t, root, NativeFiles{}, file.DurableWriter{})
			paths := completeFiles(t, root)
			require.Len(t, paths, 1)
			require.Equal(t, []avro.Record{testRecord(42)}, readFile(t, paths[0]))
		})
	}
	require.NoError(t, recovered.Close())
}
