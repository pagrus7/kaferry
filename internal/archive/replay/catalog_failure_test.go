package replay

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

type faultyFiles struct {
	Filesystem
	fault  string
	target string
	start  int64
	active int
}

func TestInspectionDoesNotReadPayloadsButExecutionHashesThem(t *testing.T) {
	// Given a storage port that permits headers but fails every payload read.
	root := t.TempDir()
	file := writeFile(t, root, 1, records(42), 0)
	files := &faultyFiles{Filesystem: NativeFiles{}, fault: "hash", start: file.Start}
	catalog := NewCatalog(root, codec(t), files)
	// When previewing a query, then headers and lengths are readable without a full content scan.
	selection, err := catalog.Preview().Select(nil)
	require.NoError(t, err)
	require.Len(t, selection.Files, 1)
	require.NoError(t, catalog.Preview().Validate([]File{file}))
	require.Zero(t, files.active)
	// When executing, then the same unreadable payload must fail before the selection is pinned.
	_, err = catalog.Select(nil)
	require.ErrorIs(t, err, errDisk)
	require.ErrorIs(t, catalog.Validate([]File{file}), errDisk)
	require.Zero(t, files.active)
}

type faultyInput struct {
	InputFile
	owner *faultyFiles
	name  string
}
type faultyDirectory struct {
	Directory
	owner *faultyFiles
}

func (f *faultyFiles) Open(path string) (InputFile, error) {
	if f.fault == "open" {
		return nil, errDisk
	}
	input, err := f.Filesystem.Open(path)
	if err != nil {
		return nil, err
	}
	f.active++
	return &faultyInput{InputFile: input, owner: f, name: filepath.Base(path)}, nil
}
func (f *faultyFiles) OpenDirectory(path string) (Directory, error) {
	dir, err := f.Filesystem.OpenDirectory(path)
	if err != nil {
		return nil, err
	}
	f.active++
	return &faultyDirectory{Directory: dir, owner: f}, nil
}
func (f *faultyInput) Stat() (os.FileInfo, error) {
	if f.owner.fault == "stat" {
		return nil, errDisk
	}
	return f.InputFile.Stat()
}
func (f *faultyInput) ReadAt(data []byte, offset int64) (int, error) {
	if f.owner.fault == "read" {
		return 0, errDisk
	}
	if f.owner.fault == "hash" && (f.owner.target == "" || f.owner.target == f.name) && offset+int64(len(data)) > f.owner.start {
		allowed := max(int64(0), f.owner.start-offset)
		n, _ := f.InputFile.ReadAt(data[:int(allowed)], offset)
		return n, errDisk
	}
	return f.InputFile.ReadAt(data, offset)
}
func (f *faultyInput) Close() error {
	f.owner.active--
	err := f.InputFile.Close()
	if f.owner.fault == "file close" {
		err = errors.Join(err, errDisk)
	}
	return err
}
func (d *faultyDirectory) ReadDir(n int) ([]os.DirEntry, error) {
	if d.owner.fault == "directory read" {
		return nil, errDisk
	}
	return d.Directory.ReadDir(n)
}
func (d *faultyDirectory) Close() error {
	d.owner.active--
	err := d.Directory.Close()
	if d.owner.fault == "directory close" {
		err = errors.Join(err, errDisk)
	}
	return err
}

func TestSelectionStorageFailuresNeverPinAPartialDirectory(t *testing.T) {
	for _, fault := range []string{"open", "stat", "read", "hash", "file close", "directory read", "directory close"} {
		t.Run(fault, func(t *testing.T) {
			// Given a real completed file and failure at a directory/header/hash/close boundary.
			root := t.TempDir()
			f := writeFile(t, root, 1, records(42), 0)
			fs := &faultyFiles{Filesystem: NativeFiles{}, fault: fault, start: f.Start}
			store, blocks := &memoryCheckpoints{}, &memoryBlocks{}
			// When starting replay, then no incomplete selection is saved or input returned.
			_, err := NewSession(query(), clock{}, store, NewCatalog(root, codec(t), fs), blocks, 10)
			require.ErrorIs(t, err, errDisk)
			require.Empty(t, store.writes)
			require.Zero(t, blocks.reads)
			require.Zero(t, fs.active, "every directory and data handle is closed")
		})
	}
}

func TestPreviewValidationClosesFilesOnFailure(t *testing.T) {
	for _, fault := range []string{"stat", "file close"} {
		t.Run(fault, func(t *testing.T) {
			// Given a saved descriptor and a failed header check or close.
			root := t.TempDir()
			file := writeFile(t, root, 1, records(42), 0)
			files := &faultyFiles{Filesystem: NativeFiles{}, fault: fault}
			inputs := NewCatalog(root, codec(t), files).Preview()

			// When validating the saved selection for inspection.
			err := inputs.Validate([]File{file})

			// Then the failure is returned and no handle remains open.
			require.ErrorIs(t, err, errDisk)
			require.Zero(t, files.active)
		})
	}
}
