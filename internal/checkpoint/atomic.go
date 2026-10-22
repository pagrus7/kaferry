package checkpoint

import (
	"os"
	"path/filepath"
)

type AtomicWriter interface{ Replace(string, []byte) error }
type DurableWriter struct{}

func (DurableWriter) Replace(path string, data []byte) error {
	return replaceCheckpoint(nativeReplacement{}, path, data)
}

type replacementFS interface {
	CreateTemp(string, string) (*os.File, error)
	Write(*os.File, []byte) error
	Sync(*os.File) error
	Rename(string, string) error
	Remove(string) error
	SyncDirectory(string) error
}
type nativeReplacement struct{}

func (nativeReplacement) CreateTemp(dir, pattern string) (*os.File, error) {
	return os.CreateTemp(dir, pattern)
}
func (nativeReplacement) Write(file *os.File, data []byte) error {
	_, err := file.Write(data)
	return err
}
func (nativeReplacement) Sync(file *os.File) error     { return file.Sync() }
func (nativeReplacement) Rename(old, new string) error { return os.Rename(old, new) }
func (nativeReplacement) Remove(path string) error     { return os.Remove(path) }
func (nativeReplacement) SyncDirectory(path string) error {
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func replaceCheckpoint(files replacementFS, path string, data []byte) error {
	file, err := files.CreateTemp(filepath.Dir(path), ".checkpoint-*")
	if err != nil {
		return err
	}
	name := file.Name()
	defer files.Remove(name)
	if err = files.Write(file, data); err != nil {
		file.Close()
		return err
	}
	if err = files.Sync(file); err != nil {
		file.Close()
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	if err = files.Rename(name, path); err != nil {
		return err
	}
	return files.SyncDirectory(path)
}
