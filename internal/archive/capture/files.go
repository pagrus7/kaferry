package capture

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

type DataFile interface {
	io.ReaderAt
	io.Writer
	io.Closer
	Stat() (os.FileInfo, error)
	Sync() error
	Truncate(int64) error
}

// Filesystem supplies the file operations whose ordering protects saved records.
type Filesystem interface {
	ResolveRoot(string) (string, error)
	OpenFile(string, int, os.FileMode) (DataFile, error)
	ReadDir(string) ([]os.DirEntry, error)
	OpenDirectory(string) (Directory, error)
	Lstat(string) (os.FileInfo, error)
	Mkdir(string, os.FileMode) error
	RenameExclusive(string, string) error
	Remove(string) error
	SyncDirectory(string) error
	TryLock(string) (io.Closer, error)
}

var ErrBusy = errors.New("archive writer is locked")
var ErrUnsupportedStorage = errors.New("archive storage cannot provide a required capability")

type Directory interface {
	io.Closer
	ReadDir(int) ([]os.DirEntry, error)
}

type NativeFiles struct{}

func (NativeFiles) ResolveRoot(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		return "", err
	}
	return filepath.Join(parent, filepath.Base(absolute)), nil
}

func (NativeFiles) OpenFile(path string, flags int, mode os.FileMode) (DataFile, error) {
	return os.OpenFile(path, flags|unix.O_NOFOLLOW|unix.O_NONBLOCK, mode)
}
func (NativeFiles) ReadDir(path string) ([]os.DirEntry, error) { return os.ReadDir(path) }
func (NativeFiles) Lstat(path string) (os.FileInfo, error)     { return os.Lstat(path) }
func (NativeFiles) Mkdir(path string, mode os.FileMode) error  { return os.Mkdir(path, mode) }
func (NativeFiles) Remove(path string) error                   { return os.Remove(path) }
func (NativeFiles) RenameExclusive(old, new string) error {
	return storageError("atomic rename without replacement", renameExclusive(old, new))
}
func (NativeFiles) OpenDirectory(path string) (Directory, error) {
	return os.OpenFile(path, os.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW, 0)
}
func (NativeFiles) SyncDirectory(path string) error {
	f, err := os.OpenFile(path, os.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}
func (NativeFiles) TryLock(path string) (io.Closer, error) {
	f, err := os.OpenFile(path, os.O_RDWR|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err == nil && !info.Mode().IsRegular() {
		err = errors.New("archive lock must be a regular file")
	}
	if err == nil {
		err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	}
	if err != nil {
		_ = f.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, ErrBusy
		}
		return nil, storageError("exclusive writer lock", err)
	}
	return f, nil
}

func storageError(capability string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.ENOLCK) {
		return fmt.Errorf("%w: %s: %w", ErrUnsupportedStorage, capability, err)
	}
	return fmt.Errorf("%s: %w", capability, err)
}
