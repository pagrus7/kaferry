package replay

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/pagrus7/kaferry/internal/checkpoint"
	"golang.org/x/sys/unix"
)

type Checkpoints interface {
	Load() (*State, error)
	Save(State) error
	Close() error
}
type AtomicWriter interface{ Replace(string, []byte) error }
type GroupFiles interface {
	Read(string, int64) ([]byte, error)
	Acquire(string) (io.Closer, error)
}

var ErrGroupBusy = errors.New("replay group is already running")

type NativeGroupFiles struct{}

func (NativeGroupFiles) Read(path string, limit int64) ([]byte, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	return checkpoint.Read(f, limit)
}

func (NativeGroupFiles) Acquire(path string) (lock io.Closer, result error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0600)
	if err != nil {
		return nil, err
	}
	defer func() {
		if result != nil {
			result = errors.Join(result, f.Close())
		}
	}()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("replay group lock must be a regular file")
	}
	if err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w: %w", ErrGroupBusy, err)
		}
		return nil, fmt.Errorf("exclusive replay group lock: %w", err)
	}
	// Keep the lock name bound to its inode if shared storage restarts while workers live.
	if err = f.Sync(); err != nil {
		return nil, err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	if err = errors.Join(dir.Sync(), dir.Close()); err != nil {
		return nil, err
	}
	return f, nil
}

// Journal owns a stable group lock outside the input archive.
type Journal struct {
	path, group string
	files       GroupFiles
	writer      AtomicWriter
	lock        io.Closer
	closed      bool
}

func NewJournal(directory, group string, files GroupFiles, writer AtomicWriter) (*Journal, error) {
	if !validGroup(group) {
		return nil, errors.New("replay requires a group")
	}
	path := checkpoint.Path(directory, group)
	lock, err := files.Acquire(path + ".lock")
	if err != nil {
		return nil, err
	}
	return &Journal{path: path, group: group, files: files, writer: writer, lock: lock}, nil
}

func (j *Journal) Load() (saved *State, result error) {
	defer func() {
		if result != nil {
			result = fmt.Errorf("replay checkpoint %s: %w", j.path, result)
		}
	}()
	if j.closed {
		return nil, errors.New("replay checkpoint is closed")
	}
	return readCheckpoint(j.path, j.group, j.files)
}

// ReadCheckpoint reads saved progress without acquiring a lock or changing files.
func ReadCheckpoint(directory, group string, files GroupFiles) (*State, error) {
	path := checkpoint.Path(directory, group)
	saved, err := readCheckpoint(path, group, files)
	if err != nil {
		return nil, fmt.Errorf("replay checkpoint %s: %w", path, err)
	}
	return saved, nil
}
func readCheckpoint(path, group string, files GroupFiles) (*State, error) {
	data, err := files.Read(path, maxCheckpointBytes)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	state, err := checkpoint.Decode[State](data, maxCheckpointBytes)
	if err != nil {
		return nil, err
	}
	if err = state.validate(); err != nil {
		return nil, err
	}
	if state.Group != group {
		return nil, errors.New("checkpoint belongs to a different replay group")
	}
	return &state, nil
}

func (j *Journal) Save(state State) (result error) {
	defer func() {
		if result != nil {
			result = fmt.Errorf("replay checkpoint %s: %w", j.path, result)
		}
	}()
	if j.closed {
		return errors.New("replay checkpoint is closed")
	}
	if err := state.validate(); err != nil {
		return err
	}
	if state.Group != j.group {
		return errors.New("checkpoint belongs to a different replay group")
	}
	data, err := checkpoint.Encode(state, maxCheckpointBytes)
	if err != nil {
		return err
	}
	return j.writer.Replace(j.path, data)
}
func (j *Journal) Close() error {
	if j.closed {
		return nil
	}
	j.closed = true
	return j.lock.Close()
}
