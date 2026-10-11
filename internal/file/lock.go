package file

import (
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
)

func lock(file *os.File) error {
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return fmt.Errorf("file is busy or cannot be locked: %w", err)
	}
	return nil
}
func syncDirectory(path string) error {
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

type Conflict struct{ Err error }

func (e *Conflict) Error() string               { return e.Err.Error() }
func (e *Conflict) Unwrap() error               { return e.Err }
func (*Conflict) ExitCode() int                 { return 2 }
func conflict(format string, args ...any) error { return &Conflict{Err: fmt.Errorf(format, args...)} }
