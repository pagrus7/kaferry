package checkpoint

import (
	"errors"
	"io"
	"os"
)

type ReadFile interface {
	io.ReaderAt
	Stat() (os.FileInfo, error)
}

// Read reads bounded, regular-file contents. Callers own closing the file and
// opening it without following symlinks or blocking on special files.
func Read(file ReadFile, limit int64) ([]byte, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > limit {
		return nil, errors.New("checkpoint must be a regular file within the size limit")
	}
	data, err := io.ReadAll(io.NewSectionReader(file, 0, info.Size()))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != info.Size() {
		return nil, io.ErrUnexpectedEOF
	}
	return data, nil
}
