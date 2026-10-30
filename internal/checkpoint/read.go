package checkpoint

import (
	"errors"
	"io"
	"os"
)

// File is an opened checkpoint file.
type File interface {
	io.ReaderAt
	io.Closer
	Stat() (os.FileInfo, error)
}

// Read takes ownership of file and closes it, preserving close failures.
// It rejects nonregular or oversized files and reads exactly the size reported
// by Stat. Callers control opening flags and handle missing files.
func Read(file File, limit int64) (data []byte, result error) {
	defer func() { result = errors.Join(result, file.Close()) }()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > limit {
		return nil, errors.New("checkpoint must be a regular file within the size limit")
	}
	data, err = io.ReadAll(io.NewSectionReader(file, 0, info.Size()))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != info.Size() {
		return nil, io.ErrUnexpectedEOF
	}
	return data, nil
}
