package checkpoint

import (
	"bytes"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

type readFile struct {
	*bytes.Reader
	size                       int64
	mode                       os.FileMode
	statErr, readErr, closeErr error
	reads, closes              int
}

func (f *readFile) Stat() (os.FileInfo, error) {
	return readInfo{size: f.size, mode: f.mode}, f.statErr
}
func (f *readFile) ReadAt(p []byte, off int64) (int, error) {
	f.reads++
	if f.readErr != nil {
		return 0, f.readErr
	}
	return f.Reader.ReadAt(p, off)
}
func (f *readFile) Close() error { f.closes++; return f.closeErr }

type readInfo struct {
	os.FileInfo
	size int64
	mode os.FileMode
}

func (i readInfo) Size() int64       { return i.size }
func (i readInfo) Mode() os.FileMode { return i.mode }

func TestReadBoundsAndClosesCheckpointFiles(t *testing.T) {
	statFailure := errors.New("stat failed")
	readFailure := errors.New("read failed")
	closeFailure := errors.New("close failed")
	for _, tc := range []struct {
		name                       string
		contents                   string
		size, limit                int64
		mode                       os.FileMode
		statErr, readErr, closeErr error
		want                       string
		failure                    error
		message                    string
		unread                     bool
	}{
		{name: "at limit", contents: "saved", size: 5, limit: 5, want: "saved"},
		{name: "empty", limit: 5, unread: true},
		{name: "growth after stat", contents: "savedmore", size: 5, limit: 5, want: "saved"},
		{name: "oversized", contents: "saved", size: 5, limit: 4, message: "size limit", unread: true},
		{name: "directory", mode: os.ModeDir, limit: 5, message: "regular file", unread: true},
		{name: "truncated", contents: "sav", size: 5, limit: 5, failure: io.ErrUnexpectedEOF},
		{name: "stat failure", statErr: statFailure, limit: 5, failure: statFailure, unread: true},
		{name: "read failure", size: 5, limit: 5, readErr: readFailure, failure: readFailure},
		{name: "close failure", contents: "saved", size: 5, limit: 5, closeErr: closeFailure, failure: closeFailure},
		{name: "read and close failures", size: 5, limit: 5, readErr: readFailure, closeErr: closeFailure, failure: readFailure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given a checkpoint handle with a size or I/O boundary to exercise.
			file := &readFile{Reader: bytes.NewReader([]byte(tc.contents)), size: tc.size, mode: tc.mode,
				statErr: tc.statErr, readErr: tc.readErr, closeErr: tc.closeErr}

			// When reading through the shared bounded reader.
			data, err := Read(file, tc.limit)

			// Then bytes respect the observed size, failures remain identifiable,
			// and the owned handle closes exactly once on every path.
			switch {
			case tc.failure != nil:
				require.ErrorIs(t, err, tc.failure)
			case tc.message != "":
				require.ErrorContains(t, err, tc.message)
			default:
				require.NoError(t, err)
				require.Equal(t, tc.want, string(data))
			}
			if tc.closeErr != nil {
				require.ErrorIs(t, err, tc.closeErr)
			}
			if tc.unread {
				require.Zero(t, file.reads)
			}
			require.Equal(t, 1, file.closes)
		})
	}
}
