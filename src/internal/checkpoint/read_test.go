package checkpoint

import (
	"bytes"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

type readFixture struct {
	io.ReaderAt
	info    os.FileInfo
	statErr error
	reads   int
}

func (f *readFixture) Stat() (os.FileInfo, error) { return f.info, f.statErr }
func (f *readFixture) ReadAt(p []byte, offset int64) (int, error) {
	f.reads++
	return f.ReaderAt.ReadAt(p, offset)
}

type failingReader struct{ err error }

func (r failingReader) ReadAt([]byte, int64) (int, error) { return 0, r.err }

func TestReadRejectsChangedOrUnboundedFiles(t *testing.T) {
	// Given the size of a real checkpoint and injected failures after opening it.
	file, err := os.CreateTemp(t.TempDir(), "state")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, file.Close()) })
	_, err = file.WriteString("state")
	require.NoError(t, err)
	info, err := file.Stat()
	require.NoError(t, err)
	directory, err := os.Stat(t.TempDir())
	require.NoError(t, err)
	failure := errors.New("fixture I/O failure")
	for _, tc := range []struct {
		name    string
		info    os.FileInfo
		reader  io.ReaderAt
		limit   int64
		statErr error
		wantErr error
		noRead  bool
	}{
		{name: "bounded prefix", info: info, reader: bytes.NewReader([]byte("state appended")), limit: 5},
		{name: "truncated", info: info, reader: bytes.NewReader([]byte("sta")), limit: 5, wantErr: io.ErrUnexpectedEOF},
		{name: "read failure", info: info, reader: failingReader{failure}, limit: 5, wantErr: failure},
		{name: "stat failure", statErr: failure, wantErr: failure, noRead: true},
		{name: "oversized", info: info, limit: 4, noRead: true},
		{name: "directory", info: directory, limit: 1 << 20, noRead: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := &readFixture{ReaderAt: tc.reader, info: tc.info, statErr: tc.statErr}
			// When read, then short or failed I/O cannot become a complete checkpoint.
			data, err := Read(input, tc.limit)
			switch {
			case tc.wantErr != nil:
				require.ErrorIs(t, err, tc.wantErr)
			case tc.noRead:
				require.ErrorContains(t, err, "regular file within the size limit")
			default:
				require.NoError(t, err)
				require.Equal(t, "state", string(data))
			}
			if tc.noRead {
				require.Zero(t, input.reads)
			}
		})
	}
}
