package avro

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	"github.com/iskorotkov/avro/v2/ocf"
	"github.com/stretchr/testify/require"
)

func TestWriterFailuresAtHeaderAndForcedFlush(t *testing.T) {
	// Given storage that cannot write the header, when opened, then no writer is returned.
	c := codec(t, 4096)
	_, err := c.NewWriter(&brokenWriter{fail: true}, identity())
	require.ErrorIs(t, err, errDisk)
	// Given a valid pending block followed by a record that forces it to flush.
	output := &brokenWriter{}
	w, err := c.NewWriter(output, identity())
	require.NoError(t, err)
	record := records()[0]
	record.Value = make([]byte, 2500)
	require.NoError(t, w.Append(record))
	output.fail = true
	// When that flush fails, then the error stays fatal and the new record is not accepted.
	require.ErrorIs(t, w.Append(record), errDisk)
	require.EqualValues(t, 1, w.Count())
	require.ErrorIs(t, w.Flush(), errDisk)
}

func TestHeaderCountLimitMatchesReader(t *testing.T) {
	// Given a record at the maximum accepted header count.
	c := codec(t, 2<<20)
	record := records()[0]
	record.Headers = make([]Header, c.maxHeaders)
	for i := range record.Headers {
		record.Headers[i].Key = []byte{}
	}
	// When flushed and decoded, then the writer's admitted count fits the reader's allocation cap.
	data := write(t, c, []Record{record}, 1)
	require.Equal(t, []Record{record}, read(t, c, data))
}

func TestCompressedBlockGuards(t *testing.T) {
	c := codec(t, 4096)
	valid := write(t, c, records()[:1], 1)
	r, err := c.Open(bytes.NewReader(valid), int64(len(valid)))
	require.NoError(t, err)
	for _, compressed := range [][]byte{
		append(binary.AppendUvarint(nil, 4097), 0, 0, 0, 0),
		{1, 255, 0, 0, 0, 0},
		{128, 0, 0, 0, 0},
		(&ocf.SnappyCodec{}).Encode(nil),
	} {
		// Given a bad Snappy payload, excessive declared expansion or impossible record count.
		data := bytes.Clone(valid[:r.Start])
		data = binary.AppendVarint(data, 1)
		data = binary.AppendVarint(data, int64(len(compressed)))
		data = append(data, compressed...)
		data = append(data, valid[len(valid)-16:]...)
		reader, err := c.Open(bytes.NewReader(data), int64(len(data)))
		require.NoError(t, err)
		// When the frame is read, then it fails before any record can be returned.
		_, err = reader.Block(reader.Start)
		require.Error(t, err)
	}
}

type failingReaderAt struct {
	io.ReaderAt
	failAt int64
}

func (r failingReaderAt) ReadAt(p []byte, offset int64) (int, error) {
	if offset >= r.failAt {
		return 0, errDisk
	}
	return r.ReaderAt.ReadAt(p, offset)
}

func TestStorageReadErrorsAreNotEndOfFile(t *testing.T) {
	c := codec(t, 4096)
	data := write(t, c, records()[:1], 1)
	for _, endOfBlock := range []bool{false, true} {
		// Given a file whose storage fails while reading block data or its sync marker.
		r, err := c.Open(bytes.NewReader(data), int64(len(data)))
		require.NoError(t, err)
		failAt := r.Start + 2
		if endOfBlock {
			failAt = int64(len(data)) - 16
		}
		r.input = failingReaderAt{ReaderAt: bytes.NewReader(data), failAt: failAt}
		// When scanned, then the I/O failure is preserved rather than treated as completion.
		_, err = r.Scan()
		require.ErrorIs(t, err, errDisk)
	}
}
