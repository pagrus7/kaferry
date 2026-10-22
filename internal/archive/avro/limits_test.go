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

func TestBlockTargetAllowsLargeRecordsWithoutCombiningFullBlocks(t *testing.T) {
	// Given records just over half the block target and one record larger than the target.
	c := codec(t, 2*BlockTarget)
	values := make([]Record, 4)
	for i := range values {
		values[i] = records()[0]
		values[i].Offset += int64(i)
		values[i].Value = bytes.Repeat([]byte{byte(i)}, BlockTarget/2)
	}
	values[2].Value = bytes.Repeat([]byte{2}, BlockTarget+100)
	// When encoded without explicit intermediate flushes, then each remains in its own readable block.
	data := write(t, c, values, len(values))
	require.Equal(t, values, read(t, c, data))
	r, err := c.Open(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	blocks := 0
	for position := r.Start; position < int64(len(data)); blocks++ {
		block, err := r.Block(position)
		require.NoError(t, err)
		require.EqualValues(t, 1, block.Count, "an oversized record stays alone; other records fit the target")
		position = block.End
	}
	require.Len(t, values, blocks)
}

func TestRejectedEncodedRecordPreservesEarlierAndLaterRecords(t *testing.T) {
	// Given a valid pending record and a rejection caused by encoded envelope size.
	c := codec(t, 4096)
	var output bytes.Buffer
	w, err := c.NewWriter(&output, identity())
	require.NoError(t, err)
	values := records()[:2]
	require.NoError(t, w.Append(values[0]))
	before := bytes.Clone(output.Bytes())
	rejected := values[0]
	rejected.Key, rejected.Headers = nil, nil
	rejected.Value = make([]byte, c.limit)
	// When encoding fails its limit, then no pending data is overwritten or published.
	var invalid *InvalidRecord
	require.ErrorAs(t, w.Append(rejected), &invalid)
	require.Equal(t, before, output.Bytes())
	require.EqualValues(t, 1, w.Count())
	// Then a later valid record uses clean scratch space and both admitted records survive.
	require.NoError(t, w.Append(values[1]))
	require.NoError(t, w.Flush())
	require.Nil(t, w.pending)
	require.Nil(t, w.datum)
	require.Equal(t, values, read(t, c, output.Bytes()))
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
	err    error
}

func (r failingReaderAt) ReadAt(p []byte, offset int64) (int, error) {
	if offset >= r.failAt {
		return 0, r.err
	}
	return r.ReaderAt.ReadAt(p, offset)
}

func TestStorageReadErrorsAreNotEndOfFile(t *testing.T) {
	c := codec(t, 4096)
	data := write(t, c, records()[:1], 1)
	for _, failure := range []error{errDisk, io.EOF} {
		for _, endOfBlock := range []bool{false, true} {
			// Given a file whose storage fails while reading block data or its sync marker.
			r, err := c.Open(bytes.NewReader(data), int64(len(data)))
			require.NoError(t, err)
			failAt := r.Start + 2
			if endOfBlock {
				failAt = int64(len(data)) - 16
			}
			r.input = failingReaderAt{ReaderAt: bytes.NewReader(data), failAt: failAt, err: failure}
			// When scanned, then the I/O failure is preserved rather than treated as completion.
			_, err = r.Scan()
			if failure == io.EOF {
				require.ErrorIs(t, err, io.ErrUnexpectedEOF)
			} else {
				require.ErrorIs(t, err, failure)
			}
			require.NotErrorIs(t, err, io.EOF, "data loss must not resemble completion")
		}
	}
}
