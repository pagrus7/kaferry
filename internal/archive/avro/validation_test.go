package avro

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"testing"

	"github.com/iskorotkov/avro/v2/ocf"
	"github.com/stretchr/testify/require"
)

func TestScanAndOffsetOrder(t *testing.T) {
	// Given unordered record timestamps, duplicate offsets, and a missing timestamp.
	c := codec(t, 4<<20)
	data := write(t, c, records(), 1)
	r, err := c.Open(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	// When every block is scanned, then statistics retain exact signed ranges and missing counts.
	stats, err := r.Scan()
	require.NoError(t, err)
	require.Equal(t, Statistics{Records: 4, MinOffset: 42, MaxOffset: 42, MinTimestamp: new(int64(math.MinInt64)), MaxTimestamp: new(int64(math.MaxInt64)), MissingTimestamps: 1}, stats)
	// Given a backward offset crossing a complete-block boundary.
	broken := records()[:3]
	broken[1].Offset = 43
	bad := rawContainer(t, c, broken)
	reader, err := c.Open(bytes.NewReader(bad), int64(len(bad)))
	require.NoError(t, err)
	// When validation reads it, then the header's first offset and file ordering are enforced.
	_, err = reader.Scan()
	require.ErrorContains(t, err, "between Avro blocks")
}

func TestReaderRejectsInvalidRecordIdentityAndOrder(t *testing.T) {
	c := codec(t, 4<<20)
	for _, tc := range []struct {
		name   string
		mutate func([]Record)
	}{
		{"different source", func(records []Record) { records[1].Source.TopicID[0]++ }},
		{"backward offset", func(records []Record) { records[1].Offset-- }},
		{"first offset", func(records []Record) { records[0].Offset++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given a structurally valid container with invalid logical source metadata.
			values := records()
			tc.mutate(values)
			data := rawContainer(t, c, values)
			r, err := c.Open(bytes.NewReader(data), int64(len(data)))
			require.NoError(t, err)
			// When the same strict iterator reads it, then the mismatch stops the scan.
			_, err = r.Scan()
			require.Error(t, err)
		})
	}
}

func TestBlockPositionsAndExactCount(t *testing.T) {
	// Given a two-record block whose declared count is reduced without changing its checksum.
	c := codec(t, 4<<20)
	data := write(t, c, records()[:2], 2)
	r, err := c.Open(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	data[r.Start] = 2
	r, err = c.Open(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	block, err := r.Block(r.Start)
	require.NoError(t, err)
	// When resuming directly at that block, then omitted records cannot disappear at EOF.
	_, err = block.Next()
	require.ErrorContains(t, err, "consume block exactly")
	_, again := block.Next()
	require.ErrorIs(t, again, err, "a failed decoder stays failed")
	for _, position := range []int64{-1, r.Start - 1, int64(len(data)) + 1} {
		_, err := r.Block(position)
		require.Error(t, err)
	}
	_, err = r.Block(int64(len(data)))
	require.ErrorIs(t, err, io.EOF)
}

func TestHeaderValidation(t *testing.T) {
	c := codec(t, 4096)
	meta, err := json.Marshal(identity())
	require.NoError(t, err)
	for _, tc := range []struct {
		name   string
		change func(*ocf.Header)
	}{
		{"magic", func(h *ocf.Header) { h.Magic[0]++ }},
		{"codec", func(h *ocf.Header) { h.Meta["avro.codec"] = []byte("null") }},
		{"schema syntax", func(h *ocf.Header) { h.Meta["avro.schema"] = []byte("{") }},
		{"schema", func(h *ocf.Header) { h.Meta["avro.schema"] = []byte(`"bytes"`) }},
		{"identity syntax", func(h *ocf.Header) { h.Meta["kaferry"] = []byte("{") }},
		{"trailing identity", func(h *ocf.Header) { h.Meta["kaferry"] = append(h.Meta["kaferry"], []byte(" []")...) }},
		{"unknown identity field", func(h *ocf.Header) { h.Meta["kaferry"] = []byte(`{"unexpected":true}`) }},
		{"version", func(h *ocf.Header) {
			h.Meta["kaferry"] = bytes.ReplaceAll(h.Meta["kaferry"], []byte(`"version":1`), []byte(`"version":2`))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given an OCF header that violates the archive contract.
			header := ocf.Header{Magic: [4]byte{'O', 'b', 'j', 1}, Meta: map[string][]byte{"avro.codec": []byte("snappy"), "avro.schema": []byte(Schema()), "kaferry": bytes.Clone(meta)}}
			tc.change(&header)
			data, err := c.api.Marshal(ocf.HeaderSchema, header)
			require.NoError(t, err)
			// When opened, then the specific header is rejected before block reads.
			_, err = c.Open(bytes.NewReader(data), int64(len(data)))
			require.Error(t, err)
		})
	}
}

func TestInvalidFileIdentityAndInitialOffset(t *testing.T) {
	c := codec(t, 4096)
	for _, change := range []func(*Identity){
		func(i *Identity) { i.Version++ }, func(i *Identity) { i.WriterID = "" },
		func(i *Identity) { i.FileID = "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz" },
		func(i *Identity) { i.FirstOffset = -1 }, func(i *Identity) { i.Source.Cluster = "" },
		func(i *Identity) { i.Source.Cluster = "\xc2"; i.Source.Topic = "\xa2" },
	} {
		// Given an invalid identity, when a writer is created, then it writes nothing.
		id := identity()
		change(&id)
		var output bytes.Buffer
		_, err := c.NewWriter(&output, id)
		require.Error(t, err)
		require.Empty(t, output.Bytes())
	}
	var output bytes.Buffer
	w, err := c.NewWriter(&output, identity())
	require.NoError(t, err)
	bad := records()[0]
	bad.Offset++
	var rejected *InvalidRecord
	err = w.Append(bad)
	require.ErrorContains(t, err, "first source offset")
	require.False(t, errors.As(err, &rejected), "metadata mistakes cannot be skipped")
	require.Zero(t, w.Count())
	// Then the expected first record can still be appended after a rejection.
	require.NoError(t, w.Append(records()[0]))
	require.EqualValues(t, 1, w.Count())
}

func TestPendingBlockNeverExceedsReadLimit(t *testing.T) {
	// Given several records that fit individually but would exceed one shared block limit.
	c := codec(t, 4096)
	want := records()[:3]
	for i := range want {
		want[i].Value = bytes.Repeat([]byte{byte(i)}, 2500)
	}
	// When appended without explicit intermediate flushes, then the writer cuts before overflowing.
	data := write(t, c, want, 100)
	require.Equal(t, want, read(t, c, data))
}

type observedReaderAt struct {
	io.ReaderAt
	minimum int64
}

func (r *observedReaderAt) ReadAt(p []byte, offset int64) (int, error) {
	r.minimum = min(r.minimum, offset)
	return r.ReaderAt.ReadAt(p, offset)
}

func TestResumeReadsNoEarlierBlocks(t *testing.T) {
	// Given several blocks with indistinguishable source offsets.
	c := codec(t, 4<<20)
	data := write(t, c, records(), 2)
	input := &observedReaderAt{ReaderAt: bytes.NewReader(data), minimum: math.MaxInt64}
	r, err := c.Open(input, int64(len(data)))
	require.NoError(t, err)
	first, err := r.Block(r.Start)
	require.NoError(t, err)
	// When reopening the next block and skipping one physical record.
	input.minimum = math.MaxInt64
	second, err := r.Block(first.End)
	require.NoError(t, err)
	_, err = second.Next()
	require.NoError(t, err)
	actual, err := second.Next()
	require.NoError(t, err)
	// Then the exact duplicate resumes without scanning preceding data.
	require.Equal(t, records()[3], actual)
	require.GreaterOrEqual(t, input.minimum, first.End)
}

func FuzzContainer(f *testing.F) {
	c := codec(f, 4<<20)
	f.Add(write(f, c, records(), 2))
	f.Add([]byte("not an archive"))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 8<<20 {
			t.Skip()
		}
		r, err := c.Open(bytes.NewReader(data), int64(len(data)))
		if err == nil {
			_, _ = r.Scan()
		}
	})
}

func TestMalformedBlockLengths(t *testing.T) {
	c := codec(t, 4096)
	valid := write(t, c, records()[:1], 1)
	r, err := c.Open(bytes.NewReader(valid), int64(len(valid)))
	require.NoError(t, err)
	for _, tail := range [][]byte{{128}, {2, 128}, {2, 1}, {2, 127}, {128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128}} {
		// Given a truncated, negative or overflowing block field.
		data := append(bytes.Clone(valid[:r.Start]), tail...)
		reader, err := c.Open(bytes.NewReader(data), int64(len(data)))
		require.NoError(t, err)
		// When reading its frame, then no declared-length allocation is attempted.
		_, err = reader.Block(reader.Start)
		require.Error(t, err)
	}
}
