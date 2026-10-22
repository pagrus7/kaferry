package avro

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"

	lib "github.com/iskorotkov/avro/v2"
	"github.com/iskorotkov/avro/v2/ocf"
	goavro "github.com/linkedin/goavro/v2"
	"github.com/stretchr/testify/require"
)

func identity() Identity {
	return Identity{Version: Version, BlockLimit: 4 << 20, WriterID: "0123456789abcdef0123456789abcdef", FileID: "abcdef0123456789abcdef0123456789", Source: Source{Cluster: "cluster", Topic: "orders", TopicID: [16]byte{1, 2, 3}, Partition: 2}, FirstOffset: 42}
}

func records() []Record {
	var result []Record
	for i, ts := range []*int64{nil, new(int64(-1)), new(int64(math.MinInt64)), new(int64(math.MaxInt64))} {
		result = append(result, Record{Source: identity().Source, Offset: 42,
			TimestampMS: ts, TimestampKind: []string{"unknown", "create_time", "log_append_time", "create_time"}[i],
			Key: [][]byte{nil, {}, {0, 255}, {128}}[i], Value: [][]byte{{}, nil, {0, 128}, {255}}[i],
			Headers: []Header{{Key: []byte{0, 255}, Value: nil}, {Key: []byte{0, 255}, Value: []byte{}}, {Key: []byte{}, Value: []byte{128}}},
		})
	}
	return result
}

func codec(t testing.TB, limit int64) *Codec {
	t.Helper()
	c, err := New(limit)
	require.NoError(t, err)
	return c
}

func write(t testing.TB, c *Codec, records []Record, batch int) []byte {
	t.Helper()
	var output bytes.Buffer
	w, err := c.NewWriter(&output, identity())
	require.NoError(t, err)
	for i, record := range records {
		require.NoError(t, w.Append(record))
		if (i+1)%batch == 0 {
			require.NoError(t, w.Flush())
		}
	}
	require.NoError(t, w.Flush())
	require.Nil(t, w.pending, "flush releases retained partition payload memory")
	return output.Bytes()
}

func read(t testing.TB, c *Codec, data []byte) []Record {
	t.Helper()
	r, err := c.Open(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	var result []Record
	for position := r.Start; position < int64(len(data)); {
		b, err := r.Block(position)
		require.NoError(t, err)
		for range b.Count {
			record, err := b.Next()
			require.NoError(t, err)
			result = append(result, record)
		}
		_, err = b.Next()
		require.ErrorIs(t, err, io.EOF)
		position = b.End
	}
	return result
}

func nullable(value []byte) any {
	if value == nil {
		return nil
	}
	return goavro.Union("bytes", value)
}

func native(record Record) map[string]any {
	headers := make([]any, len(record.Headers))
	for i, h := range record.Headers {
		headers[i] = map[string]any{"key": h.Key, "value": nullable(h.Value)}
	}
	var ts any
	if record.TimestampMS != nil {
		ts = goavro.Union("long", *record.TimestampMS)
	}
	return map[string]any{"source": map[string]any{"cluster": record.Source.Cluster, "topic": record.Source.Topic, "topic_id": record.Source.TopicID[:], "partition": record.Source.Partition}, "offset": record.Offset, "timestamp_ms": ts, "timestamp_kind": record.TimestampKind, "key": nullable(record.Key), "value": nullable(record.Value), "headers": headers}
}

func TestFidelityAndIndependentReader(t *testing.T) {
	// Given complete Kafka records, including duplicate offsets and binary header names.
	c, want := codec(t, 4<<20), records()
	data := write(t, c, want, 2)
	// When the block reader and an independent implementation read the container.
	require.Equal(t, want, read(t, c, data))
	r, err := goavro.NewOCFReader(bytes.NewReader(data))
	require.NoError(t, err)
	var actual []any
	for r.Scan() {
		v, err := r.Read()
		require.NoError(t, err)
		actual = append(actual, v)
	}
	// Then null/empty, raw bytes, provenance and signed timestamps survive in both readers.
	require.NoError(t, r.Err())
	require.Len(t, actual, len(want))
	for i := range want {
		require.Equal(t, native(want[i]), actual[i], "logical record %d", i)
	}
	require.Equal(t, "snappy", r.CompressionName())
}

func TestIndependentWriter(t *testing.T) {
	// Given the published schema and identity written by another Avro implementation.
	var output bytes.Buffer
	meta, err := json.Marshal(identity())
	require.NoError(t, err)
	w, err := goavro.NewOCFWriter(goavro.OCFConfig{W: &output, Schema: Schema(), CompressionName: "snappy", MetaData: map[string][]byte{"kaferry": meta}})
	require.NoError(t, err)
	values := make([]any, len(records()))
	for i, record := range records() {
		values[i] = native(record)
	}
	require.NoError(t, w.Append(values))
	// When our block reader opens it, then all records remain identical.
	require.Equal(t, records(), read(t, codec(t, 4<<20), output.Bytes()))
}

func TestRejectedRecordDoesNotChangePendingBlock(t *testing.T) {
	// Given a valid record pending in a container.
	c := codec(t, 4096)
	var output bytes.Buffer
	w, err := c.NewWriter(&output, identity())
	require.NoError(t, err)
	good := records()[0]
	require.NoError(t, w.Append(good))
	// When invalid input is rejected, then flushing retains only the valid record.
	for _, tc := range []struct {
		name         string
		change       func(*Record)
		skip, rotate bool
	}{
		{"bad kind", func(r *Record) { r.TimestampKind = "bad" }, false, false},
		{"negative offset", func(r *Record) { r.Offset = -1 }, false, false},
		{"backward offset", func(r *Record) { r.Offset-- }, false, true},
		{"changed topic", func(r *Record) { r.Source.TopicID[0]++ }, false, true},
		{"header limit", func(r *Record) { r.Headers = make([]Header, c.maxHeaders+1) }, true, false},
		{"payload limit", func(r *Record) { r.Value = make([]byte, 4096) }, true, false},
		{"encoded limit", func(r *Record) { r.Value = make([]byte, 4096-8) }, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := good
			tc.change(&bad)
			err := w.Append(bad)
			require.Error(t, err)
			require.NotEmpty(t, err.Error(), "rejection explains the failure")
			var rejected *InvalidRecord
			require.Equal(t, tc.skip, errors.As(err, &rejected), "only format limits permit skipping")
			require.Equal(t, tc.rotate, errors.Is(err, ErrRotationRequired), "identity/ordering changes require rotation")
			if tc.skip {
				require.NotNil(t, errors.Unwrap(err), "classification retains the cause")
			}
		})
	}
	require.NoError(t, w.Flush())
	require.Equal(t, []Record{good}, read(t, c, output.Bytes()))
}

type brokenWriter struct {
	bytes.Buffer
	fail  bool
	short bool
	calls int
}

var errDisk = errors.New("disk write failed")

func (w *brokenWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.fail {
		return 0, errDisk
	}
	if w.short {
		return len(p) - 1, nil
	}
	return w.Buffer.Write(p)
}

func TestWriteFailuresAreSticky(t *testing.T) {
	for _, short := range []bool{false, true} {
		t.Run(map[bool]string{false: "write error", true: "short write"}[short], func(t *testing.T) {
			// Given a writer whose next complete-block write fails.
			output := &brokenWriter{}
			w, err := codec(t, 4<<20).NewWriter(output, identity())
			require.NoError(t, err)
			require.NoError(t, w.Append(records()[0]))
			output.fail, output.short = !short, short
			// When flush fails, then later calls return that error without more writes.
			err = w.Flush()
			require.Error(t, err)
			calls := output.calls
			require.ErrorIs(t, w.Flush(), err)
			require.ErrorIs(t, w.Append(records()[1]), err)
			require.Equal(t, calls, output.calls)
		})
	}
}

func TestAutomaticBlocksAndSharedLimits(t *testing.T) {
	// Given a record larger than the block target and another near the configured raw limit.
	c := codec(t, 2<<20)
	want := records()[:3]
	want[0].Value = bytes.Repeat([]byte{1}, BlockTarget+1)
	want[1].Value = bytes.Repeat([]byte{2}, (2<<20)-1024)
	data := write(t, c, want, 3)
	// When automatic size cuts and explicit flushes finish, then every block fits the reader.
	require.Equal(t, want, read(t, c, data))
	r, err := c.Open(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	b, err := r.Block(r.Start)
	require.NoError(t, err)
	require.EqualValues(t, 1, b.Count, "size target emits a complete crossing block")
}

func TestSavedPrefixAndCursor(t *testing.T) {
	// Given synced batches whose duplicate source offsets cannot serve as replay cursors.
	c := codec(t, 4<<20)
	f, err := os.Create(filepath.Join(t.TempDir(), "data.partial"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })
	w, err := c.NewWriter(f, identity())
	require.NoError(t, err)
	for _, record := range records() {
		require.NoError(t, w.Append(record))
		require.NoError(t, w.Flush())
		require.NoError(t, f.Sync())
	}
	saved := w.Size()
	_, err = f.Write([]byte{2, 128})
	require.NoError(t, err)
	// When reopened at the saved boundary and second block, then earlier blocks are unnecessary.
	r, err := c.Open(f, saved)
	require.NoError(t, err)
	first, err := r.Block(r.Start)
	require.NoError(t, err)
	second, err := r.Block(first.End)
	require.NoError(t, err)
	record, err := second.Next()
	require.NoError(t, err)
	require.Equal(t, records()[1], record)
	// Then truncation of the unacknowledged tail restores exactly the saved prefix.
	require.NoError(t, f.Truncate(saved))
	require.NoError(t, f.Sync())
	data, err := os.ReadFile(f.Name())
	require.NoError(t, err)
	require.Equal(t, records(), read(t, c, data))
}

func TestStrictBlockValidation(t *testing.T) {
	c := codec(t, 4<<20)
	data := write(t, c, records(), 4)
	r, err := c.Open(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	for _, tc := range []struct {
		name string
		edit func([]byte) []byte
	}{
		{"zero count", func(b []byte) []byte { b[r.Start] = 0; return b }},
		{"negative count", func(b []byte) []byte { b[r.Start] = 1; return b }},
		{"too few records", func(b []byte) []byte { b[r.Start] = 2; return b }},
		{"too many records", func(b []byte) []byte { b[r.Start] = 10; return b }},
		{"checksum", func(b []byte) []byte { b[len(b)-17] ^= 1; return b }},
		{"sync", func(b []byte) []byte { b[len(b)-1] ^= 1; return b }},
		{"torn tail", func(b []byte) []byte { return b[:len(b)-1] }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given a malformed complete-file candidate.
			bad := tc.edit(bytes.Clone(data))
			reader, err := c.Open(bytes.NewReader(bad), int64(len(bad)))
			require.NoError(t, err)
			// When read from its first block, then corruption is reported instead of silently losing records.
			block, err := reader.Block(reader.Start)
			if err == nil {
				for range block.Count {
					_, err = block.Next()
					if err != nil {
						break
					}
				}
			}
			require.Error(t, err)
			require.NotErrorIs(t, err, io.EOF, "invalid input must not resemble completion")
		})
	}
}

func TestInvalidArrayCountIsRejected(t *testing.T) {
	// Given a malicious negative array count whose absolute value overflows int64.
	var raw [10]byte
	n := binary.PutVarint(raw[:], math.MinInt64)
	schema, err := lib.Parse(`{"type":"array","items":"bytes"}`)
	require.NoError(t, err)
	var value [][]byte
	// When the configured datum decoder reads it, then it fails without constructing an invalid slice.
	err = codec(t, 4096).api.Unmarshal(schema, raw[:n], &value)
	require.Error(t, err)
	require.Empty(t, value)
}

func TestUnsupportedHeadersAndIdentity(t *testing.T) {
	// Given invalid limits or an identity without an original Kafka topic ID.
	for _, limit := range []int64{0, -1, 1 << 32} {
		_, err := New(limit)
		require.Error(t, err)
	}
	bad := identity()
	bad.Source.TopicID = [16]byte{}
	// When creating a container, then unsupported input is rejected before writing.
	var output bytes.Buffer
	_, err := codec(t, 4096).NewWriter(&output, bad)
	require.Error(t, err)
	require.Empty(t, output.Bytes())
	_, err = codec(t, 4096).Open(bytes.NewReader([]byte("not Avro")), 8)
	require.Error(t, err)
}

// A directly encoded bad block tests validation without making the writer accept bad input.
func rawContainer(t *testing.T, c *Codec, values []Record) []byte {
	t.Helper()
	var output bytes.Buffer
	id := identity()
	id.BlockLimit = c.limit
	meta, err := json.Marshal(id)
	require.NoError(t, err)
	w, err := ocf.NewEncoder(Schema(), &output, ocf.WithCodec(ocf.Snappy), ocf.WithBlockLength(1), ocf.WithMetadata(map[string][]byte{"kaferry": meta}), ocf.WithSchemaMarshaler(ocf.FullSchemaMarshaler))
	require.NoError(t, err)
	for _, record := range values {
		data, err := c.api.Marshal(c.schema, record)
		require.NoError(t, err)
		_, err = w.Write(data)
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())
	return output.Bytes()
}
