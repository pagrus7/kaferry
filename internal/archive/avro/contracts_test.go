package avro

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShortenedFileCannotCompleteNormally(t *testing.T) {
	// Given a pinned length with a complete final block missing from storage.
	c := codec(t, 4<<20)
	data := write(t, c, records(), 2)
	r, err := c.Open(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	first, err := r.Block(r.Start)
	require.NoError(t, err)
	short, err := c.Open(bytes.NewReader(data[:first.End]), int64(len(data)))
	require.NoError(t, err)
	// When the saved next-block cursor is read, then it is data loss, not normal EOF.
	_, err = short.Block(first.End)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.NotErrorIs(t, err, io.EOF)
	_, err = short.Scan()
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestRecordedLimitAndReaderCap(t *testing.T) {
	// Given valid data produced with a larger configured block limit.
	data := write(t, codec(t, 8<<20), records(), 2)
	// When a reader has a lower cap, then it reports a configuration requirement, not corruption.
	_, err := codec(t, 4<<20).Open(bytes.NewReader(data), int64(len(data)))
	var limit *LimitError
	require.ErrorAs(t, err, &limit)
	require.EqualValues(t, 8<<20, limit.Required)
	require.EqualValues(t, 4<<20, limit.Allowed)
	require.ErrorContains(t, err, "configured maximum")
	// Then a reader admitting that limit can read every record.
	require.Equal(t, records(), read(t, codec(t, 64<<20), data))
}

func TestRecordedHeaderLimitIsEnforced(t *testing.T) {
	// Given a structurally valid file that exceeds its advertised header count cap.
	c := codec(t, 4096)
	record := records()[0]
	record.Headers = make([]Header, c.maxHeaders+1)
	data := rawContainer(t, c, []Record{record})
	r, err := codec(t, 4<<20).Open(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	// When a reader with a larger local cap reads it, then the file's recorded bound still applies.
	_, err = r.Scan()
	require.ErrorContains(t, err, "MaxSliceAllocSize")
}

func TestCanonicalIdentityJSON(t *testing.T) {
	// Given the complete, canonical identity with a compact textual topic ID.
	id := identity()
	id.Source.TopicID = TopicID{0xab, 0xcd}
	data, err := json.Marshal(id)
	require.NoError(t, err)
	require.Equal(t, `{"version":1,"writer_id":"0123456789abcdef0123456789abcdef","file_id":"abcdef0123456789abcdef0123456789","source":{"cluster":"cluster","topic":"orders","topic_id":"abcd0000000000000000000000000000","partition":2},"first_offset":42,"block_limit":4194304}`, string(data), "v1 metadata has a fixed field order and topic ID representation")
	got, err := parseIdentity(data)
	require.NoError(t, err)
	require.Equal(t, id, got)
	// When ambiguous or non-canonical JSON forms are supplied, then all are rejected.
	for _, tc := range []struct {
		name   string
		change func([]byte) []byte
	}{
		{"whitespace", func(b []byte) []byte { return append([]byte(" "), b...) }},
		{"key case", func(b []byte) []byte { return bytes.Replace(b, []byte(`"file_id"`), []byte(`"FILE_ID"`), 1) }},
		{"duplicate", func(b []byte) []byte {
			return append(b[:len(b)-1], []byte(`,"file_id":"abcdef0123456789abcdef0123456789"}`)...)
		}},
		{"array topic ID", func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"abcd0000000000000000000000000000"`), []byte(`[171,205]`), 1)
		}},
		{"short topic ID", func(b []byte) []byte {
			return bytes.Replace(b, []byte(`abcd0000000000000000000000000000`), []byte(`abcd`), 1)
		}},
		{"uppercase topic ID", func(b []byte) []byte {
			return bytes.Replace(b, []byte(`abcd0000000000000000000000000000`), []byte(`ABCD0000000000000000000000000000`), 1)
		}},
		{"invalid limit", func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"block_limit":4194304`), []byte(`"block_limit":4294967296`), 1)
		}},
		{"missing limit", func(b []byte) []byte { return bytes.Replace(b, []byte(`,"block_limit":4194304`), nil, 1) }},
	} {
		t.Run(tc.name, func(t *testing.T) { _, err := parseIdentity(tc.change(bytes.Clone(data))); require.Error(t, err) })
	}
}
