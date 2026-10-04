package file

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	core "kafka-fwd2/internal/copy"
)

func TestLowerFrameLimitCannotTruncateCompleteUnacknowledgedData(t *testing.T) {
	// Given
	path := filepath.Join(t.TempDir(), "out.cbor")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	large, _ := NewCodec(1 << 20)

	o, err := NewOutput(NewPublisher(), path, "g", "fp", "", 0, large, 10, logger)

	if err != nil {
		t.Fatal(err)
	}
	id, headerEnd := o.DurableOutput()
	if err := o.Submit(core.Record{Value: make([]byte, 2000), TimestampType: "unknown"}, core.Token{}); err != nil {
		t.Fatal(err)
	}
	if err := o.Flush(); err != nil {
		t.Fatal(err)
	}
	o.Close()
	before, _ := os.Stat(path)
	small, _ := NewCodec(1000)

	// When reopening with a smaller frame limit.
	out, err := NewOutput(NewPublisher(), path, "g", "fp", id, headerEnd, small, 10, logger)

	// Then complete unacknowledged data is rejected without truncation.
	if err == nil {
		out.Close()
		t.Fatal("complete oversized frame was adopted")
	}
	after, _ := os.Stat(path)
	if before.Size() != after.Size() {
		t.Fatal("valid unacknowledged data was truncated due to a changed limit")
	}
}
func TestTemporaryCleanupRespectsActiveLocks(t *testing.T) {
	// Given
	dir := t.TempDir()
	path := filepath.Join(dir, "out.cbor")
	active, err := os.CreateTemp(dir, temporaryPrefix(path))
	if err != nil {
		t.Fatal(err)
	}
	defer active.Close()
	if err := lock(active); err != nil {
		t.Fatal(err)
	}
	stale, err := os.CreateTemp(dir, temporaryPrefix(path))
	if err != nil {
		t.Fatal(err)
	}
	stale.Close()

	// When
	cleanupTemporaryErr := cleanupTemporary(path)

	// Then
	if cleanupTemporaryErr != nil {
		t.Fatal(cleanupTemporaryErr)
	}
	if _, err := os.Stat(active.Name()); err != nil {
		t.Fatal("active temporary file was removed")
	}
	if _, err := os.Stat(stale.Name()); !os.IsNotExist(err) {
		t.Fatal("orphan temporary file remained")
	}
}
func TestEncoderDoesNotCreateUnreadableHeaderArrays(t *testing.T) {
	// Given
	c, _ := NewCodec(512)
	headers := make([]core.Header, c.maxHeaders+1)

	// When
	_, encodeRecordErr := c.EncodeRecord(core.Record{Headers: headers, TimestampType: "unknown"})

	// Then
	if encodeRecordErr == nil {
		t.Fatal("writer exceeded its reader's structural bound")
	}
}
func FuzzRecordDecoder(f *testing.F) {
	c, _ := NewCodec(1 << 20)
	data, _ := c.EncodeRecord(core.Record{TimestampType: "unknown"})
	f.Add(data[4 : len(data)-4])
	f.Add([]byte{0xa0})
	f.Fuzz(func(t *testing.T, payload []byte) {
		if len(payload) > 1<<20 {
			t.Skip()
		}
		_, _ = c.DecodeRecord(payload)
	})
}

func TestHeadersRequireNameAndValue(t *testing.T) {
	// Given
	c := codec(t)
	for _, h := range []map[string]any{{"name": []byte("header")}, {"value": nil}, {"name": "text", "value": nil}} {
		payload, err := c.encoder.Marshal(map[string]any{"key": nil, "value": nil, "headers": []map[string]any{h}, "timestamp_ms": nil, "timestamp_type": "unknown"})
		if err != nil {
			t.Fatal(err)
		}

		// When
		_, decodeRecordErr := c.DecodeRecord(payload)

		// Then
		if decodeRecordErr == nil {
			t.Fatal("malformed header was normalized silently")
		}
	}
}
