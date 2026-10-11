package file

import (
	"bytes"
	"strings"
	"testing"

	"github.com/fxamacker/cbor/v2"
	core "github.com/pagrus7/kaferry/internal/copy"
)

func TestRecordDecoderRejectsMalformedFields(t *testing.T) {
	for _, tc := range []struct {
		name, field, want string
		value             any
	}{
		{"missing key", "key", "record missing key", nil},
		{"undefined key", "key", "invalid record field key", cbor.RawMessage{0xf7}},
		{"numeric value", "value", "invalid record field value", 42},
		{"null headers", "headers", "invalid record field headers", nil},
		{"text timestamp", "timestamp_ms", "invalid record field timestamp_ms", "now"},
		{"unknown timestamp type", "timestamp_type", "invalid timestamp type", "event_time"},
		{"unknown field", "event_time", "unknown record field", 42},
		{"invalid provenance", "source", "invalid record provenance", map[string]any{"partition": "three"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given one malformed field in an otherwise valid CBOR record.
			c := codec(t)
			fields := map[string]any{"key": nil, "value": []byte{}, "headers": []any{}, "timestamp_ms": nil, "timestamp_type": "unknown"}
			fields[tc.field] = tc.value
			if tc.name == "missing key" {
				delete(fields, tc.field)
			}
			payload, err := c.encoder.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}

			// When decoding an archive record.
			_, err = c.DecodeRecord(payload)

			// Then malformed fields fail instead of being silently normalized.
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("DecodeRecord error=%v, want %q", err, tc.want)
			}
		})
	}
	// Given malformed CBOR rather than a record map.
	// When decoding it, then fail without accepting a record.
	if _, err := codec(t).DecodeRecord([]byte{0xff}); err == nil {
		t.Fatal("malformed CBOR accepted")
	}
}

func TestArchiveHeaderRejectsInvalidSchema(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		fields     map[string]any
	}{
		{"malformed CBOR", "invalid archive header CBOR", nil},
		{"unsupported version", "invalid or unsupported", map[string]any{"version": 2, "file_id": "id", "group": "g", "fingerprint": "fp"}},
		{"missing identity", "invalid or unsupported", map[string]any{"version": 1, "group": "g", "fingerprint": "fp"}},
		{"unknown field", "invalid archive header CBOR", map[string]any{"version": 1, "file_id": "id", "group": "g", "fingerprint": "fp", "extra": 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given a framed archive with a malformed or incompatible header.
			c := codec(t)
			payload := []byte{0xff}
			if tc.fields != nil {
				var err error
				payload, err = c.encoder.Marshal(tc.fields)
				if err != nil {
					t.Fatal(err)
				}
			}
			data := append(append([]byte{}, magic...), frame(payload)...)

			// When opening the archive header.
			_, _, err := c.Header(bytes.NewReader(data), int64(len(data)))

			// Then the archive is rejected before any records can be read.
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Header error=%v, want %q", err, tc.want)
			}
		})
	}
	// Given bytes without the archive signature.
	// When opening them, then report invalid magic.
	if _, _, err := codec(t).Header(bytes.NewReader([]byte("other format")), 12); err == nil || !strings.Contains(err.Error(), "magic") {
		t.Fatal("foreign file accepted", err)
	}
}

func TestCodecRejectsOversizedDataAndRemainsUsable(t *testing.T) {
	// Given a codec whose record limit is smaller than the submitted value.
	c, err := NewCodec(128)
	if err != nil {
		t.Fatal(err)
	}

	// When encoding oversized record and header data.
	_, recordErr := c.EncodeRecord(core.Record{Value: make([]byte, 129), TimestampType: "unknown"})
	_, headerErr := c.EncodeHeader(Header{Version: 1, ID: "id", Group: strings.Repeat("g", int(headerLimit)), Fingerprint: "fp"})

	// Then both limits fail and a later small record can still round-trip.
	if recordErr == nil || !strings.Contains(recordErr.Error(), "frame limit") || headerErr == nil || !strings.Contains(headerErr.Error(), "header too large") {
		t.Fatal("size limits not enforced", recordErr, headerErr)
	}
	data, err := c.EncodeRecord(core.Record{TimestampType: "unknown"})
	if err != nil {
		t.Fatal("size failure poisoned codec", err)
	}
	payload, _, _, err := readFrame(bytes.NewReader(data), 0, int64(len(data)), c.FrameLimit())
	if err != nil {
		t.Fatal("small record framing failed", err)
	}
	if _, err := c.DecodeRecord(payload); err != nil {
		t.Fatal("small record no longer decodes", err)
	}
}
