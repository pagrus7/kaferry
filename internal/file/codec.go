// Package file implements framed CBOR archives and durable local progress.
package file

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/fxamacker/cbor/v2"
	core "kafka-fwd2/internal/copy"
)

var magic = []byte{'K', 'F', 'W', 'D', '2', '\r', '\n', 0x1a}
var checksum = crc32.MakeTable(crc32.Castagnoli)

const headerLimit int64 = 65536

type Header struct {
	Version     uint64 `cbor:"version"`
	ID          string `cbor:"file_id"`
	Group       string `cbor:"group"`
	Fingerprint string `cbor:"fingerprint"`
}
type Codec struct {
	encoder    cbor.EncMode
	decoder    cbor.DecMode
	limit      int64
	maxHeaders int
}

func (c *Codec) FrameLimit() int64 { return c.limit }

func NewCodec(limit int64) (*Codec, error) {
	encoder, err := cbor.CanonicalEncOptions().EncMode()
	if err != nil {
		return nil, err
	}
	arrays := limit / 32
	if arrays < 16 {
		arrays = 16
	}
	if arrays > 2147483647 {
		arrays = 2147483647
	}
	decoder, err := (cbor.DecOptions{DupMapKey: cbor.DupMapKeyEnforcedAPF, MaxNestedLevels: 8, MaxArrayElements: int(arrays), MaxMapPairs: 16, ExtraReturnErrors: cbor.ExtraDecErrorUnknownField, IndefLength: cbor.IndefLengthForbidden, TagsMd: cbor.TagsForbidden}).DecMode()
	if err != nil {
		return nil, err
	}
	return &Codec{encoder: encoder, decoder: decoder, limit: limit, maxHeaders: int(arrays)}, nil
}

type FrameLimit struct{ Length, Limit int64 }

func (e *FrameLimit) Error() string {
	return fmt.Sprintf("frame length %d exceeds configured limit %d", e.Length, e.Limit)
}
func frame(payload []byte) []byte {
	data := make([]byte, len(payload)+8)
	binary.BigEndian.PutUint32(data, uint32(len(payload)))
	copy(data[4:], payload)
	binary.BigEndian.PutUint32(data[len(data)-4:], crc32.Checksum(data[:len(data)-4], checksum))
	return data
}
func readFrame(reader io.ReaderAt, position, size, limit int64) ([]byte, int64, uint32, error) {
	var prefix [4]byte
	if position < 0 || position > size {
		return nil, position, 0, fmt.Errorf("invalid frame position")
	}
	if size-position < 4 {
		return nil, position, 0, io.EOF
	}
	if err := readFrameBytes(reader, prefix[:], position); err != nil {
		return nil, position, 0, err
	}
	length := int64(binary.BigEndian.Uint32(prefix[:]))
	if length == 0 {
		return nil, position, 0, fmt.Errorf("frame length is empty")
	}
	if length+8 > size-position {
		return nil, position, 0, io.EOF
	}
	if length > limit {
		return nil, position, 0, &FrameLimit{Length: length, Limit: limit}
	}
	next := position + length + 8
	data := make([]byte, length+8)
	copy(data[:4], prefix[:])
	if err := readFrameBytes(reader, data[4:], position+4); err != nil {
		return nil, position, 0, err
	}
	crc := binary.BigEndian.Uint32(data[len(data)-4:])
	if crc32.Checksum(data[:len(data)-4], checksum) != crc {
		return nil, position, 0, fmt.Errorf("frame CRC32C mismatch at %d", position)
	}
	return data[4 : len(data)-4], next, crc, nil
}

func readFrameBytes(reader io.ReaderAt, data []byte, position int64) error {
	count, err := reader.ReadAt(data, position)
	if err == nil && count != len(data) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return conflict("source file changed: short read at %d", position)
	}
	return err
}
func (c *Codec) Header(reader io.ReaderAt, size int64) (Header, int64, error) {
	var header Header
	signature := make([]byte, len(magic))
	if _, err := reader.ReadAt(signature, 0); err != nil || string(signature) != string(magic) {
		return header, 0, fmt.Errorf("invalid archive magic")
	}
	payload, next, _, err := readFrame(reader, int64(len(magic)), size, headerLimit)
	if err != nil {
		return header, 0, fmt.Errorf("invalid archive header: %w", err)
	}
	if err = c.decoder.Unmarshal(payload, &header); err != nil {
		return header, 0, fmt.Errorf("invalid archive header CBOR")
	}
	if header.Version != 1 || header.ID == "" || header.Group == "" || header.Fingerprint == "" {
		return header, 0, fmt.Errorf("invalid or unsupported archive header")
	}
	return header, next, nil
}
func (c *Codec) EncodeHeader(header Header) ([]byte, error) {
	payload, err := c.encoder.Marshal(header)
	if err != nil {
		return nil, err
	}
	if int64(len(payload)) > headerLimit {
		return nil, fmt.Errorf("archive header too large")
	}
	return append(append([]byte{}, magic...), frame(payload)...), nil
}

type wireHeader struct {
	Name  []byte `cbor:"name"`
	Value []byte `cbor:"value"`
}

type wireProvenance struct {
	Topic     string `cbor:"topic"`
	Partition int32  `cbor:"partition"`
	Offset    int64  `cbor:"offset"`
}

type decodedHeader struct {
	Name  cbor.RawMessage `cbor:"name"`
	Value cbor.RawMessage `cbor:"value"`
}

func (c *Codec) EncodeRecord(record core.Record) ([]byte, error) {
	if len(record.Headers) > c.maxHeaders {
		return nil, fmt.Errorf("record exceeds configured header element limit")
	}
	headers := make([]wireHeader, len(record.Headers))
	for i, h := range record.Headers {
		headers[i] = wireHeader{[]byte(h.Name), h.Value}
	}
	values := map[string]any{"key": record.Key, "value": record.Value, "headers": headers, "timestamp_ms": record.TimestampMS, "timestamp_type": record.TimestampType}
	if record.Source != nil {
		values["source"] = &wireProvenance{Topic: record.Source.Topic, Partition: record.Source.Partition, Offset: record.Source.Offset}
	}
	payload, err := c.encoder.Marshal(values)
	if err != nil {
		return nil, err
	}
	if int64(len(payload)) > c.limit {
		return nil, fmt.Errorf("record exceeds configured frame limit")
	}
	return frame(payload), nil
}
func (c *Codec) DecodeRecord(payload []byte) (core.Record, error) {
	var record core.Record
	var fields map[string]cbor.RawMessage
	if err := c.decoder.Unmarshal(payload, &fields); err != nil {
		return record, fmt.Errorf("invalid record CBOR")
	}
	headers, err := c.recordFields(fields, &record)
	if err != nil {
		return record, err
	}
	if raw, present := fields["source"]; present {
		var source *wireProvenance
		if err := c.decoder.Unmarshal(raw, &source); err != nil {
			return record, fmt.Errorf("invalid record provenance")
		}
		if source != nil {
			record.Source = &core.Provenance{Topic: source.Topic, Partition: source.Partition, Offset: source.Offset}
		}
	}
	switch record.TimestampType {
	case "create_time", "log_append_time", "unknown":
	default:
		return record, fmt.Errorf("invalid timestamp type")
	}
	for _, header := range headers {
		decoded, err := c.decodeHeader(header)
		if err != nil {
			return record, err
		}
		record.Headers = append(record.Headers, decoded)
	}
	return record, nil
}

func (c *Codec) recordFields(fields map[string]cbor.RawMessage, record *core.Record) ([]decodedHeader, error) {
	var headers []decodedHeader
	required := map[string]any{"key": &record.Key, "value": &record.Value, "headers": &headers, "timestamp_ms": &record.TimestampMS, "timestamp_type": &record.TimestampType}
	for key, target := range required {
		raw, present := fields[key]
		if !present {
			return nil, fmt.Errorf("record missing %s", key)
		}
		if len(raw) == 1 && (raw[0] == 0xf7 || key == "headers" && raw[0] == 0xf6) {
			return nil, fmt.Errorf("invalid record field %s", key)
		}
		if err := c.decoder.Unmarshal(raw, target); err != nil {
			return nil, fmt.Errorf("invalid record field %s", key)
		}
	}
	for key := range fields {
		if _, ok := required[key]; !ok && key != "source" {
			return nil, fmt.Errorf("unknown record field")
		}
	}
	return headers, nil
}

func (c *Codec) decodeHeader(header decodedHeader) (core.Header, error) {
	var name, value []byte
	if !byteString(header.Name) || !(byteString(header.Value) || len(header.Value) == 1 && header.Value[0] == 0xf6) {
		return core.Header{}, fmt.Errorf("header requires byte-string name and nullable byte-string value")
	}
	if err := c.decoder.Unmarshal(header.Name, &name); err != nil {
		return core.Header{}, fmt.Errorf("invalid header name")
	}
	if err := c.decoder.Unmarshal(header.Value, &value); err != nil {
		return core.Header{}, fmt.Errorf("invalid header value")
	}
	return core.Header{Name: string(name), Value: value}, nil
}

func byteString(raw cbor.RawMessage) bool { return len(raw) > 0 && raw[0]>>5 == 2 }
