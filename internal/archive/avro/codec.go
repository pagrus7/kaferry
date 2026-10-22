// Package avro stores Kafka records in independently readable Avro OCF blocks.
package avro

import (
	"bytes"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/golang/snappy"
	lib "github.com/iskorotkov/avro/v2"
)

//go:embed schema.json
var schemaText string

var recordSchema = lib.MustParse(schemaText)

const (
	Version        = 1
	BlockTarget    = 256 << 10
	maxHeaderBytes = 64 << 10
	maxRecords     = 1_000_000
)

type Source struct {
	Cluster   string  `avro:"cluster" json:"cluster"`
	Topic     string  `avro:"topic" json:"topic"`
	TopicID   TopicID `avro:"topic_id" json:"topic_id"`
	Partition int32   `avro:"partition" json:"partition"`
}

// TopicID is fixed binary data in Avro and lowercase hexadecimal text in JSON.
type TopicID [16]byte

func (id TopicID) MarshalText() ([]byte, error) {
	text := make([]byte, 32)
	hex.Encode(text, id[:])
	return text, nil
}

func (id *TopicID) UnmarshalText(text []byte) error {
	if !validID(string(text)) {
		return errors.New("topic ID requires 32 lowercase hexadecimal characters")
	}
	_, _ = hex.Decode(id[:], text)
	return nil
}

type Header struct {
	Key   []byte `avro:"key"`
	Value []byte `avro:"value"`
}

type Record struct {
	Source        Source   `avro:"source"`
	Offset        int64    `avro:"offset"`
	TimestampMS   *int64   `avro:"timestamp_ms"`
	TimestampKind string   `avro:"timestamp_kind"`
	Key           []byte   `avro:"key"`
	Value         []byte   `avro:"value"`
	Headers       []Header `avro:"headers"`
}

type Identity struct {
	Version     int    `json:"version"`
	WriterID    string `json:"writer_id"`
	FileID      string `json:"file_id"`
	Source      Source `json:"source"`
	FirstOffset int64  `json:"first_offset"`
	BlockLimit  int64  `json:"block_limit"`
}

type Codec struct {
	api        lib.API
	schema     lib.Schema
	limit      int64
	maxHeaders int
}

func New(limit int64) (*Codec, error) {
	if !validLimit(limit) {
		return nil, errors.New("archive block limit must be positive and fit Snappy size limits")
	}
	return newCodec(limit), nil
}

func newCodec(limit int64) *Codec {
	headers := max(16, int(limit/32))
	api := lib.Config{MaxByteSliceSize: int(limit), MaxSliceAllocSize: headers, MaxMapAllocSize: 16}.Freeze()
	return &Codec{api: api, schema: recordSchema, limit: limit, maxHeaders: headers}
}

func validLimit(limit int64) bool {
	return limit > 0 && limit <= 1<<32-1 && snappy.MaxEncodedLen(int(limit)) >= 0
}

// Schema returns the published record schema for standard Avro tooling.
func Schema() string { return schemaText }

// InvalidRecord identifies Append input rejected by format limits before output changes.
type InvalidRecord struct{ Err error }

func (e *InvalidRecord) Error() string { return e.Err.Error() }
func (e *InvalidRecord) Unwrap() error { return e.Err }

// ErrRotationRequired means Append needs a new file for this source or offset.
var ErrRotationRequired = errors.New("source identity changed or offset decreased; start another file")

type LimitError struct{ Required, Allowed int64 }

func (e *LimitError) Error() string {
	return fmt.Sprintf("archive needs a block limit of %d bytes; configured maximum is %d bytes", e.Required, e.Allowed)
}

func (s Source) validate() error {
	if s.Cluster == "" || len(s.Cluster) > 4096 || s.Topic == "" || len(s.Topic) > 249 || !utf8.ValidString(s.Cluster) || !utf8.ValidString(s.Topic) || s.TopicID == [16]byte{} || s.Partition < 0 {
		return errors.New("invalid original source identity")
	}
	return nil
}

func (i Identity) validate() error {
	if i.Version != Version || !validID(i.WriterID) || !validID(i.FileID) || i.FirstOffset < 0 || !validLimit(i.BlockLimit) {
		return errors.New("invalid or unsupported Avro file identity")
	}
	return i.Source.validate()
}

func validID(value string) bool {
	if len(value) != 32 {
		return false
	}
	for _, r := range value {
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') {
			return false
		}
	}
	return true
}

func (r Record) validate() error {
	if r.Offset < 0 {
		return errors.New("invalid original source offset")
	}
	switch r.TimestampKind {
	case "unknown", "create_time", "log_append_time":
	default:
		return errors.New("invalid record timestamp kind")
	}
	return nil
}

func parseIdentity(data []byte) (Identity, error) {
	var identity Identity
	if err := json.Unmarshal(data, &identity); err != nil {
		return identity, fmt.Errorf("invalid Avro identity: %w", err)
	}
	canonical, _ := json.Marshal(identity)
	if !bytes.Equal(data, canonical) {
		return identity, errors.New("archive identity JSON is not canonical")
	}
	return identity, identity.validate()
}
