// Package avro stores Kafka records in independently readable Avro OCF blocks.
package avro

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	Cluster   string   `avro:"cluster" json:"cluster"`
	Topic     string   `avro:"topic" json:"topic"`
	TopicID   [16]byte `avro:"topic_id" json:"topic_id"`
	Partition int32    `avro:"partition" json:"partition"`
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
}

type Codec struct {
	api        lib.API
	schema     lib.Schema
	limit      int64
	maxHeaders int
}

func New(limit int64) (*Codec, error) {
	if limit < 1 || limit > 1<<32-1 || snappy.MaxEncodedLen(int(limit)) < 0 {
		return nil, errors.New("Avro block limit must be positive and fit Snappy size limits")
	}
	headers := max(16, int(limit/32))
	api := lib.Config{MaxByteSliceSize: int(limit), MaxSliceAllocSize: headers, MaxMapAllocSize: 16}.Freeze()
	return &Codec{api: api, schema: recordSchema, limit: limit, maxHeaders: headers}, nil
}

// Schema returns the published record schema for standard Avro tooling.
func Schema() string { return schemaText }

// InvalidRecord identifies input rejected before any container bytes are changed.
type InvalidRecord struct{ Err error }

func (e *InvalidRecord) Error() string { return e.Err.Error() }
func (e *InvalidRecord) Unwrap() error { return e.Err }

func (s Source) validate() error {
	if s.Cluster == "" || len(s.Cluster) > 4096 || s.Topic == "" || len(s.Topic) > 249 || !utf8.ValidString(s.Cluster+s.Topic) || s.TopicID == [16]byte{} || s.Partition < 0 {
		return errors.New("invalid original source identity")
	}
	return nil
}

func (i Identity) validate() error {
	if i.Version != Version || !validID(i.WriterID) || !validID(i.FileID) || i.FirstOffset < 0 {
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

func (r Record) validate(source Source, maxHeaders int) error {
	if r.Source != source || r.Offset < 0 {
		return errors.New("record does not belong to this source partition")
	}
	if len(r.Headers) > maxHeaders {
		return errors.New("record exceeds header count limit")
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
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&identity); err != nil {
		return identity, fmt.Errorf("invalid Avro identity: %w", err)
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return identity, errors.New("trailing Avro identity data")
	}
	return identity, identity.validate()
}
