package avro

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"

	"github.com/iskorotkov/avro/v2/ocf"
)

type countingWriter struct {
	io.Writer
	bytes int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	w.bytes += int64(n)
	if n != len(p) && err == nil {
		err = io.ErrShortWrite
	}
	return n, err
}

type Writer struct {
	codec      *Codec
	identity   Identity
	output     *countingWriter
	pending    []byte
	blockCount int64
	sync       [16]byte
	last       int64
	count      int64
	failed     error
}

func (c *Codec) NewWriter(output io.Writer, identity Identity) (*Writer, error) {
	if err := identity.validate(); err != nil {
		return nil, err
	}
	meta, _ := json.Marshal(identity)
	w := &Writer{codec: c, identity: identity, output: &countingWriter{Writer: output}, last: identity.FirstOffset}
	_, _ = rand.Read(w.sync[:])
	header := ocf.Header{Magic: [4]byte{'O', 'b', 'j', 1}, Sync: w.sync, Meta: map[string][]byte{
		"avro.schema": []byte(schemaText), "avro.codec": []byte("snappy"), "kaferry": meta,
	}}
	data, err := c.api.Marshal(ocf.HeaderSchema, header)
	if err != nil {
		return nil, err
	}
	if _, err = w.output.Write(data); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *Writer) Append(record Record) error {
	if w.failed != nil {
		return w.failed
	}
	if err := record.validate(w.identity.Source, w.codec.maxHeaders); err != nil {
		return &InvalidRecord{Err: err}
	}
	if record.Offset < w.last || w.count == 0 && record.Offset != w.identity.FirstOffset {
		return &InvalidRecord{Err: errors.New("source offset decreased or disagrees with file header")}
	}
	if recordBytes(record) > w.codec.limit {
		return &InvalidRecord{Err: errors.New("record exceeds Avro block limit")}
	}
	// Finish encoding before changing the container: a rejected record is safe to skip.
	data, err := w.codec.api.Marshal(w.codec.schema, record)
	if err != nil {
		return &InvalidRecord{Err: err}
	}
	if int64(len(data)) > w.codec.limit {
		return &InvalidRecord{Err: errors.New("record exceeds Avro block limit")}
	}
	if int64(len(w.pending))+int64(len(data)) > w.codec.limit {
		if err := w.Flush(); err != nil {
			return err
		}
	}
	w.pending = append(w.pending, data...)
	w.blockCount++
	w.last, w.count = record.Offset, w.count+1
	if len(w.pending) >= BlockTarget || w.blockCount >= maxRecords {
		return w.Flush()
	}
	return nil
}

func recordBytes(record Record) int64 {
	size := int64(len(record.Key)) + int64(len(record.Value))
	for _, header := range record.Headers {
		size += int64(len(header.Key)) + int64(len(header.Value))
	}
	return size
}

// Flush emits complete blocks; the storage component must sync them before acknowledgement.
func (w *Writer) Flush() error {
	if w.failed != nil {
		return w.failed
	}
	if w.blockCount == 0 {
		return nil
	}
	compressed := (&ocf.SnappyCodec{}).Encode(w.pending)
	var prefix [20]byte
	n := binary.PutVarint(prefix[:], w.blockCount)
	n += binary.PutVarint(prefix[n:], int64(len(compressed)))
	for _, data := range [][]byte{prefix[:n], compressed, w.sync[:]} {
		if _, w.failed = w.output.Write(data); w.failed != nil {
			break
		}
	}
	// Release each partition's block capacity after the batch; idle partitions retain no payload buffers.
	w.pending, w.blockCount = nil, 0
	return w.failed
}

// Size is the emitted compressed container size, excluding the pending raw block.
func (w *Writer) Size() int64  { return w.output.bytes }
func (w *Writer) Count() int64 { return w.count }
