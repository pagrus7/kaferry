package avro

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"

	lib "github.com/iskorotkov/avro/v2"
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
	reserve    int
	datum      *lib.Writer
	blockCount int64
	sync       [16]byte
	last       int64
	count      int64
	failed     error
}

func (c *Codec) NewWriter(output io.Writer, identity Identity) (*Writer, error) {
	identity.BlockLimit = c.limit
	if err := identity.Validate(); err != nil {
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
	if err := record.validate(); err != nil {
		return err
	}
	if record.Source != w.identity.Source {
		return ErrRotationRequired
	}
	if w.count == 0 && record.Offset != w.identity.FirstOffset {
		return errors.New("first source offset disagrees with file header")
	}
	if record.Offset < w.last {
		return ErrRotationRequired
	}
	if len(record.Headers) > w.codec.maxHeaders {
		return &InvalidRecord{Err: errors.New("record exceeds header count limit")}
	}
	if recordBytes(record) > w.codec.limit {
		return &InvalidRecord{Err: errors.New("record exceeds Avro block limit")}
	}
	// Finish encoding before changing the container; format-limit rejections leave valid pending data intact.
	if w.datum == nil {
		w.datum = lib.NewWriter(nil, 0, lib.WithWriterConfig(w.codec.api))
	}
	w.datum.Reset(nil)
	w.datum.Error = nil
	w.datum.WriteVal(w.codec.schema, record)
	if err := w.datum.Error; err != nil {
		w.datum = nil
		return err
	}
	data := w.datum.Buffer()
	if int64(len(data)) > w.codec.limit {
		w.datum = nil
		return &InvalidRecord{Err: errors.New("record exceeds Avro block limit")}
	}
	if int64(len(w.pending))+int64(len(data)) > min(BlockTarget, w.codec.limit) {
		if err := w.Flush(); err != nil {
			return err
		}
	}
	if w.pending == nil {
		w.pending = make([]byte, 0, max(w.reserve, len(data)))
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
	// Keep only a bounded size hint, so sparse writers do not reserve full blocks.
	w.reserve = min(min(len(w.pending), BlockTarget/2)*2, int(w.codec.limit))
	// Release each partition's block capacity after the batch; idle partitions retain no payload buffers.
	w.pending, w.blockCount, w.datum = nil, 0, nil
	return w.failed
}

// Size is the emitted compressed container size, excluding the pending raw block.
func (w *Writer) Size() int64        { return w.output.bytes }
func (w *Writer) Count() int64       { return w.count }
func (w *Writer) Identity() Identity { return w.identity }
