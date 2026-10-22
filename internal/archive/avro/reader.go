package avro

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/golang/snappy"
	lib "github.com/iskorotkov/avro/v2"
	"github.com/iskorotkov/avro/v2/ocf"
)

type countingReader struct {
	io.Reader
	bytes int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.bytes += int64(n)
	return n, err
}

func (r *countingReader) ReadByte() (byte, error) {
	var b [1]byte
	_, err := io.ReadFull(r, b[:])
	return b[0], err
}

type Reader struct {
	codec    *Codec
	input    io.ReaderAt
	size     int64
	Identity Identity
	Start    int64
	sync     [16]byte
}

func (c *Codec) Open(input io.ReaderAt, size int64) (*Reader, error) {
	r := &Reader{codec: c, input: input, size: size}
	stream := &countingReader{Reader: io.NewSectionReader(input, 0, min(size, maxHeaderBytes))}
	config := lib.Config{ReadBufSize: 1, MaxByteSliceSize: maxHeaderBytes, MaxSliceAllocSize: 16, MaxMapAllocSize: 16}.Freeze()
	decoder := lib.NewReader(stream, 1, lib.WithReaderConfig(config))
	var header ocf.Header
	decoder.ReadVal(ocf.HeaderSchema, &header)
	if decoder.Error != nil {
		return nil, fmt.Errorf("archive header: %w", incomplete(decoder.Error))
	}
	if header.Magic != [4]byte{'O', 'b', 'j', 1} || string(header.Meta["avro.codec"]) != "snappy" {
		return nil, errors.New("archive requires Snappy Avro OCF")
	}
	schema, err := lib.ParseWithCache(string(header.Meta["avro.schema"]), "", &lib.SchemaCache{})
	if err != nil {
		return nil, errors.New("invalid Avro schema")
	}
	if schema.Fingerprint() != c.schema.Fingerprint() {
		return nil, errors.New("unsupported archive record schema")
	}
	identity, err := parseIdentity(header.Meta["kaferry"])
	if err != nil {
		return nil, err
	}
	if identity.BlockLimit > c.limit {
		return nil, &LimitError{Required: identity.BlockLimit, Allowed: c.limit}
	}
	if identity.BlockLimit != c.limit {
		r.codec = newCodec(identity.BlockLimit)
	}
	r.Identity, r.Start, r.sync = identity, stream.bytes, header.Sync
	return r, nil
}

// Block holds one decompressed block. Reopening at Start avoids preceding blocks.
type Block struct {
	Start, End int64
	Count      int64
	index      int64
	last       int64
	reader     *lib.Reader
	codec      *Codec
	identity   Identity
	first      bool
	err        error
}

func (r *Reader) Block(position int64) (*Block, error) {
	if position == r.size {
		return nil, io.EOF
	}
	if position < r.Start || position > r.size {
		return nil, errors.New("archive block position is outside the file")
	}
	input := &countingReader{Reader: io.NewSectionReader(r.input, position, r.size-position)}
	count, err := binary.ReadVarint(input)
	if err != nil {
		return nil, fmt.Errorf("archive block count: %w", incomplete(err))
	}
	if count < 1 || count > maxRecords {
		return nil, errors.New("invalid Avro block record count")
	}
	size, err := binary.ReadVarint(input)
	if err != nil {
		return nil, fmt.Errorf("archive block length: %w", incomplete(err))
	}
	// A Snappy block includes a four-byte checksum; bound compressed allocation too.
	if size < 5 || size > int64(snappy.MaxEncodedLen(int(r.codec.limit)))+4 || size > r.size-position-input.bytes-16 {
		return nil, errors.New("invalid or incomplete Avro block length")
	}
	compressed := make([]byte, int(size))
	if _, err := io.ReadFull(input, compressed); err != nil {
		return nil, fmt.Errorf("archive block data: %w", incomplete(err))
	}
	var sync [16]byte
	if _, err := io.ReadFull(input, sync[:]); err != nil {
		return nil, fmt.Errorf("archive block sync: %w", incomplete(err))
	}
	if sync != r.sync {
		return nil, errors.New("archive block sync mismatch")
	}
	end := position + input.bytes
	raw, err := r.codec.decompress(compressed)
	if err != nil {
		return nil, err
	}
	if count > int64(len(raw)) {
		return nil, errors.New("archive record count exceeds block data")
	}
	decoder := lib.NewReader(nil, 0, lib.WithReaderConfig(r.codec.api)).Reset(raw)
	return &Block{Start: position, End: end, Count: count, last: -1, reader: decoder, codec: r.codec, identity: r.Identity, first: position == r.Start}, nil
}

func (c *Codec) decompress(compressed []byte) ([]byte, error) {
	rawSize, err := snappy.DecodedLen(compressed[:len(compressed)-4])
	if err != nil || int64(rawSize) > c.limit {
		return nil, errors.New("invalid or oversized decoded Avro block")
	}
	return (&ocf.SnappyCodec{}).Decode(compressed)
}

func incomplete(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}

func (b *Block) Next() (Record, error) {
	var record Record
	if b.err != nil {
		return record, b.err
	}
	if b.index == b.Count {
		return record, io.EOF
	}
	b.reader.ReadVal(b.codec.schema, &record)
	if b.reader.Error != nil {
		b.err = fmt.Errorf("archive record: %w", incomplete(b.reader.Error))
		return record, b.err
	}
	b.err = record.validate()
	if b.err == nil && record.Source != b.identity.Source {
		b.err = errors.New("record source disagrees with Avro header")
	}
	if b.err == nil && b.first && b.index == 0 && record.Offset != b.identity.FirstOffset {
		b.err = errors.New("first source offset disagrees with Avro header")
	}
	if b.err == nil && (record.Offset < b.last || record.Offset < b.identity.FirstOffset) {
		b.err = errors.New("source offset decreased within Avro file")
	}
	if b.err == nil && b.index == b.Count-1 {
		b.reader.Peek()
		if !errors.Is(b.reader.Error, io.EOF) {
			b.err = errors.New("archive record count does not consume block exactly")
		}
	}
	if b.err != nil {
		return Record{}, b.err
	}
	b.last, b.index = record.Offset, b.index+1
	return record, nil
}
