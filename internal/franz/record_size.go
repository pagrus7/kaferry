package franz

import (
	"encoding/binary"

	core "github.com/pagrus7/kaferry/internal/copy"
)

// A single-record v2 batch has a 61-byte header. Its timestamp/offset deltas
// are zero; the producer starts another batch when additional records won't fit.
func kafkaRecordBytes(r core.Record) int64 {
	n := int64(3) + nullableBytes(r.Key) + nullableBytes(r.Value) + varintBytes(int64(len(r.Headers)))
	for _, h := range r.Headers {
		n += varintBytes(int64(len(h.Name))) + int64(len(h.Name)) + nullableBytes(h.Value)
	}
	return 61 + varintBytes(n) + n
}
func nullableBytes(value []byte) int64 {
	if value == nil {
		return 1
	}
	return varintBytes(int64(len(value))) + int64(len(value))
}
func varintBytes(value int64) int64 {
	var encoded [binary.MaxVarintLen64]byte
	return int64(binary.PutVarint(encoded[:], value))
}

// kgo counts the enclosing bytes-field prefix against its batch ceiling.
// Before version negotiation it reserves the larger of legacy and compact prefixes.
func kafkaRecordBufferBytes(r core.Record) int64 {
	size := kafkaRecordBytes(r)
	var encoded [binary.MaxVarintLen64]byte
	prefix := int64(binary.PutUvarint(encoded[:], uint64(size+1)))
	return size + max(4, prefix)
}
