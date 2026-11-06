//go:build integration && load

package compat

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

type integrityEvidence struct {
	Algorithm        string `json:"algorithm"`
	SubmittedRecords int    `json:"submitted_records"`
	ReceivedRecords  int    `json:"received_records"`
	SubmittedSHA256  string `json:"submitted_sha256"`
	ReceivedSHA256   string `json:"received_sha256"`
	PendingRecords   int    `json:"pending_records"`
	Matched          *bool  `json:"matched"`
}

// streamIntegrity hashes actual submissions and unique receipts in logical-ID
// order within each source partition. Only out-of-order receipt digests wait.
type streamIntegrity struct {
	submitted, received           []hash.Hash
	submittedCount, receivedCount int
	next                          []int
	pending                       map[int][sha256.Size]byte
	maxPending                    int
}

func newStreamIntegrity() *streamIntegrity {
	return newPartitionedStreamIntegrity(4)
}

func newPartitionedStreamIntegrity(partitions int) *streamIntegrity {
	s := &streamIntegrity{maxPending: 8192, next: make([]int, partitions), submitted: make([]hash.Hash, partitions), received: make([]hash.Hash, partitions), pending: make(map[int][sha256.Size]byte)}
	for partition := range partitions {
		s.next[partition] = partition
		s.submitted[partition], s.received[partition] = sha256.New(), sha256.New()
	}
	return s
}

func recordFingerprint(key, value []byte, headers []kgo.RecordHeader) [sha256.Size]byte {
	h := sha256.New()
	h.Write([]byte("kaferry-record-v1\x00"))
	var size [8]byte
	field := func(value []byte) {
		length := uint64(len(value))
		if value == nil {
			length = ^uint64(0)
		}
		binary.BigEndian.PutUint64(size[:], length)
		h.Write(size[:])
		h.Write(value)
	}
	field(key)
	field(value)
	binary.BigEndian.PutUint64(size[:], uint64(len(headers)))
	h.Write(size[:])
	for _, header := range headers {
		field([]byte(header.Key))
		field(header.Value)
	}
	return [sha256.Size]byte(h.Sum(nil))
}

func (s *streamIntegrity) submit(id int, record *kgo.Record) {
	digest := recordFingerprint(record.Key, record.Value, record.Headers)
	s.submitted[id%len(s.next)].Write(digest[:])
	s.submittedCount++
}

func (s *streamIntegrity) receive(t *testing.T, id int, key, value []byte, headers []kgo.RecordHeader) {
	t.Helper()
	s.pending[id] = recordFingerprint(key, value, headers)
	s.receivedCount++
	partition := id % len(s.next)
	for {
		digest, exists := s.pending[s.next[partition]]
		if !exists {
			break
		}
		s.received[partition].Write(digest[:])
		delete(s.pending, s.next[partition])
		s.next[partition] += len(s.next)
	}
	require.LessOrEqual(t, len(s.pending), s.maxPending, "integrity gaps exceeded the bounded ID window; fingerprints are partial")
}

func (s *streamIntegrity) snapshot(complete bool, submitted, received int) integrityEvidence {
	root := func(partitions []hash.Hash) string {
		h := sha256.New()
		h.Write([]byte("kaferry-stream-v1\x00"))
		for _, partition := range partitions {
			h.Write(partition.Sum(nil))
		}
		return fmt.Sprintf("%x", h.Sum(nil))
	}
	r := integrityEvidence{Algorithm: "sha256-canonical-records-v1", SubmittedRecords: s.submittedCount,
		ReceivedRecords: s.receivedCount, SubmittedSHA256: root(s.submitted), ReceivedSHA256: root(s.received), PendingRecords: len(s.pending)}
	if complete {
		matched := r.SubmittedRecords == submitted && r.ReceivedRecords == received && r.PendingRecords == 0 && r.SubmittedSHA256 == r.ReceivedSHA256
		r.Matched = &matched
	}
	return r
}
