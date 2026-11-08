//go:build integration && load

package compat

import (
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"os"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kgo"
)

const workloadIDHeader = "kaferry-fixture-id"

// The versioned name and parameters travel with every load/soak measurement.
type workloadProfile struct {
	Name                 string     `json:"name"`
	SourceCompression    string     `json:"source_compression"`
	SourceBufferRecords  int        `json:"source_buffer_records,omitempty"`
	ValueBytes           [2]int     `json:"value_bytes_min_max"`
	ValueEntropy         string     `json:"value_entropy"`
	KeyCount             int        `json:"key_count"`
	NullKeyEvery         int        `json:"null_key_every"`
	Zipf                 [2]float64 `json:"zipf_s_v,omitempty"`
	Seed                 uint64     `json:"seed"`
	HeaderCount          int        `json:"header_count"`
	HeaderValueBytes     int        `json:"heavy_header_value_bytes,omitempty"`
	TombstoneEvery       int        `json:"tombstone_every,omitempty"`
	TopicMaxMessageBytes int        `json:"topic_max_message_bytes,omitempty"`
	CompactionLagMS      int        `json:"min_compaction_lag_ms,omitempty"`
}

func defaultWorkloadProfile() workloadProfile {
	entropy := "repeated-byte"
	if os.Getenv("KAFERRY_ARCHIVE_PAYLOAD") == "random" {
		entropy = "PCG seeded by record ID and 42"
	}
	return workloadProfile{Name: "cyclic-v1", SourceCompression: "none", ValueBytes: [2]int{1024, 5119},
		ValueEntropy: entropy, KeyCount: 257, NullKeyEvery: 7, Seed: 42, HeaderCount: 3}
}

func loadWorkloadProfiles() []workloadProfile {
	large := defaultWorkloadProfile()
	large.Name, large.ValueBytes = "near-limit-v1", [2]int{999000, 999127}
	large.TopicMaxMessageBytes, large.ValueEntropy = 1000000, "PCG seeded by record ID and 42"
	hot := defaultWorkloadProfile()
	hot.Name, hot.NullKeyEvery, hot.Zipf = "hot-keys-v1", 0, [2]float64{1.25, 1}
	headers := defaultWorkloadProfile()
	headers.Name, headers.HeaderCount, headers.HeaderValueBytes = "headers-v1", 64, 256
	tombstones := defaultWorkloadProfile()
	tombstones.Name, tombstones.KeyCount, tombstones.NullKeyEvery = "tombstones-v1", 32, 0
	tombstones.HeaderCount, tombstones.TombstoneEvery, tombstones.CompactionLagMS = 4, 5, 600000
	profiles := []workloadProfile{large, hot, headers, tombstones}
	for _, codec := range []string{"none", "gzip", "snappy", "lz4", "zstd"} {
		profile := defaultWorkloadProfile()
		profile.Name, profile.SourceCompression = "source-"+codec+"-v1", codec
		profile.ValueEntropy = "repeated-byte"
		profiles = append(profiles, profile)
	}
	for i := range profiles {
		profiles[i].SourceBufferRecords = 64
	}
	return profiles
}

func (p workloadProfile) compression() kgo.CompressionCodec {
	switch p.SourceCompression {
	case "gzip":
		return kgo.GzipCompression()
	case "snappy":
		return kgo.SnappyCompression()
	case "lz4":
		return kgo.Lz4Compression()
	case "zstd":
		return kgo.ZstdCompression()
	default:
		return kgo.NoCompression()
	}
}

func (s *streamRecords) record(id int) *kgo.Record {
	input := archiveLoadRecordPartitions(id, len(s.receipts.next))
	if p := s.profile; p != nil {
		input = archiveLoadRecordPayload(id, len(s.receipts.next), p.ValueBytes, p.ValueEntropy != "repeated-byte", p.Seed)
	}
	message := &kgo.Record{Topic: s.topic, Partition: input.Source.Partition, Key: input.Key,
		Value: input.Value, Timestamp: s.stamp, Headers: s.headers}
	if s.profile == nil {
		return message
	}
	p := s.profile
	key := id % p.KeyCount
	if p.Zipf[0] != 0 {
		key = int(rand.NewZipf(rand.New(rand.NewPCG(uint64(id), p.Seed)), p.Zipf[0], p.Zipf[1], uint64(p.KeyCount-1)).Uint64())
	}
	message.Key = []byte(fmt.Sprintf("key-%d", key))
	if p.NullKeyEvery > 0 && id%p.NullKeyEvery == 0 {
		message.Key = nil
	}
	if p.HeaderValueBytes != 0 {
		message.Headers = make([]kgo.RecordHeader, p.HeaderCount)
		for i := range message.Headers {
			value := make([]byte, p.HeaderValueBytes)
			for j := range value {
				value[j] = byte(id + i + j)
			}
			message.Headers[i] = kgo.RecordHeader{Key: fmt.Sprintf("header-%d", i%4), Value: value}
		}
		message.Headers[0].Value, message.Headers[1].Value = nil, []byte{}
	}
	if p.TombstoneEvery != 0 {
		idBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(idBytes, uint32(id))
		message.Headers = append(append([]kgo.RecordHeader(nil), message.Headers...), kgo.RecordHeader{Key: workloadIDHeader, Value: idBytes})
		if id%p.TombstoneEvery == 0 {
			message.Value = nil
		}
	}
	return message
}

func (s *streamRecords) recordID(value []byte, headers []kgo.RecordHeader) (int, bool) {
	if s.profile != nil && s.profile.TombstoneEvery != 0 {
		for _, header := range headers {
			if header.Key == workloadIDHeader {
				if len(header.Value) != 4 {
					return 0, false
				}
				return int(binary.BigEndian.Uint32(header.Value)), true
			}
		}
		return 0, false
	}
	if len(value) < 4 {
		return 0, false
	}
	return int(binary.BigEndian.Uint32(value)), true
}

// Count the codecs actually written by the source fixture, rather than just its configuration.
type sourceCompressionCounts [5]atomic.Int64

func (c *sourceCompressionCounts) OnProduceBatchWritten(_ kgo.BrokerMetadata, _ string, _ int32, batch kgo.ProduceBatchMetrics) {
	if int(batch.CompressionType) < len(c) {
		c[batch.CompressionType].Add(int64(batch.NumRecords))
	}
}

func (c *sourceCompressionCounts) snapshot() map[string]int64 {
	result := make(map[string]int64)
	for i, name := range []string{"none", "gzip", "snappy", "lz4", "zstd"} {
		if count := c[i].Load(); count != 0 {
			result[name] = count
		}
	}
	return result
}
