//go:build integration && harness

package compat

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	core "github.com/pagrus7/kaferry/src/internal/copy"
	"github.com/stretchr/testify/require"
)

func TestArchiveFixturePayloadParametersPreserveRecordContract(t *testing.T) {
	for _, test := range []struct {
		name       string
		id         int
		partitions int
		bounds     [2]int
		size       int
	}{
		{"default minimum", 0, 4, [2]int{1024, 5119}, 1024},
		{"default maximum", 4095, 4, [2]int{1024, 5119}, 5119},
		{"default wrap", 4096, 4, [2]int{1024, 5119}, 1024},
		{"null key", 7, 4, [2]int{1024, 5119}, 1031},
		{"near-limit minimum", 0, 24, [2]int{999000, 999127}, 999000},
		{"near-limit maximum", 127, 24, [2]int{999000, 999127}, 999127},
		{"near-limit wrap", 128, 24, [2]int{999000, 999127}, 999000},
		{"fixed size and cyclic key", 257, 3, [2]int{16, 16}, 16},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Given explicit payload bounds and a source partition count.
			for _, randomPayload := range []bool{false, true} {
				// When generating repeated or seeded bytes with the same record identity.
				record := archiveLoadRecordPayload(test.id, test.partitions, test.bounds, randomPayload, 42)
				// Then both endpoints and wrapping retain exact sizes, IDs and deterministic bytes.
				require.Len(t, record.Value, test.size)
				require.EqualValues(t, test.id, binary.BigEndian.Uint32(record.Value))
				repeatedRecord := archiveLoadRecordPayload(test.id, test.partitions, test.bounds, randomPayload, 42)
				require.True(t, bytes.Equal(record.Value, repeatedRecord.Value), "payload generation must be deterministic")
				repeated := bytes.Repeat([]byte{byte(test.id % 251)}, test.size-4)
				if randomPayload {
					require.False(t, bytes.Equal(repeated, record.Value[4:]), "seeded payload must change the repeated-byte input")
				} else {
					require.True(t, bytes.Equal(repeated, record.Value[4:]))
				}
				key := []byte(fmt.Sprintf("key-%d", test.id%257))
				if test.id%7 == 0 {
					key = nil
				}
				record.Value = nil // Payload bytes are checked separately from the logical record metadata.
				require.Equal(t, core.Record{Key: key,
					TimestampMS: &archiveFixtureTimestamp, TimestampType: "create_time",
					Source: &core.Provenance{Cluster: "fixture", TopicID: [16]byte{1}, Topic: "orders",
						Partition: int32(test.id % test.partitions), Offset: int64(test.id / test.partitions)},
					Headers: []core.Header{{Name: "trace", Value: []byte{0, 255}}, {Name: "dup", Value: nil}, {Name: "dup", Value: []byte{}}}}, record)
			}
		})
	}
}

func TestArchiveFixtureSeededPayloadBytes(t *testing.T) {
	// Given the PCG byte sequence identified by record ID and the reported seed.
	for _, test := range []struct {
		seed  uint64
		value []byte
	}{
		{42, []byte{0, 0, 1, 1, 0xde, 0x34, 0x82, 0xc9, 0x85, 0xff, 0x23, 0x85, 0x4c, 0x45, 0x70, 0x5b}},
		{43, []byte{0, 0, 1, 1, 0x20, 0x3c, 0xde, 0x2a, 0x15, 0x5c, 0x13, 0x35, 0x99, 0xc0, 0xdb, 0x66}},
	} {
		// When generating a fixed-length value, then the exact bytes include the big-endian record ID.
		record := archiveLoadRecordPayload(257, 3, [2]int{16, 16}, true, test.seed)
		require.Equal(t, test.value, record.Value)
	}
}

func TestArchiveFixtureDefaultWrappersRetainPayload(t *testing.T) {
	for _, entropy := range []string{"", "random"} {
		t.Run("payload="+entropy, func(t *testing.T) {
			// Given either supported default entropy configuration.
			t.Setenv("KAFERRY_ARCHIVE_PAYLOAD", entropy)
			for _, id := range []int{0, 7, 257, 4095, 4096} {
				// When using the four-partition or explicit-partition wrapper.
				defaultRecord := archiveLoadRecord(id)
				partitionedRecord := archiveLoadRecordPartitions(id, 24)
				// Then defaults retain their complete logical record and seeded payload.
				require.Equal(t, archiveLoadRecordPayload(id, 4, [2]int{1024, 5119}, entropy == "random", 42), defaultRecord)
				require.Equal(t, archiveLoadRecordPayload(id, 24, [2]int{1024, 5119}, entropy == "random", 42), partitionedRecord)
			}
		})
	}
}
