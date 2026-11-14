//go:build integration && load && harness

package compat

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestLoadWorkloadProfilesPreserveFidelityAndIdentifyTombstones(t *testing.T) {
	for _, profile := range loadWorkloadProfiles() {
		t.Run(profile.Name, func(t *testing.T) {
			// Given the existing bounded stream fixture with this profile selected.
			const count = 64
			stream := newFiniteStreamRecords("fixture", time.Unix(10, 0), count)
			stream.profile = &profile
			for id := range count {
				stream.receipts.submit(id, stream.record(id))
			}
			stream.published, stream.acknowledged = count, count
			// When every generated record is delivered, including null values.
			for id := range count {
				record := stream.record(id)
				require.Equal(t, record, stream.record(id), "record generation must be deterministic")
				stream.verify(t, record)
				if profile.TombstoneEvery != 0 {
					require.Equal(t, id%profile.TombstoneEvery == 0, record.Value == nil)
					require.NotNil(t, record.Key, "compacted topics need keys")
				}
			}
			// Then counts and the submission/receipt fingerprint agree.
			require.Equal(t, count, stream.verified)
			require.Zero(t, stream.corrupted)
			require.Zero(t, stream.unexpected)
			require.Zero(t, stream.duplicates)
			require.Zero(t, stream.reordered)
			delivery := stream.delivery(true)
			require.NotNil(t, delivery.Integrity.Matched)
			require.True(t, *delivery.Integrity.Matched)
		})
	}
}

func TestLoadSourceCompressionWaitsForDelayedObservationsAndCancellation(t *testing.T) {
	for _, cancelWhileWaiting := range []bool{false, true} {
		name := "delayed observation"
		if cancelWhileWaiting {
			name = "cancellation"
		}
		t.Run(name, func(t *testing.T) {
			// Given one observed record and two source records whose hook has not run.
			counts := newSourceCompressionCounts()
			counts.OnProduceBatchWritten(kgo.BrokerMetadata{}, "fixture", 0, kgo.ProduceBatchMetrics{NumRecords: 1, CompressionType: 1})
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			type result struct {
				observed map[string]int64
				err      error
			}
			done := make(chan result, 1)
			go func() {
				observed, err := counts.wait(ctx, 3)
				done <- result{observed, err}
			}()
			// The first hook filled this channel. Sending again proves the waiter
			// consumed its notification and is waiting for the remaining records.
			select {
			case counts.changed <- struct{}{}:
			case early := <-done:
				t.Fatalf("observation wait returned before the delayed hook: %+v", early)
			case <-ctx.Done():
				t.Fatal("observation waiter did not consume the first notification")
			}
			// When the delayed hook runs, or the wait is explicitly cancelled.
			if cancelWhileWaiting {
				cancel()
			} else {
				counts.OnProduceBatchWritten(kgo.BrokerMetadata{}, "fixture", 0, kgo.ProduceBatchMetrics{NumRecords: 2, CompressionType: 1})
			}
			// Then retain exact codec counts, and return cancellation with partial evidence.
			select {
			case completed := <-done:
				if cancelWhileWaiting {
					require.ErrorIs(t, completed.err, context.Canceled)
					require.Equal(t, map[string]int64{"gzip": 1}, completed.observed)
				} else {
					require.NoError(t, completed.err)
					require.Equal(t, map[string]int64{"gzip": 3}, completed.observed)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("observation waiter did not finish")
			}
		})
	}
}

func TestLoadSourceCompressionObservationRespectsDeadline(t *testing.T) {
	// Given partial observations and an expired publication deadline.
	counts := newSourceCompressionCounts()
	counts.OnProduceBatchWritten(kgo.BrokerMetadata{}, "fixture", 0, kgo.ProduceBatchMetrics{NumRecords: 1, CompressionType: 4})
	ctx, cancel := context.WithDeadline(t.Context(), time.Time{})
	defer cancel()
	// When awaiting all source records, then the deadline preserves partial evidence.
	observed, err := counts.wait(ctx, 3)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, map[string]int64{"zstd": 1}, observed)
}

func TestLoadWorkloadTombstoneNullIsNotEmptyAndRequiresItsID(t *testing.T) {
	// Given a compacted-topic profile and one submitted tombstone.
	var profile workloadProfile
	for _, candidate := range loadWorkloadProfiles() {
		if candidate.TombstoneEvery != 0 {
			profile = candidate
		}
	}
	for _, malformed := range []string{"empty value", "missing ID", "short ID"} {
		t.Run(malformed, func(t *testing.T) {
			stream := newFiniteStreamRecords("fixture", time.Unix(10, 0), 1)
			stream.profile, stream.published = &profile, 1
			record := stream.record(0)
			stream.receipts.submit(0, record)
			// When null changes to empty or the only tombstone identity is damaged.
			switch malformed {
			case "empty value":
				record.Value = []byte{}
			case "missing ID":
				record.Headers = record.Headers[:len(record.Headers)-1]
			case "short ID":
				record.Headers[len(record.Headers)-1].Value = []byte{0}
			}
			stream.verify(t, record)
			// Then no corrupted or unidentified record counts as verified.
			require.Zero(t, stream.verified)
			require.Equal(t, 1, stream.corrupted+stream.unexpected)
		})
	}
}

func TestLoadWorkloadProfilesExerciseSizesHeadersAndHotKeys(t *testing.T) {
	// Given each declared workload and deterministic records across 257 keys.
	for _, profile := range loadWorkloadProfiles() {
		stream := newStreamRecords("fixture", time.Unix(10, 0))
		stream.profile = &profile
		switch profile.Name {
		case "near-limit-v1":
			// When values approach the topic limit, then framing still has headroom.
			for id := range 8 {
				record := stream.record(id)
				require.GreaterOrEqual(t, len(record.Value), profile.TopicMaxMessageBytes*999/1000)
				require.Less(t, len(record.Value), profile.TopicMaxMessageBytes)
			}
		case "headers-v1":
			record := stream.record(0)
			require.Len(t, record.Headers, 64)
			require.Nil(t, record.Headers[0].Value)
			require.NotNil(t, record.Headers[1].Value)
			require.Empty(t, record.Headers[1].Value)
			require.Equal(t, record.Headers[2].Key, record.Headers[6].Key, "duplicate header names remain ordered")
			require.Len(t, record.Headers[2].Value, 256)
		case "hot-keys-v1":
			counts := make(map[string]int)
			for id := range 4096 {
				counts[string(stream.record(id).Key)]++
			}
			require.Greater(t, counts["key-0"], 1000, "Zipf head must be hot")
			require.Greater(t, len(counts), 100, "Zipf tail must remain represented")
			require.Zero(t, counts[""])
		}
	}
}

func TestLoadWorkloadSourceCodecNamesMatchCompressedBytes(t *testing.T) {
	// Given the codec profiles and compressible fixture bytes.
	t.Setenv("KAFERRY_ARCHIVE_PAYLOAD", "random")
	types := map[string]kgo.CompressionCodecType{"none": kgo.CodecNone, "gzip": kgo.CodecGzip,
		"snappy": kgo.CodecSnappy, "lz4": kgo.CodecLz4, "zstd": kgo.CodecZstd}
	input := bytes.Repeat([]byte("fixture bytes"), 1000)
	for _, profile := range loadWorkloadProfiles() {
		if !strings.HasPrefix(profile.Name, "source-") {
			continue
		}
		t.Run(profile.Name, func(t *testing.T) {
			// When the configured compressor runs, then its wire codec matches the report.
			stream := newStreamRecords("fixture", time.Unix(10, 0))
			stream.profile = &profile
			record := stream.record(3)
			require.Equal(t, "repeated-byte", profile.ValueEntropy)
			require.Equal(t, bytes.Repeat([]byte{3}, len(record.Value)-4), record.Value[4:],
				"codec coverage uses compressible input regardless of archive entropy overrides")
			compressor, err := kgo.DefaultCompressor(profile.compression())
			require.NoError(t, err)
			encoded, codec := input, kgo.CodecNone
			if compressor != nil {
				encoded, codec = compressor.Compress(new(bytes.Buffer), input)
			}
			expected, exists := types[profile.SourceCompression]
			require.True(t, exists, "source codec must have a declared wire type")
			require.Equal(t, expected, codec)
			decoded, err := kgo.DefaultDecompressor().Decompress(encoded, codec)
			require.NoError(t, err)
			require.Equal(t, input, decoded)
		})
	}
}
