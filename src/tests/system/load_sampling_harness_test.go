//go:build integration && load && harness

package compat

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestLoadStreamingVerifierBoundsHistory(t *testing.T) {
	// Given many epochs arriving out of order within a bounded verification window.
	stamp := time.Now().Truncate(time.Millisecond)
	stream := newStreamRecords("fixture", stamp)
	stream.published = 4096
	for first := 0; first < stream.published; first += 16 {
		for partition := 3; partition >= 0; partition-- {
			for id := first + partition; id < first+16; id += 4 {
				input := archiveLoadRecord(id)
				headers := make([]kgo.RecordHeader, len(input.Headers))
				for i, h := range input.Headers {
					headers[i] = kgo.RecordHeader{Key: h.Name, Value: h.Value}
				}
				// When consumed records are verified, then completed history is released.
				stream.verify(t, &kgo.Record{Key: input.Key, Value: input.Value, Headers: headers, Timestamp: stamp, Partition: int32(id % 4)})
				require.LessOrEqual(t, len(stream.receipts.pending), 16)
			}
		}
	}
	require.Equal(t, stream.published, stream.verified)
	require.Equal(t, stream.published, stream.receipts.contiguous())
	require.Empty(t, stream.receipts.pending)
}
