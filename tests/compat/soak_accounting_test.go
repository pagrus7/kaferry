//go:build integration && load

package compat

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func streamFixtureRecord(id int, stamp time.Time) *kgo.Record {
	record := archiveLoadRecord(id)
	headers := make([]kgo.RecordHeader, len(record.Headers))
	for i, h := range record.Headers {
		headers[i] = kgo.RecordHeader{Key: h.Name, Value: h.Value}
	}
	return &kgo.Record{Key: record.Key, Value: record.Value, Headers: headers, Timestamp: stamp, Partition: int32(id % 4)}
}

func TestLoadStreamingAccountingRetainsAllDeliveryFailures(t *testing.T) {
	// Given acknowledged source IDs with two gaps and one corrupted value.
	stream := newStreamRecords("fixture", time.Now().Truncate(time.Millisecond))
	stream.published, stream.acknowledged = 12, 12
	for _, id := range []int{0, 1, 2, 3, 4, 6, 7, 8, 10, 11} {
		record := streamFixtureRecord(id, stream.stamp)
		if id == 4 {
			record.Value[len(record.Value)-1] ^= 1
		}
		stream.verify(t, record)
	}

	// When extra output arrives after all available unique IDs were consumed.
	stream.verify(t, streamFixtureRecord(0, stream.stamp))
	stream.verify(t, streamFixtureRecord(12, stream.stamp))
	result := stream.delivery(true)

	// Then reconciliation counts every failure rather than stopping at the first.
	require.True(t, result.ReconciliationComplete)
	require.Equal(t, 12, result.Published)
	require.Equal(t, 12, result.Received)
	require.Equal(t, 10, result.UniqueReceived)
	require.Equal(t, 9, result.Verified)
	require.Equal(t, 2, *result.Missing)
	require.Equal(t, 1, result.Duplicates)
	require.Equal(t, 1, result.Corrupted)
	require.Equal(t, 1, result.Unexpected)
}

func TestLoadStreamingAccountingMarksPartialEvidence(t *testing.T) {
	// Given a publication failure and a received ID whose acknowledgement is pending.
	stream := newStreamRecords("fixture", time.Now().Truncate(time.Millisecond))
	stream.published = 3
	stream.verify(t, streamFixtureRecord(0, stream.stamp))
	reports := make(chan kgo.ProduceResult, 2)
	reports <- kgo.ProduceResult{}
	reports <- kgo.ProduceResult{Err: errors.New("fixture publication failed")}

	// When callbacks and a partial snapshot are recorded.
	stream.drainReports(t, reports)
	result := stream.delivery(false)

	// Then observed counters survive, without claiming that pending IDs were lost.
	require.Equal(t, 1, result.Published)
	require.Equal(t, 1, result.PublishErrors)
	require.Equal(t, 2, result.Pending)
	require.Nil(t, result.Missing)
	require.False(t, result.ReconciliationComplete)
	require.Nil(t, stream.delivery(true).Missing, "failed publication cannot establish complete reconciliation")
}

func TestLoadArchiveAccountingReleasesEachPartitionHistory(t *testing.T) {
	// Given a large archive scanned one source partition at a time.
	stream := newStreamRecords("fixture", time.Now())
	stream.published = 65536
	for partition := 0; partition < 4; partition++ {
		for id := partition; id < stream.published; id += 4 {
			// When ordered IDs are observed, then another partition cannot retain their history.
			_, unique := stream.receive(t, archiveLoadRecord(id).Value)
			require.True(t, unique)
			require.Empty(t, stream.seen)
		}
	}
	require.Equal(t, stream.published, stream.unique)
}

func TestLoadStreamingVerifierConsumesTrailingDuplicates(t *testing.T) {
	// Given a verifier that already consumed all initially available output.
	a := admin(t)
	destination := topic(t, a, 2)
	consumer := fixtureClient(t, broker(t))
	assign(t, consumer, destination, 2)
	p := producer(t)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	stream := newStreamRecords(destination, time.Now().Truncate(time.Millisecond))
	stream.published, stream.acknowledged = 2, 2
	first := streamFixtureRecord(0, stream.stamp)
	first.Topic, first.Partition = destination, 0
	require.NoError(t, p.ProduceSync(ctx, first).FirstErr())
	reports := make(chan kgo.ProduceResult)
	for stream.unique < 1 {
		require.NoError(t, ctx.Err())
		stream.catchUp(t, consumer, reports)
	}

	// When a second ID and a trailing duplicate appear before the final offset boundary.
	second, duplicate := streamFixtureRecord(1, stream.stamp), streamFixtureRecord(0, stream.stamp)
	second.Topic, second.Partition = destination, 1
	duplicate.Topic, duplicate.Partition = destination, 0
	require.NoError(t, p.ProduceSync(ctx, second, duplicate).FirstErr())
	ends := destinationOffsets(t, a, destination, 2)
	require.Equal(t, int64(2), destinationOffsetLag(ends, stream.offsets))
	for destinationOffsetLag(ends, stream.offsets) != 0 {
		require.NoError(t, ctx.Err())
		stream.catchUp(t, consumer, reports)
	}

	// Then the boundary includes the duplicate even after both unique IDs have arrived.
	result := stream.delivery(true)
	require.Equal(t, 3, result.Received)
	require.Equal(t, 2, result.UniqueReceived)
	require.Zero(t, *result.Missing)
	require.Equal(t, 1, result.Duplicates)
	require.Zero(t, destinationOffsetLag(ends, stream.offsets))
}
