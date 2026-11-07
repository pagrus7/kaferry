//go:build integration && load

package compat

import (
	"context"
	"errors"
	"math"
	"sync"
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

func TestLoadDeliveryObservationRetainsTransientPeakAndLongestLatency(t *testing.T) {
	// Given three submissions made at different instants, with no periodic samples.
	start := time.Now()
	now := start
	observer := newDeliveryObserver(func() time.Time { return now })
	for id := range 3 {
		now = start.Add(time.Duration(id) * 100 * time.Millisecond)
		observer.submit(id)
	}
	require.Zero(t, observer.snapshot().Backlog)

	// When all acknowledgements arrive, then receipts clear the backlog before the next snapshot.
	for id := range 3 {
		observer.publication(id, nil)
	}
	for id := range 3 {
		now = start.Add(500*time.Millisecond + time.Duration(id)*50*time.Millisecond)
		observer.receive(id)
	}

	// Then the transient peak and longest record time survive even though current lag is zero.
	result := observer.snapshot()
	require.Zero(t, result.Backlog)
	require.Zero(t, result.Unsettled)
	require.Equal(t, 3, result.PeakBacklog)
	require.Equal(t, 3, result.TimedReceipts)
	require.InDelta(t, 0.5, *result.MaxLatencySeconds, 1e-9)
}

func TestLoadDeliveryObservationMatchesIDsWhenReceiptPrecedesAcknowledgement(t *testing.T) {
	// Given one acknowledged ID and a different ID received before its acknowledgement.
	now := time.Now()
	observer := newDeliveryObserver(func() time.Time { return now })
	observer.submit(0)
	observer.submit(1)
	observer.publication(0, nil)
	observer.receive(1)
	observer.receive(1)
	require.Equal(t, 1, observer.snapshot().Backlog, "a different receipt cannot hide the acknowledged ID")

	// When the late acknowledgement arrives, followed by the outstanding receipt.
	observer.publication(1, nil)
	require.Equal(t, 1, observer.snapshot().Backlog)
	now = now.Add(time.Second)
	observer.receive(0)

	// Then acknowledgement ordering creates neither a false peak nor duplicate timing samples.
	result := observer.snapshot()
	require.Zero(t, result.Backlog)
	require.Zero(t, result.Unsettled)
	require.Equal(t, 1, result.PeakBacklog)
	require.Equal(t, 2, result.TimedReceipts)
	require.Equal(t, 1.0, *result.MaxLatencySeconds)
}

func TestLoadDeliveryObservationReleasesFailedAndCompletedHistory(t *testing.T) {
	// Given a failed source publication without a destination receipt.
	observer := newDeliveryObserver(time.Now)
	observer.submit(0)
	observer.publication(0, errors.New("fixture failure"))
	require.Nil(t, observer.snapshot().MaxLatencySeconds, "no receipts is not a measured zero latency")
	require.Nil(t, observer.snapshot().P95LatencySeconds)
	require.Nil(t, observer.snapshot().P99LatencySeconds)
	require.Nil(t, observer.snapshot().P999LatencySeconds)
	require.Zero(t, observer.snapshot().Unsettled)

	// When many later records are acknowledged and received.
	for id := 1; id <= 10000; id++ {
		observer.submit(id)
		observer.publication(id, nil)
		observer.receive(id)
	}

	// Then timing history does not grow with the total number of completed records.
	result := observer.snapshot()
	require.Zero(t, result.Backlog)
	require.Zero(t, result.Unsettled)
	require.Equal(t, 10000, result.TimedReceipts)
}

func TestLoadDeliveryObservationSynchronizesCallbacksAndReceipts(t *testing.T) {
	// Given submissions whose callbacks and receipts can run on different goroutines.
	observer := newDeliveryObserver(time.Now)
	const count = 4096
	for id := range count {
		observer.submit(id)
	}
	var workers sync.WaitGroup
	workers.Add(2)

	// When callbacks arrive forwards while receipts arrive backwards.
	go func() {
		defer workers.Done()
		for id := range count {
			observer.publication(id, nil)
		}
	}()
	go func() {
		defer workers.Done()
		for id := count - 1; id >= 0; id-- {
			observer.receive(id)
			if id%64 == 0 {
				observer.snapshot()
			}
		}
	}()
	completed := make(chan observedDelivery, 1)
	go func() {
		workers.Wait()
		completed <- observer.snapshot()
	}()

	// Then each ID is timed once and all state settles without a race.
	var result observedDelivery
	select {
	case result = <-completed:
	case <-time.After(5 * time.Second):
		t.Fatal("delivery observer callbacks and receipts did not settle")
	}
	require.Zero(t, result.Backlog)
	require.Zero(t, result.Unsettled)
	require.Equal(t, count, result.TimedReceipts)
	require.GreaterOrEqual(t, result.PeakBacklog, 0)
	require.LessOrEqual(t, result.PeakBacklog, count)
}

func TestLoadDeliveryObservationSeesCallbacksBeforeReportDrain(t *testing.T) {
	// Given a real producer whose fixture report channel has not yet been drained.
	a := admin(t)
	name := topic(t, a, 4)
	stream := newStreamRecords(name, time.Now().Truncate(time.Millisecond))
	stream.observer = newDeliveryObserver(time.Now)
	p := producer(t)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	reports := make(chan kgo.ProduceResult, 8)
	stream.publish(t, ctx, p, reports, 8)
	require.NoError(t, p.Flush(ctx))
	require.Eventually(t, func() bool { return len(reports) == 8 }, 5*time.Second, time.Millisecond,
		"all callbacks must reach the undrained report channel")

	// When callbacks complete before the verifier starts consuming.
	require.Zero(t, stream.acknowledged)
	require.Equal(t, 8, stream.observer.snapshot().Backlog)
	c := consumer(t)
	assign(t, c, name, 4)
	for stream.unique < 8 {
		require.NoError(t, ctx.Err())
		stream.catchUp(t, c, reports)
	}
	stream.drainReports(t, reports)

	// Then the peak was measured at callback time and every first receipt has a duration.
	result := stream.delivery(true)
	require.True(t, result.ReconciliationComplete)
	require.Equal(t, 8, result.Observed.PeakBacklog)
	require.Equal(t, 8, result.Observed.TimedReceipts)
	require.Zero(t, result.Observed.Backlog)
	require.Zero(t, result.Observed.Unsettled)
	require.Positive(t, *result.Observed.MaxLatencySeconds)
	require.True(t, *result.Integrity.Matched)
	require.Equal(t, 8, result.Integrity.SubmittedRecords)
	require.Equal(t, 8, result.Integrity.ReceivedRecords)
}

func TestLoadDeliveryPercentilesCountEveryReceiptAndRetainTheTail(t *testing.T) {
	// Given 10,000 receipts with known percentile boundaries and a longer maximum.
	now := time.Now()
	observer := newDeliveryObserver(func() time.Time { return now })
	for id := range 10000 {
		latency := time.Millisecond
		switch {
		case id >= 9990:
			latency = time.Second
		case id >= 9900:
			latency = 100 * time.Millisecond
		case id >= 9500:
			latency = 10 * time.Millisecond
		}
		observer.submit(id)
		observer.publication(id, nil)
		now = now.Add(latency)
		observer.receive(id)
		observer.receive(id) // A duplicate cannot enter the timing population.
	}

	// When all first receipts are summarized, then each nearest-rank boundary is retained.
	result := observer.snapshot()
	require.Equal(t, 10000, result.TimedReceipts)
	require.Equal(t, 1.0, *result.MaxLatencySeconds)
	for i, measured := range []*float64{result.P95LatencySeconds, result.P99LatencySeconds, result.P999LatencySeconds} {
		want := []float64{0.001, 0.01, 0.1}[i]
		require.GreaterOrEqual(t, *measured, want)
		require.Less(t, *measured, want*1.001)
	}
}

func TestLoadDeliveryPercentilesCoverZeroSmallPopulationsAndLongDurations(t *testing.T) {
	// Given populations spanning zero, nanoseconds, bucket boundaries and int64's limit.
	for _, latency := range []time.Duration{0, 1, 2047, 2048, time.Microsecond * 77, 24 * time.Hour, math.MaxInt64} {
		t.Run(latency.String(), func(t *testing.T) {
			now := time.Now()
			observer := newDeliveryObserver(func() time.Time { return now })
			observer.submit(0)
			observer.publication(0, nil)
			now = now.Add(latency)
			observer.receive(0)

			// When one receipt is summarized, then all percentiles are capped at its exact maximum.
			result := observer.snapshot()
			for _, measured := range []*float64{result.P95LatencySeconds, result.P99LatencySeconds, result.P999LatencySeconds} {
				require.Equal(t, latency.Seconds(), *measured)
			}
		})
	}
}

func TestLoadDeliveryPercentilesRoundNonIntegralRanksUp(t *testing.T) {
	// Given 101 distinct receipt times, entered in descending order.
	now := time.Now()
	observer := newDeliveryObserver(func() time.Time { return now })
	for id := range 101 {
		observer.submit(id)
		observer.publication(id, nil)
		now = now.Add(time.Duration(101-id) * time.Nanosecond)
		observer.receive(id)
	}

	// When percentile ranks are fractional, then the next full rank is selected.
	result := observer.snapshot()
	require.Equal(t, 101, result.TimedReceipts)
	require.Equal(t, (96 * time.Nanosecond).Seconds(), *result.P95LatencySeconds)
	require.Equal(t, (100 * time.Nanosecond).Seconds(), *result.P99LatencySeconds)
	require.Equal(t, (101 * time.Nanosecond).Seconds(), *result.P999LatencySeconds)
}
