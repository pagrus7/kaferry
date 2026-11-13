//go:build integration && load && harness

package compat

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestSoakMonitorRequiresFiveIdleRSSSamples(t *testing.T) {
	var monitor soakMonitor
	for i := 0; i < 30; i++ {
		monitor.recordRSS(20<<10, false)
	}
	for i := 0; i < 4; i++ {
		monitor.recordRSS(22<<10, true)
	}
	require.ErrorContains(t, monitor.checkIdleRSS(5), "got 4, need 5")
	monitor.recordRSS(22<<10, true)
	require.NoError(t, monitor.checkIdleRSS(5))
	_, final, _ := memoryEnvelope(t, monitor.rss, 5)
	require.Equal(t, int64(22<<10), final)
	monitor.recordRSS(20<<10, false)
	require.ErrorContains(t, monitor.checkIdleRSS(5), "got 0, need 5")
}

func TestConsumeOnlyStartAllowsLatePublishingGrace(t *testing.T) {
	scheduledEnd := time.Unix(0, 0)
	require.Equal(t, scheduledEnd.Add(10*time.Second),
		consumeOnlyStart(scheduledEnd, scheduledEnd.Add(-time.Second)).Add(soakDrainDuration))
	lastPublish := scheduledEnd.Add(5 * time.Second)
	deadline := consumeOnlyStart(scheduledEnd, lastPublish).Add(soakDrainDuration)
	require.Equal(t, scheduledEnd.Add(15*time.Second), deadline)
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

func TestLoadStreamingVerifierConsumesTrailingDuplicates(t *testing.T) {
	// Given a verifier that already consumed all initially available output.
	const destination = "fixture"
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, destination))
	t.Cleanup(cluster.Close)
	consumer := fixtureClient(t, cluster.ListenAddrs()[0], kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	a := kadm.NewClient(consumer)
	assign(t, consumer, destination, 2)
	p := fixtureClient(t, cluster.ListenAddrs()[0], kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.ProducerBatchCompression(kgo.NoCompression()))
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
	// Given a producer whose fixture report channel has not yet been drained.
	const name = "fixture"
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(4, name))
	t.Cleanup(cluster.Close)
	stream := newStreamRecords(name, time.Now().Truncate(time.Millisecond))
	stream.observer = newDeliveryObserver(time.Now)
	p := fixtureClient(t, cluster.ListenAddrs()[0], kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.ProducerBatchCompression(kgo.NoCompression()))
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
	c := fixtureClient(t, cluster.ListenAddrs()[0], kgo.FetchIsolationLevel(kgo.ReadCommitted()))
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
