//go:build integration && fault

package compat

import (
	"context"
	"encoding/binary"
	"io"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestReplicatedPublisherAcknowledgesAllPartitionsAndStops(t *testing.T) {
	// Given a bounded publisher and 24 independently observable source partitions.
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(replicatedPartitions, "source"))
	defer cluster.Close()
	client := fixtureClient(t, cluster.ListenAddrs()[0], kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.AllowIdempotentProduceCancellation(), kgo.RecordDeliveryTimeout(time.Second), kgo.MaxBufferedRecords(48))
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	publisher := startReplicatedPublisher(ctx, client, "source")
	t.Cleanup(func() { publisher.stop() })

	// When two complete batches are acknowledged, stop and join the publisher.
	require.Eventually(t, func() bool { return publisher.snapshot().Batches >= 2 }, 5*time.Second, 10*time.Millisecond)
	state := publisher.stop()
	require.NoError(t, state.err)
	require.False(t, state.Stopped.IsZero())
	require.Zero(t, client.BufferedProduceRecords())
	ends, err := kadm.NewClient(client).ListEndOffsets(ctx, "source")
	require.NoError(t, err)
	require.NoError(t, ends.Error())
	assign(t, client, "source", replicatedPartitions)
	seen := make([]int64, replicatedPartitions)
	total := int64(state.Batches * replicatedPartitions)

	// Then each acknowledgement corresponds to an actual complete unique record
	// on its intended partition, and shutdown leaves no buffered publication.
	for received := int64(0); received < total; {
		fetches := client.PollFetches(ctx)
		require.Empty(t, fetches.Errors())
		for _, record := range fetches.Records() {
			require.Len(t, record.Value, 2048)
			require.EqualValues(t, seen[record.Partition], binary.BigEndian.Uint64(record.Value))
			require.EqualValues(t, record.Partition, binary.BigEndian.Uint32(record.Value[8:]))
			require.Equal(t, []kgo.RecordHeader{{Key: "duplicate", Value: nil}, {Key: "duplicate", Value: []byte{}}}, record.Headers)
			seen[record.Partition]++
			received++
		}
		require.NoError(t, ctx.Err())
	}
	for partition, count := range state.Acknowledged {
		require.EqualValues(t, state.Batches, count)
		require.Equal(t, count, ends["source"][int32(partition)].Offset)
		require.Equal(t, count, seen[partition])
	}
}

func TestReplicatedBrokerPublisherKeepsHealthyPartitionsAndDrainsUncertainWrites(t *testing.T) {
	// Given one temporarily unavailable source leader and two healthy leaders.
	cluster := kfake.MustCluster(kfake.NumBrokers(3), kfake.SeedTopics(replicatedPartitions, "source"))
	defer cluster.Close()
	var blocked atomic.Bool
	blocked.Store(true)
	cluster.ControlKey(int16(kmsg.Produce), func(request kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		produce := request.(*kmsg.ProduceRequest)
		target := false
		for _, topic := range produce.Topics {
			for _, partition := range topic.Partitions {
				target = target || partition.Partition == 0
			}
		}
		if !blocked.Load() || !target {
			return nil, nil, false
		}
		return rejectedReplicatedProduce(produce, 7), nil, true
	})
	client := replicatedBrokerProducer(t, cluster.ListenAddrs()[0], 10*time.Second)
	publisher := startReplicatedBrokerPublisher(t.Context(), client, "source", 10*time.Second)
	t.Cleanup(func() { blocked.Store(false); publisher.stop() })
	// When a partition waits for its acknowledgement, healthy partitions still
	// acknowledge new offers, with only one outstanding offer on each partition.
	require.Eventually(t, func() bool {
		return slices.Max(publisher.snapshot().Acknowledged) >= 3
	}, 5*time.Second, 10*time.Millisecond)
	state := publisher.snapshot()
	require.Zero(t, state.Acknowledged[0])
	require.EqualValues(t, 1, state.Offered[0])
	for partition, offered := range state.Offered {
		require.LessOrEqual(t, offered-state.Acknowledged[partition], int64(1))
	}
	publisher.cancel()
	select {
	case <-publisher.done:
		t.Fatal("stop abandoned uncertain source writes")
	default:
	}
	blocked.Store(false)
	state = publisher.stop()
	// Then shutdown joins all uncertain callbacks and every acknowledged unique
	// offer exists exactly once at the source before any final snapshot.
	require.NoError(t, state.err)
	require.Equal(t, state.Offered, state.Acknowledged)
	require.Zero(t, client.BufferedProduceRecords())
	ends, err := kadm.NewClient(client).ListEndOffsets(t.Context(), "source")
	require.NoError(t, err)
	require.NoError(t, ends.Error())
	for partition, count := range state.Acknowledged {
		require.Equal(t, count, ends["source"][int32(partition)].Offset)
	}
}

func TestReplicatedBrokerPublisherRetriesNegotiationEOFAfterConnectionLoss(t *testing.T) {
	// Given an established idempotent producer on the known-plain fixture.
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(replicatedPartitions, "source"))
	defer cluster.Close()
	client := replicatedBrokerProducer(t, cluster.ListenAddrs()[0], 10*time.Second)
	require.NoError(t, client.ProduceSync(t.Context(), &kgo.Record{Topic: "source", Partition: 0, Key: []byte("warmup"), Value: []byte("warmup")}).FirstErr())
	var cut atomic.Bool
	cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) { return nil, io.EOF, true })
	cluster.ControlKey(int16(kmsg.ApiVersions), func(kmsg.Request) (kmsg.Response, error, bool) {
		cut.Store(true)
		return nil, io.EOF, true
	})
	// When the established connection drops and the replacement's first API
	// negotiation also closes, retry the same offers without manual resubmission.
	publisher := startReplicatedBrokerPublisher(t.Context(), client, "source", 10*time.Second)
	t.Cleanup(func() { publisher.stop() })
	require.Eventually(t, func() bool {
		state := publisher.snapshot()
		return state.err != nil || slices.Min(state.Acknowledged) >= 2
	}, 5*time.Second, 10*time.Millisecond)
	state := publisher.stop()
	// Then every offered write is acknowledged once and exact source ends match.
	require.True(t, cut.Load(), "replacement API negotiation was not cut")
	require.NoError(t, state.err)
	require.GreaterOrEqual(t, slices.Min(state.Acknowledged), int64(2))
	require.Equal(t, state.Offered, state.Acknowledged)
	require.Zero(t, client.BufferedProduceRecords())
	ends, err := kadm.NewClient(client).ListEndOffsets(t.Context(), "source")
	require.NoError(t, err)
	require.NoError(t, ends.Error())
	for partition, count := range state.Acknowledged {
		if partition == 0 {
			count++
		} // The independently acknowledged warmup record.
		require.Equal(t, count, ends["source"][int32(partition)].Offset)
	}
}

func rejectedReplicatedProduce(produce *kmsg.ProduceRequest, code int16) *kmsg.ProduceResponse {
	response := kmsg.NewPtrProduceResponse()
	response.SetVersion(produce.Version)
	for _, topic := range produce.Topics {
		result := kmsg.ProduceResponseTopic{Topic: topic.Topic, TopicID: topic.TopicID}
		for _, partition := range topic.Partitions {
			result.Partitions = append(result.Partitions, kmsg.ProduceResponseTopicPartition{Partition: partition.Partition, ErrorCode: code})
		}
		response.Topics = append(response.Topics, result)
	}
	return response
}

func TestReplicatedPublisherStopsAfterRejectedBatch(t *testing.T) {
	// Given a broker that rejects publication rather than acknowledging input.
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(replicatedPartitions, "source"))
	defer cluster.Close()
	cluster.ControlKey(int16(kmsg.Produce), func(request kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		produce := request.(*kmsg.ProduceRequest)
		return rejectedReplicatedProduce(produce, 29), nil, true
	})
	client := fixtureClient(t, cluster.ListenAddrs()[0], kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.AllowIdempotentProduceCancellation())
	publisher := startReplicatedPublisher(t.Context(), client, "source")
	t.Cleanup(func() { publisher.stop() })

	// When the first batch is rejected, then the error is retained and no later
	// scheduled batch can make the fixture appear to have maintained input.
	select {
	case <-publisher.done:
	case <-time.After(5 * time.Second):
		t.Fatal("publisher did not stop after a permanent source rejection")
	}
	state := publisher.stop()
	require.Error(t, state.err)
	require.Equal(t, 1, state.Batches)
	for _, count := range state.Acknowledged {
		require.Zero(t, count)
	}
	// When the broker-fault publisher meets the same permanent rejection, then
	// it also stops, retains the error and drains all offered callbacks.
	source := replicatedBrokerProducer(t, cluster.ListenAddrs()[0], time.Second)
	offers := startReplicatedBrokerPublisher(t.Context(), source, "source", time.Second)
	t.Cleanup(func() { offers.stop() })
	select {
	case <-offers.done:
	case <-time.After(5 * time.Second):
		t.Fatal("offer publisher ignored a permanent source rejection")
	}
	failed := offers.stop()
	require.Error(t, failed.err)
	require.Positive(t, slices.Max(failed.Offered))
	require.Zero(t, slices.Max(failed.Acknowledged))
	require.Zero(t, source.BufferedProduceRecords())
}
