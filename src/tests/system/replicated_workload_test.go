//go:build integration && fault

package compat

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type replicatedDelayedPublisher struct {
	publisher    *replicatedPublisher
	client       *kgo.Client
	ctx          context.Context
	release      func()
	releaseOther func()
}

func startReplicatedDelayedPublisher(t *testing.T, reject bool) replicatedDelayedPublisher {
	t.Helper()
	cluster := kfake.MustCluster(kfake.NumBrokers(3), kfake.SeedTopics(replicatedPartitions, "source"))
	t.Cleanup(cluster.Close)
	release, otherRelease := make(chan struct{}), make(chan struct{})
	var released, otherReleased sync.Once
	unblock := func() { released.Do(func() { close(release) }) }
	unblockOther := func() { otherReleased.Do(func() { close(otherRelease) }) }
	var delayed, otherDelayed atomic.Bool
	cluster.ControlKey(int16(kmsg.Produce), func(request kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		produce := request.(*kmsg.ProduceRequest)
		if replicatedProduceContainsZero(produce) && delayed.CompareAndSwap(false, true) {
			cluster.SleepControl(func() { <-release })
			if reject {
				return rejectedReplicatedProduce(produce, 29), nil, true
			}
		} else if reject && !replicatedProduceContainsZero(produce) && otherDelayed.CompareAndSwap(false, true) {
			cluster.SleepControl(func() { <-otherRelease })
		}
		return nil, nil, false
	})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	client := replicatedBrokerProducer(t, cluster.ListenAddrs()[0], 5*time.Second)
	publisher := startReplicatedBrokerPublisher(ctx, client, "source", 5*time.Second)
	t.Cleanup(func() { unblock(); unblockOther(); publisher.stop() })
	require.Eventually(t, func() bool {
		return delayed.Load() && (!reject || otherDelayed.Load()) && slices.Max(publisher.snapshot().Acknowledged) >= 6
	}, 3*time.Second, 5*time.Millisecond)
	before := publisher.snapshot()
	require.Zero(t, before.Acknowledged[0])
	require.EqualValues(t, 1, before.Offered[0])
	require.EqualValues(t, 48, client.OptValue(kgo.MaxBufferedRecords))
	for partition, offered := range before.Offered {
		require.LessOrEqual(t, offered-before.Acknowledged[partition], int64(1))
	}
	return replicatedDelayedPublisher{publisher, client, ctx, unblock, unblockOther}
}

func (s replicatedDelayedPublisher) reconcile(t *testing.T, state replicatedPublishing) int64 {
	t.Helper()
	require.False(t, state.Stopped.IsZero())
	require.Zero(t, s.client.BufferedProduceRecords())
	ends, err := kadm.NewClient(s.client).ListEndOffsets(s.ctx, "source")
	require.NoError(t, err)
	require.NoError(t, ends.Error())
	var total int64
	for partition, count := range state.Acknowledged {
		require.Equal(t, count, ends["source"][int32(partition)].Offset)
		total += count
	}
	return total
}

func (s replicatedDelayedPublisher) stopWhileHeld(t *testing.T) <-chan replicatedPublishing {
	t.Helper()
	cancelled := make(chan struct{})
	var once sync.Once
	cancel := s.publisher.cancel
	s.publisher.cancel = func() {
		cancel()
		once.Do(func() { close(cancelled) })
	}
	stopped := make(chan replicatedPublishing, 1)
	go func() { stopped <- s.publisher.stop() }()
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("stop did not cancel publication while its response was held")
	}
	select {
	case <-stopped:
		t.Fatal("stop abandoned an uncertain offer before its response was released")
	case <-time.After(100 * time.Millisecond):
	}
	return stopped
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

func replicatedProduceContainsZero(produce *kmsg.ProduceRequest) bool {
	for _, topic := range produce.Topics {
		for _, partition := range topic.Partitions {
			if partition.Partition == 0 {
				return true
			}
		}
	}
	return false
}
