//go:build harness

package faults

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestProducerPreparationObservesEveryBrokerThenInitializesBeforeAnyWrite(t *testing.T) {
	// Given one broker initially lacks the topic while the other route brokers know it.
	cluster, routes := readinessCluster(t)
	var metadataReads [3]atomic.Int32
	var ids, writes atomic.Int32
	var earlyID atomic.Bool
	cluster.ControlKey(int16(kmsg.Metadata), func(request kmsg.Request) (kmsg.Response, error, bool) {
		metadata := request.(*kmsg.MetadataRequest)
		if len(metadata.Topics) == 0 {
			return nil, nil, false
		}
		node := cluster.CurrentNode()
		count := metadataReads[node].Add(1)
		if node != 2 || count > 1 {
			return nil, nil, false
		}
		cluster.KeepControl()
		return unknownProxyTopic(metadata.Version, routes), nil, true
	})
	cluster.ControlKey(int16(kmsg.InitProducerID), func(kmsg.Request) (kmsg.Response, error, bool) {
		ids.Add(1)
		earlyID.Store(metadataReads[0].Load() == 0 || metadataReads[1].Load() == 0 || metadataReads[2].Load() < 2)
		return nil, nil, false
	})
	cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		writes.Add(1)
		return nil, nil, false
	})
	connections := &producerConnections{counts: make(map[int32]int)}
	client, err := proxyProducerWithHooks(cluster.ListenAddrs(), connections)
	require.NoError(t, err)
	defer client.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	// When preparing the actual producer, wait for every route's topic view.
	require.NoError(t, prepareProxyProducer(ctx, client, "records", 1, routes, t.Logf))
	// Then PID and the separate produce connection exist before the single write.
	require.False(t, earlyID.Load())
	require.Equal(t, int32(1), ids.Load())
	require.Zero(t, writes.Load())
	view, err := kadm.NewClient(client).Metadata(ctx, "records")
	require.NoError(t, err)
	leader := view.Topics["records"].Partitions[1].Leader
	connections.mu.Lock()
	opened := connections.counts[leader]
	connections.mu.Unlock()
	require.Equal(t, 2, opened, "metadata and produce connections must both be open")
	require.NoError(t, client.ProduceSync(ctx, &kgo.Record{Topic: "records", Partition: 1, Value: []byte("once")}).FirstErr())
	require.Equal(t, int32(1), writes.Load())
}

func TestProducerPreparationRejectsMetadataOutsideSelectedRoute(t *testing.T) {
	// Given reachable brokers whose discovered address differs from the selected path.
	cluster, routes := readinessCluster(t)
	routes[1] = "127.0.0.1:1"
	client, err := proxyProducer(cluster.ListenAddrs()...)
	require.NoError(t, err)
	defer client.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	// When metadata is inspected, then refuse the mismatched endpoint before writing.
	err = prepareProxyProducer(ctx, client, "records", 1, routes, t.Logf)
	require.ErrorContains(t, err, "bypasses the selected producer route")
	require.Zero(t, client.BufferedProduceRecords())
}

func TestProducerPreparationHonorsCancellationAndDeadlineBeforeAnyWrite(t *testing.T) {
	for _, mode := range []string{"cancel", "deadline"} {
		t.Run(mode, func(t *testing.T) { checkProducerPreparationContext(t, mode) })
	}
}
