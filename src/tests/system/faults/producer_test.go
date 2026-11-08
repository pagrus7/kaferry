package faults

import (
	"context"
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

func proxyProducer(seeds ...string) (*kgo.Client, error) {
	return proxyProducerWithHooks(seeds)
}

func proxyProducerWithHooks(seeds []string, hooks ...kgo.Hook) (*kgo.Client, error) {
	// A diagnostic write must be cancellable even after an ambiguous request.
	// Close this producer before healing; output reconciliation still rejects
	// any cancelled record that actually reached Kafka.
	return kgo.NewClient(kgo.SeedBrokers(seeds...), kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.AllowIdempotentProduceCancellation(), kgo.RecordDeliveryTimeout(2*time.Second),
		kgo.RecordRetries(0), kgo.ProduceRequestTimeout(time.Second), kgo.RequestTimeoutOverhead(time.Second), kgo.WithHooks(hooks...))
}

func TestProxyProducerCancellationCannotRetryAfterHealing(t *testing.T) {
	// Given a successful write followed by a request that receives no outcome.
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "records"))
	defer cluster.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	defer cancel()
	client, err := proxyProducer(cluster.ListenAddrs()...)
	require.NoError(t, err)
	closeProbe := sync.OnceFunc(client.Close)
	defer closeProbe()
	produce := func(producer *kgo.Client, value string) error {
		return producer.ProduceSync(ctx, &kgo.Record{Topic: "records", Partition: 0, Value: []byte(value)}).FirstErr()
	}
	require.NoError(t, produce(client, "before"))
	observed := make(chan struct{}, 1)
	var healed atomic.Bool
	cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		if healed.Load() {
			return nil, nil, false
		}
		cluster.KeepControl()
		select {
		case observed <- struct{}{}:
		default:
		}
		return nil, nil, true
	})
	blocked, stopBlocked := context.WithCancel(ctx)
	defer stopBlocked()
	result := make(chan error, 1)
	go func() {
		result <- client.ProduceSync(blocked, &kgo.Record{Topic: "records", Partition: 0, Value: []byte("blocked")}).FirstErr()
	}()
	select {
	case <-observed:
	case <-ctx.Done():
		t.Fatal("diagnostic request was not observed", ctx.Err())
	}
	// When the already-issued diagnostic record is cancelled, its callback must
	// complete without waiting for restoration or an idempotent sequence outcome.
	stopBlocked()
	select {
	case err = <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-ctx.Done():
		closeProbe()
		<-result
		t.Fatal("in-flight diagnostic cancellation did not complete", ctx.Err())
	}
	closeProbe()
	require.Zero(t, client.BufferedProduceRecords())
	healed.Store(true)
	// Then fresh producers on the same endpoint can write, with no delayed copy
	// of the cancelled record. Read every record and the final end offset.
	recovered, err := proxyProducer(cluster.ListenAddrs()...)
	require.NoError(t, err)
	defer recovered.Close()
	require.NoError(t, produce(recovered, "observer"))
	require.NoError(t, produce(recovered, "after"))
	reader, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{"records": {0: kgo.NewOffset().AtStart()}}))
	require.NoError(t, err)
	defer reader.Close()
	var values []string
	for len(values) < 3 && ctx.Err() == nil {
		fetches := reader.PollRecords(ctx, 10)
		require.Empty(t, fetches.Errors())
		fetches.EachRecord(func(record *kgo.Record) { values = append(values, string(record.Value)) })
	}
	require.Equal(t, []string{"before", "observer", "after"}, values)
	ends, err := kadm.NewClient(reader).ListEndOffsets(ctx, "records")
	require.NoError(t, err)
	require.NoError(t, ends.Error())
	require.Equal(t, int64(3), ends["records"][0].Offset)
}
