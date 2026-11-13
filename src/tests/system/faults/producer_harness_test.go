//go:build harness

package faults

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestProxyProducerFirstRetriableResponseRetainsOriginalDiagnostic(t *testing.T) {
	// Given prepared metadata and an open producer connection, the first Produce
	// receives NOT_ENOUGH_REPLICAS without appending anything at the broker.
	cluster, routes := readinessCluster(t)
	diagnostic := &SDKDiagnostics{}
	client, err := newProxyProducer(cluster.ListenAddrs(), diagnostic)
	require.NoError(t, err)
	defer client.Close()
	require.Equal(t, 10*time.Millisecond, client.OptValue(kgo.MetadataMinAge))
	require.Equal(t, int64(1<<63-1), client.OptValue(kgo.RecordRetries), "native retry policy")
	require.Equal(t, 2*time.Second, client.OptValue(kgo.RecordDeliveryTimeout))
	require.Equal(t, time.Second, client.OptValue(kgo.ProduceRequestTimeout))
	require.Equal(t, time.Second, client.OptValue(kgo.RequestTimeoutOverhead))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	const topic = "first-retriable-response"
	_, err = kadm.NewClient(client).CreateTopic(ctx, 3, 3, nil, topic)
	require.NoError(t, err)
	require.NoError(t, prepareProxyProducer(ctx, client, topic, 0, routes, t.Logf))
	requests := rejectFirstProxyProduce(cluster, kerr.NotEnoughReplicas.Code)
	// When the safe first rejection triggers native retry, the existing bounds
	// allow exactly one retried request and one acknowledged appended record.
	result := client.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: 0, Value: []byte("once")})
	require.Len(t, result, 1)
	require.NoError(t, result.FirstErr(), diagnostic.Snapshot())
	require.Zero(t, result[0].Record.Offset)
	require.Equal(t, int32(2), requests.Load())
	require.Contains(t, diagnostic.Snapshot(), "NOT_ENOUGH_REPLICAS")
	ends, err := kadm.NewClient(client).ListEndOffsets(ctx, topic)
	require.NoError(t, err)
	require.NoError(t, ends.Error())
	require.Equal(t, int64(1), ends[topic][0].Offset)
	// Then retaining more SDK output stays bounded and exposes any omission.
	bounded := &SDKDiagnostics{}
	for index := range 130 {
		bounded.Log(kgo.LogLevelError, fmt.Sprint("row=", index, " ", strings.Repeat("synthetic transport error ", 100)))
	}
	require.Less(t, len(bounded.Snapshot()), 128*2100)
	require.Contains(t, bounded.Snapshot(), "[truncated]")
	require.Contains(t, bounded.Snapshot(), "SDK diagnostic rows omitted: 2")
	require.NotContains(t, bounded.Snapshot(), "row=0 ")
	require.NotContains(t, bounded.Snapshot(), "row=1 ")
	require.Contains(t, bounded.Snapshot(), "row=129 ")
}

func TestProxyProducerRejectsPermanentProduceFailureWithoutRetry(t *testing.T) {
	// Given a permanent rejection, when the probe uses native retries, then it
	// still returns that error after one request without appending a record.
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "records"))
	defer cluster.Close()
	diagnostic := &SDKDiagnostics{}
	client, err := newProxyProducer(cluster.ListenAddrs(), diagnostic)
	require.NoError(t, err)
	defer client.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	requests := rejectFirstProxyProduce(cluster, kerr.TopicAuthorizationFailed.Code)
	result := client.ProduceSync(ctx, &kgo.Record{Topic: "records", Partition: 0, Value: []byte("rejected")})
	require.Len(t, result, 1)
	require.ErrorIs(t, result.FirstErr(), kerr.TopicAuthorizationFailed)
	require.Equal(t, int32(1), requests.Load())
	require.Contains(t, diagnostic.Snapshot(), "TOPIC_AUTHORIZATION_FAILED")
	ends, err := kadm.NewClient(client).ListEndOffsets(ctx, "records")
	require.NoError(t, err)
	require.NoError(t, ends.Error())
	require.Zero(t, ends["records"][0].Offset)
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
