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

func proxyProducer(seeds ...string) (*kgo.Client, error) {
	return proxyProducerWithHooks(seeds)
}

func proxyProducerWithHooks(seeds []string, hooks ...kgo.Hook) (*kgo.Client, error) {
	return newProxyProducer(seeds, nil, hooks...)
}

func newProxyProducer(seeds []string, diagnostic kgo.Logger, hooks ...kgo.Hook) (*kgo.Client, error) {
	// A diagnostic write must be cancellable even after an ambiguous request.
	// Close this producer before healing; output reconciliation still rejects
	// any cancelled record that actually reached Kafka.
	options := []kgo.Opt{kgo.SeedBrokers(seeds...), kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.AllowIdempotentProduceCancellation(), kgo.RecordDeliveryTimeout(2 * time.Second),
		kgo.RecordRetries(0), kgo.ProduceRequestTimeout(time.Second), kgo.RequestTimeoutOverhead(time.Second), kgo.WithHooks(hooks...)}
	if diagnostic != nil {
		options = append(options, kgo.WithLogger(diagnostic))
	}
	return kgo.NewClient(options...)
}

// Keep original SDK response and transport errors without unbounded debug logs.
// The fixture has only synthetic records; no record values are logged.
type proxyDiagnostics struct {
	mu      sync.Mutex
	lines   []string
	dropped int
}

func (*proxyDiagnostics) Level() kgo.LogLevel { return kgo.LogLevelDebug }

func (d *proxyDiagnostics) Log(level kgo.LogLevel, message string, values ...any) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.lines) >= 128 {
		d.dropped++
		return
	}
	line := fmt.Sprintf("%s %s %v", level, message, values)
	if len(line) > 2048 {
		line = line[:2048] + " [truncated]"
	}
	d.lines = append(d.lines, line)
}

func (d *proxyDiagnostics) snapshot() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return fmt.Sprintf("%s\nSDK diagnostic rows omitted: %d", strings.Join(d.lines, "\n"), d.dropped)
}

func TestProxyProducerFirstRetriableResponseRetainsOriginalDiagnostic(t *testing.T) {
	// Given prepared metadata and an open producer connection, the first Produce
	// receives NOT_ENOUGH_REPLICAS without appending anything at the broker.
	cluster, routes := readinessCluster(t)
	diagnostic := &proxyDiagnostics{}
	client, err := newProxyProducer(cluster.ListenAddrs(), diagnostic)
	require.NoError(t, err)
	defer client.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	const topic = "first-retriable-response"
	_, err = kadm.NewClient(client).CreateTopic(ctx, 3, 3, nil, topic)
	require.NoError(t, err)
	require.NoError(t, prepareProxyProducer(ctx, client, topic, 0, routes, t.Logf))
	var requests atomic.Int32
	cluster.ControlKey(int16(kmsg.Produce), func(request kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		if requests.Add(1) != 1 {
			return nil, nil, false
		}
		produce := request.(*kmsg.ProduceRequest)
		response := kmsg.NewPtrProduceResponse()
		response.Version = produce.Version
		for _, topic := range produce.Topics {
			result := kmsg.ProduceResponseTopic{Topic: topic.Topic, TopicID: topic.TopicID}
			for _, partition := range topic.Partitions {
				failed := kmsg.NewProduceResponseTopicPartition()
				failed.Partition, failed.ErrorCode = partition.Partition, kerr.NotEnoughReplicas.Code
				result.Partitions = append(result.Partitions, failed)
			}
			response.Topics = append(response.Topics, result)
		}
		return response, nil, true
	})
	// When the zero-retry override rejects that first response, preserve its
	// original protocol error rather than assigning a cause to ErrRecordRetries.
	err = client.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: 0, Value: []byte("once")}).FirstErr()
	require.ErrorIs(t, err, kerr.NotEnoughReplicas)
	require.Equal(t, int32(1), requests.Load())
	require.Contains(t, diagnostic.snapshot(), "NOT_ENOUGH_REPLICAS")
	require.Contains(t, diagnostic.snapshot(), "max_retries_reached true")
	ends, err := kadm.NewClient(client).ListEndOffsets(ctx, topic)
	require.NoError(t, err)
	require.NoError(t, ends.Error())
	require.Zero(t, ends[topic][0].Offset)
	// Then retaining more SDK output stays bounded and exposes any omission.
	bounded := &proxyDiagnostics{}
	for range 130 {
		bounded.Log(kgo.LogLevelError, strings.Repeat("synthetic transport error ", 100))
	}
	require.Less(t, len(bounded.snapshot()), 128*2100)
	require.Contains(t, bounded.snapshot(), "[truncated]")
	require.Contains(t, bounded.snapshot(), "SDK diagnostic rows omitted: 2")
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
