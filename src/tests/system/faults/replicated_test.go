//go:build integration

package faults

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func proxyRoutes(cluster string, worker int) map[int32]string {
	routes := make(map[int32]string)
	for index, endpoint := range Endpoints(cluster, worker) {
		routes[int32(index+1)] = endpoint
	}
	return routes
}

func TestReplicatedProxyRoutes(t *testing.T) {
	// Given two replicated clusters with independent admin and worker routes.
	metadata, err := NewMetadata()
	require.NoError(t, err)
	defer metadata.Close()
	lab := &Lab{Project: os.Getenv("KAFERRY_FIXTURE_PROJECT"), API: os.Getenv("KAFERRY_TOXIPROXY_API"), Metadata: metadata}
	require.NotEmpty(t, lab.Project)
	require.NotEmpty(t, lab.API)
	var events []Event
	for _, cluster := range []string{"source", "destination"} {
		for worker := 1; worker <= 3; worker++ {
			t.Run(fmt.Sprintf("%s/worker%d", cluster, worker), func(t *testing.T) {
				proveProxyRoute(t, lab, metadata[cluster].Client, cluster, worker, func(event Event) { events = append(events, event) })
			})
		}
	}
	data, err := json.MarshalIndent(events, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(os.Getenv("KAFERRY_REPORT_DIR"), "proxy-events.json"), append(data, '\n'), 0o600))
}

func proveProxyRoute(t *testing.T, lab *Lab, admin *kadm.Client, cluster string, worker int, record func(Event)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	defer cancel()
	topic := fmt.Sprintf("kaferry-proxy-%s-%d-%d", cluster, worker, time.Now().UnixNano())
	_, err := admin.CreateTopic(ctx, 3, 3, nil, topic)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 15*time.Second)
		defer done()
		_, deleteErr := admin.DeleteTopics(cleanup, topic)
		require.NoError(t, deleteErr)
	})
	// Bootstrap on broker 1, then let Kafka metadata discover a different leader.
	workerSeed := Endpoints(cluster, worker)[0]
	diagnostic := &SDKDiagnostics{}
	t.Cleanup(func() { t.Logf("bounded producer SDK diagnostics:\n%s", diagnostic.Snapshot()) })
	client, err := newProxyProducer([]string{workerSeed}, diagnostic)
	require.NoError(t, err)
	closeProbe := sync.OnceFunc(client.Close)
	defer closeProbe()
	partition := proxyPartition(ctx, t, kadm.NewClient(client), topic)
	require.NoError(t, prepareProxyProducer(ctx, client, topic, partition, proxyRoutes(cluster, worker), t.Logf))
	produce := func(producer *kgo.Client, value string) error {
		return producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: partition, Value: []byte(value)}).FirstErr()
	}
	observer, err := kgo.NewClient(kgo.SeedBrokers(Endpoints(cluster, 0)...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	require.NoError(t, err)
	defer observer.Close()
	require.NoError(t, prepareProxyProducer(ctx, observer, topic, partition, proxyRoutes(cluster, 0), t.Logf))
	require.NoError(t, produce(client, "before"))
	// When the metadata-discovered leader's route is cut, that worker cannot write.
	injection, err := lab.Inject(ctx, Role{Kind: "leader", Cluster: cluster, Topic: topic, Partition: partition, Worker: worker}, Fault{Kind: "cut"})
	if injection != nil {
		t.Cleanup(func() {
			cleanup, done := context.WithTimeout(context.Background(), 15*time.Second)
			defer done()
			healErr := injection.Heal(cleanup)
			record(injection.Event)
			require.NoError(t, healErr)
		})
	}
	require.NoError(t, err)
	require.NotEqual(t, int32(1), injection.Event.Targets[0].Broker)
	require.Error(t, produce(client, "blocked"), "success means metadata bypassed the worker proxy")
	// Discard all diagnostic-producer state before restoring its route, so no
	// uncertain buffered record can retry after healing.
	closeProbe()
	// Then the independent route still writes to the same partition, and healing
	// restores the worker route. Reconcile the actual output through that route.
	require.NoError(t, produce(observer, "observer"))
	require.NoError(t, injection.Heal(ctx))
	recovered, err := newProxyProducer([]string{workerSeed}, diagnostic)
	require.NoError(t, err)
	defer recovered.Close()
	require.NoError(t, prepareProxyProducer(ctx, recovered, topic, partition, proxyRoutes(cluster, worker), t.Logf))
	require.NoError(t, produce(recovered, "after"))
	reader, err := kgo.NewClient(kgo.SeedBrokers(Endpoints(cluster, 0)...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {partition: kgo.NewOffset().AtStart()}}))
	require.NoError(t, err)
	defer reader.Close()
	var values []string
	for len(values) < 3 && ctx.Err() == nil {
		fetches := reader.PollRecords(ctx, 10)
		require.Empty(t, fetches.Errors())
		fetches.EachRecord(func(record *kgo.Record) { values = append(values, string(record.Value)) })
	}
	require.Equal(t, []string{"before", "observer", "after"}, values)
	ends, err := admin.ListEndOffsets(ctx, topic)
	require.NoError(t, err)
	require.NoError(t, ends.Error())
	require.Equal(t, int64(3), ends[topic][partition].Offset, "blocked publication must not reach Kafka")
}

func proxyPartition(ctx context.Context, t *testing.T, admin *kadm.Client, topic string) int32 {
	t.Helper()
	t.Log("producer setup: select a non-bootstrap partition through this producer's route")
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		metadata, err := admin.Metadata(ctx, topic)
		if err == nil {
			for _, partition := range metadata.Topics[topic].Partitions.Sorted() {
				if partition.Err == nil && partition.Leader > 1 && len(partition.ISR) == 3 {
					return partition.Partition
				}
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("no ready partition led by a non-bootstrap broker", err)
		case <-tick.C:
		}
	}
}

func prepareProxyProducer(ctx context.Context, client *kgo.Client, topic string, partition int32, routes map[int32]string, logf func(string, ...any)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	logf("producer setup: discover route brokers")
	if err := client.Ping(ctx); err != nil {
		return fmt.Errorf("discover producer route: %w", err)
	}
	ids := make([]int32, 0, len(routes))
	for id := range routes {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	leader := int32(-1)
	for {
		known, err := proxyProducerLeader(ctx, client, topic, partition, ids, routes, logf)
		if err != nil {
			return err
		}
		if known >= 0 {
			leader = known
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
	logf("producer setup: initialize producer ID")
	if _, _, err := client.ProducerID(ctx); err != nil {
		return fmt.Errorf("initialize diagnostic producer ID: %w", err)
	}
	logf("producer setup: open leader %d produce connection", leader)
	if err := client.EnsureProduceConnectionIsOpen(ctx, leader); err != nil {
		return fmt.Errorf("open diagnostic produce connection: %w", err)
	}
	logf("producer setup: ready for one proof write")
	return nil
}

func proxyProducerLeader(ctx context.Context, client *kgo.Client, topic string, partition int32, ids []int32, routes map[int32]string, logf func(string, ...any)) (int32, error) {
	leader := int32(-1)
	for _, id := range ids {
		request := kmsg.NewPtrMetadataRequest()
		request.Topics = []kmsg.MetadataRequestTopic{{Topic: &topic}}
		response, err := client.Broker(int(id)).Request(ctx, request)
		if err != nil {
			return -1, fmt.Errorf("observe producer metadata on broker %d: %w", id, err)
		}
		known, err := proxyMetadataLeader(response.(*kmsg.MetadataResponse), topic, partition, routes)
		logf("producer setup: broker %d observed leader %d, error %v", id, known, err)
		if errors.Is(err, kerr.UnknownTopicOrPartition) || errors.Is(err, kerr.LeaderNotAvailable) {
			return -1, nil
		}
		if err != nil {
			return -1, err
		}
		if known < 0 || leader >= 0 && known != leader {
			return -1, nil
		}
		leader = known
	}
	return leader, nil
}

func proxyMetadataLeader(response *kmsg.MetadataResponse, topic string, partition int32, routes map[int32]string) (int32, error) {
	if err := kerr.ErrorForCode(response.ErrorCode); err != nil {
		return -1, err
	}
	if err := verifyProducerRoute(response.Brokers, routes); err != nil {
		return -1, err
	}
	for _, detail := range response.Topics {
		if detail.Topic == nil || *detail.Topic != topic {
			continue
		}
		if err := kerr.ErrorForCode(detail.ErrorCode); err != nil {
			return -1, err
		}
		for _, part := range detail.Partitions {
			if part.Partition == partition {
				return proxyPartitionLeader(part, routes)
			}
		}
	}
	return -1, nil
}

func verifyProducerRoute(brokers []kmsg.MetadataResponseBroker, routes map[int32]string) error {
	if len(brokers) != len(routes) {
		return errors.New("producer metadata does not describe the whole selected route")
	}
	seen := make(map[int32]bool)
	for _, broker := range brokers {
		address := net.JoinHostPort(broker.Host, strconv.Itoa(int(broker.Port)))
		if seen[broker.NodeID] || routes[broker.NodeID] != address {
			return fmt.Errorf("metadata broker %d bypasses the selected producer route", broker.NodeID)
		}
		seen[broker.NodeID] = true
	}
	return nil
}

func proxyPartitionLeader(part kmsg.MetadataResponseTopicPartition, routes map[int32]string) (int32, error) {
	if err := kerr.ErrorForCode(part.ErrorCode); err != nil {
		return -1, err
	}
	replicas, isr := slices.Clone(part.Replicas), slices.Clone(part.ISR)
	slices.Sort(replicas)
	slices.Sort(isr)
	if part.Leader < 0 || len(replicas) != 3 || len(slices.Compact(replicas)) != 3 || !slices.Equal(replicas, isr) {
		return -1, nil
	}
	if _, ok := routes[part.Leader]; !ok {
		return -1, errors.New("partition leader is outside the selected producer route")
	}
	return part.Leader, nil
}

func newProxyProducer(seeds []string, diagnostic kgo.Logger, hooks ...kgo.Hook) (*kgo.Client, error) {
	// A diagnostic write must be cancellable even after an ambiguous request.
	// Close this producer before healing; output reconciliation still rejects
	// any cancelled record that actually reached Kafka.
	// The SDK's 5s metadata minimum age exceeds this probe's 2s delivery bound.
	// Its valid 10ms minimum permits native retries inside the existing bound.
	options := []kgo.Opt{kgo.SeedBrokers(seeds...), kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.AllowIdempotentProduceCancellation(), kgo.RecordDeliveryTimeout(2 * time.Second),
		kgo.MetadataMinAge(10 * time.Millisecond), kgo.ProduceRequestTimeout(time.Second), kgo.RequestTimeoutOverhead(time.Second), kgo.WithHooks(hooks...)}
	if diagnostic != nil {
		options = append(options, kgo.WithLogger(diagnostic))
	}
	return kgo.NewClient(options...)
}
