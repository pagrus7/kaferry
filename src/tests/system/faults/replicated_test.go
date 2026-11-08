//go:build integration

package faults

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

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
				proveProxyRoute(t, lab, metadata[cluster], cluster, worker, func(event Event) { events = append(events, event) })
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
	partition := proxyPartition(ctx, t, admin, topic)
	// Bootstrap on broker 1, then let Kafka metadata discover a different leader.
	workerSeed := Endpoints(cluster, worker)[0]
	client, err := proxyProducer(workerSeed)
	require.NoError(t, err)
	closeProbe := sync.OnceFunc(client.Close)
	defer closeProbe()
	produce := func(producer *kgo.Client, value string) error {
		return producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: partition, Value: []byte(value)}).FirstErr()
	}
	observer, err := kgo.NewClient(kgo.SeedBrokers(Endpoints(cluster, 0)...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	require.NoError(t, err)
	defer observer.Close()
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
	recovered, err := proxyProducer(workerSeed)
	require.NoError(t, err)
	defer recovered.Close()
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
