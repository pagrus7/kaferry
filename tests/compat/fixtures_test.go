//go:build integration

package compat

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func fixtureClient(t *testing.T, brokers string, options ...kgo.Opt) *kgo.Client {
	t.Helper()
	// Historical timestamps are deliberate; publication deadlines use contexts.
	defaults := []kgo.Opt{kgo.SeedBrokers(strings.Split(brokers, ",")...), kgo.ClientID("kaferry-fixture"), kgo.RetryTimeout(10 * time.Second), kgo.UnknownTopicRetries(5), kgo.RecordRetries(5), kgo.ConsumeResetOffset(kgo.NoResetOffset())}
	client, err := kgo.NewClient(append(defaults, options...)...)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	return client
}

func admin(t *testing.T) *kadm.Client {
	t.Helper()
	return kadm.NewClient(fixtureClient(t, broker(t)))
}

func topic(t *testing.T, a *kadm.Client, partitions int) string {
	t.Helper()
	name := fmt.Sprintf("kaferry-compat-%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err := a.CreateTopic(ctx, int32(partitions), 1, map[string]*string{"message.timestamp.before.max.ms": kadm.StringPtr("9223372036854775807"), "message.timestamp.after.max.ms": kadm.StringPtr("9223372036854775807")}, name)
	require.NoError(t, err, "create fixture topic")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = a.DeleteTopics(ctx, name)
	})
	waitTopicReady(t, a, name, partitions)
	return name
}

func waitTopicReady(t *testing.T, a *kadm.Client, name string, partitions int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		metadata, err := a.ListTopics(ctx, name)
		topic := metadata[name]
		ready := err == nil && topic.Err == nil && len(topic.Partitions) == partitions
		for _, p := range topic.Partitions {
			ready = ready && p.Err == nil && p.Leader >= 0
		}
		if ready {
			offsets, err := a.ListStartOffsets(ctx, name)
			if err == nil && offsets.Error() == nil && len(offsets[name]) == partitions {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("fixture topic %s not ready: %v %+v", name, err, metadata)
		case <-tick.C:
		}
	}
}

func alterTopicConfig(t *testing.T, a *kadm.Client, name string, settings map[string]string) {
	t.Helper()
	var configs []kadm.AlterConfig
	for key, value := range settings {
		configs = append(configs, kadm.AlterConfig{Name: key, Value: kadm.StringPtr(value)})
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	results, err := a.AlterTopicConfigs(ctx, configs, name)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.NoError(t, results[0].Err)
	for key, value := range settings {
		waitTopicConfig(t, a, name, key, value)
	}
}

func waitTopicConfig(t *testing.T, a *kadm.Client, name, key, value string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		results, err := a.DescribeTopicConfigs(ctx, name)
		if err == nil && len(results) == 1 && results[0].Err == nil {
			for _, c := range results[0].Configs {
				if c.Key == key && c.Value != nil && *c.Value == value {
					return
				}
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("fixture config %s/%s not ready: %v %+v", name, key, err, results)
		case <-tick.C:
		}
	}
}

func producer(t *testing.T, options ...kgo.Opt) *kgo.Client {
	t.Helper()
	return fixtureClient(t, broker(t), append([]kgo.Opt{kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.ProducerLinger(10 * time.Millisecond), kgo.ProducerBatchCompression(kgo.NoCompression())}, options...)...)
}

func consumer(t *testing.T) *kgo.Client {
	t.Helper()
	return fixtureClient(t, broker(t), kgo.FetchIsolationLevel(kgo.ReadCommitted()))
}

func assign(t *testing.T, client *kgo.Client, topic string, partitions int) {
	t.Helper()
	offsets := make(map[int32]kgo.Offset, partitions)
	for id := range partitions {
		offsets[int32(id)] = kgo.NewOffset().At(0)
	}
	client.AddConsumePartitions(map[string]map[int32]kgo.Offset{topic: offsets})
}

func publish(t *testing.T, client *kgo.Client, record *kgo.Record) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	require.NoError(t, client.ProduceSync(ctx, record).FirstErr(), "publish fixture record")
}

func read(t *testing.T, client *kgo.Client) *kgo.Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	for {
		fetches := client.PollRecords(ctx, 1)
		require.Empty(t, fetches.Errors(), "read fixture record")
		records := fetches.Records()
		if len(records) > 0 {
			return records[0]
		}
		require.NoError(t, ctx.Err(), "expected a fixture record before timeout")
	}
}

func topicRecordCount(t *testing.T, a *kadm.Client, topic string, partitions int) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ends, err := a.ListEndOffsets(ctx, topic)
	require.NoError(t, err)
	require.NoError(t, ends.Error())
	require.Len(t, ends[topic], partitions)
	var count int64
	for _, p := range ends[topic] {
		count += p.Offset
	}
	return count
}

func deliveryReporter(ctx context.Context, results chan<- kgo.ProduceResult) func(*kgo.Record, error) {
	return func(record *kgo.Record, err error) {
		select {
		case results <- kgo.ProduceResult{Record: record, Err: err}:
		case <-ctx.Done():
		}
	}
}
