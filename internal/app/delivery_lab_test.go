package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/pagrus7/kaferry/internal/sql"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type environment struct{}

func (environment) LookupEnv(key string) (string, bool) { return os.LookupEnv(key) }
func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func client(t *testing.T, brokers []string, options ...kgo.Opt) *kgo.Client {
	t.Helper()
	opts := append([]kgo.Opt{kgo.SeedBrokers(brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner())}, options...)
	c, err := kgo.NewClient(opts...)
	require.NoError(t, err)
	t.Cleanup(c.CloseAllowingRebalance)
	return c
}
func topic(t *testing.T, brokers []string, name string, parts int32, compact bool) {
	t.Helper()
	settings := map[string]*string{}
	if compact {
		value := "compact"
		settings["cleanup.policy"] = &value
	}
	probeClient := client(t, brokers)
	admin := kadm.NewClient(probeClient)
	ctx := testContext(t)
	for {
		result, err := admin.CreateTopics(ctx, parts, 1, settings, name)
		require.NoError(t, err)
		failure := result.Error()
		if !errors.Is(failure, kerr.InvalidReplicationFactor) {
			require.NoError(t, failure)
			// Every seed must have applied the created topic before random-seed startup checks.
			require.Eventually(t, func() bool {
				request := kmsg.NewPtrMetadataRequest()
				request.AllowAutoTopicCreation = false
				request.Topics = []kmsg.MetadataRequestTopic{{Topic: &name}}
				for _, broker := range probeClient.SeedBrokers() {
					probe, cancel := context.WithTimeout(ctx, time.Second)
					raw, err := broker.Request(probe, request)
					cancel()
					if err != nil {
						return false
					}
					metadata := raw.(*kmsg.MetadataResponse)
					if len(metadata.Topics) != 1 {
						return false
					}
					created := metadata.Topics[0]
					if created.ErrorCode != 0 || created.Topic == nil || *created.Topic != name || created.TopicID == [16]byte{} || created.TopicID != [16]byte(result[name].ID) || len(created.Partitions) != int(parts) {
						return false
					}
					for _, partition := range created.Partitions {
						if partition.ErrorCode != 0 || partition.Leader < 0 {
							return false
						}
					}
				}
				return true
			}, 10*time.Second, 10*time.Millisecond, "fixture topic is not ready on every seed")
			return
		}
		// A newly elected controller can briefly have no registered brokers.
		select {
		case <-ctx.Done():
			require.NoError(t, failure)
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
}
func query(t *testing.T, source, destination []string, predicate, options string) plan.Plan {
	t.Helper()
	text := fmt.Sprintf("INSERT INTO TOPIC 'output@dst' SELECT * FROM TOPIC 'input@src' WITH (GROUP = 'lab-%d') %s %s;", time.Now().UnixNano(), predicate, options)
	ast, err := sql.NewParser().Parse(text)
	require.NoError(t, err)
	p, err := plan.NewPlanner(environment{}).Build(ast, plan.Profiles{"src": {"bootstrap.servers": strings.Join(source, ","), "metadata.max.age.ms": "10", "session.timeout.ms": "6000", "heartbeat.interval.ms": "500"}, "dst": {"bootstrap.servers": strings.Join(destination, ",")}})
	require.NoError(t, err)
	return p
}
func clusters(t *testing.T, parts int32) ([]string, []string) {
	t.Helper()
	if sourceText, destinationText := os.Getenv("KAFERRY_LAB_SOURCE"), os.Getenv("KAFERRY_LAB_DESTINATION"); sourceText != "" && destinationText != "" {
		source, destination := strings.Split(sourceText, ","), strings.Split(destinationText, ",")
		for _, item := range []struct {
			brokers    []string
			name       string
			partitions int32
			compact    bool
		}{{source, "input", parts, false}, {destination, "output", parts, false}, {destination, "progress", 1, true}, {source, "progress", 1, true}} {
			topic(t, item.brokers, item.name, item.partitions, item.compact)
			c := client(t, item.brokers)
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_, _ = kadm.NewClient(c).DeleteTopics(ctx, item.name)
			})
		}
		return source, destination
	}
	source := kfake.MustCluster(kfake.NumBrokers(1))
	destination := kfake.MustCluster(kfake.NumBrokers(1))
	t.Cleanup(source.Close)
	t.Cleanup(destination.Close)
	topic(t, source.ListenAddrs(), "input", parts, false)
	topic(t, destination.ListenAddrs(), "output", parts, false)
	topic(t, destination.ListenAddrs(), "progress", 1, true)
	topic(t, source.ListenAddrs(), "progress", 1, true)
	return source.ListenAddrs(), destination.ListenAddrs()
}
func seed(t *testing.T, brokers []string, parts, perPart int) {
	t.Helper()
	c := client(t, brokers)
	records := make([]*kgo.Record, 0, parts*perPart)
	for part := range parts {
		for offset := range perPart {
			records = append(records, &kgo.Record{Topic: "input", Partition: int32(part), Key: []byte(fmt.Sprintf("key-%d", offset%4)), Value: []byte(fmt.Sprintf("%d:%d", part, offset)), Timestamp: time.Now().Add(-time.Minute)})
		}
	}
	require.NoError(t, c.ProduceSync(testContext(t), records...).FirstErr())
}

// committedRecords drains every selected partition through its captured stable
// end, including control records, so late duplicates cannot hide behind a set.
func committedRecords(t *testing.T, brokers []string, parts int, options ...kgo.Opt) []*kgo.Record {
	t.Helper()
	admin := kadm.NewClient(client(t, brokers, options...))
	ends, err := admin.ListCommittedOffsets(testContext(t), "output")
	require.NoError(t, err)
	require.NoError(t, ends.Error())
	result := []*kgo.Record{}
	for part := range parts {
		end := ends["output"][int32(part)].Offset
		if end == 0 {
			continue
		}
		readerOptions := append(append([]kgo.Opt{}, options...), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.KeepControlRecords(), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{"output": {int32(part): kgo.NewOffset().AtStart()}}))
		reader := client(t, brokers, readerOptions...)
		done := false
		for !done {
			fetch := reader.PollRecords(testContext(t), 1000)
			require.NoError(t, fetch.Err())
			for _, record := range fetch.Records() {
				if record.Offset >= end {
					done = true
					break
				}
				if !record.Attrs.IsControl() {
					result = append(result, record)
				}
				if record.Offset+1 >= end {
					done = true
				}
			}
		}
	}
	return result
}
func output(t *testing.T, brokers []string, parts int) map[string]int {
	return recordValues(committedRecords(t, brokers, parts))
}
func recordValues(records []*kgo.Record) map[string]int {
	result := map[string]int{}
	for _, record := range records {
		result[string(record.Value)]++
	}
	return result
}
func TestStreamingMultiplePartitions(t *testing.T) {
	// Given input larger than the bounded POC, across three source partitions.
	source, destination := clusters(t, 3)
	seed(t, source, 3, 1200)
	p := query(t, source, destination, "WHERE RECORD_OFFSET < LATEST", "")
	cfg := config.Defaults("")
	cfg.Worker.RecordCap = 100
	cfg.Worker.PartitionRecordCap = 50
	// When production source, sink and runner use destination-owned commits and resume.
	require.NoError(t, Run(testContext(t), p, cfg, Options{ProgressTopic: "progress"}))
	require.NoError(t, Run(testContext(t), p, cfg, Options{ProgressTopic: "progress"}))
	// Then all selected occurrences remain present exactly once.
	got := output(t, destination, 3)
	require.Len(t, got, 3600)
	for part := range 3 {
		for offset := range 1200 {
			require.Equal(t, 1, got[fmt.Sprintf("%d:%d", part, offset)])
		}
	}
}
