package franz

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
	"go.uber.org/goleak"
)

// runComponentCopy exercises the production adapters and runner without the CLI.
func runComponentCopy(ctx context.Context, p plan.Plan, limits config.Worker) (err error) {
	factory := componentFactory()
	sourceConnection, err := factory.Open(ctx, p.Source)
	if err != nil {
		return err
	}
	defer sourceConnection.Close(context.Background())
	if _, err = sourceConnection.ClusterID(ctx); err != nil {
		return err
	}
	destination, err := factory.Open(ctx, p.Destination)
	if err != nil {
		return err
	}
	defer destination.Close(context.Background())
	if _, err = destination.ClusterID(ctx); err != nil {
		return err
	}
	if err = configureComponentOutput(ctx, factory, p, sourceConnection, destination); err != nil {
		return err
	}
	defer func() { err = errors.Join(err, factory.session.Close()) }()
	source, store, err := sourceConnection.Source(ctx, p, "component-fingerprint", limits, nil)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, source.Close()) }()
	sink, err := destination.Sink(ctx, limits)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, factory.session.Close()); sink.Close() }()
	return core.NewRunner(source, sink, store, core.RealClock{}, quietLogger(), p, limits, core.NewTracker(), core.NoopStopObserver{}, nil).Run(ctx)
}
func boundedComponentPlan(t *testing.T, cluster *kfake.Cluster) plan.Plan {
	t.Helper()
	p := componentPlan(t, cluster)
	p.Start = ""
	p.Window.Upper = &plan.Expression{LiteralMS: time.Now().Add(-30 * time.Minute).UnixMilli()}
	return p
}
func readN(t *testing.T, c *kgo.Client, count int) []*kgo.Record {
	t.Helper()
	var records []*kgo.Record
	deadline := componentContext(t)
	for len(records) < count {
		fetch := c.PollRecords(deadline, count-len(records))
		require.NoError(t, fetch.Err())
		records = append(records, fetch.Records()...)
	}
	return records
}
func holdRequests(c *kfake.Cluster, key int16) <-chan struct{} {
	observed := make(chan struct{}, 1)
	c.ControlKey(key, func(kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		select {
		case observed <- struct{}{}:
		default:
		}
		// A handled request with no response leaves the client waiting on the network.
		return nil, nil, true
	})
	return observed
}
func await(t *testing.T, done <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal(label + " did not finish within 2 seconds")
	}
}
func TestCompactedOffsetsReachTheBoundedEnd(t *testing.T) {
	for _, scenario := range []struct {
		retained []int32
		version  int16
	}{{nil, 17}, {[]int32{0}, 17}, {[]int32{0, 5}, 17}, {nil, 12}} {
		retained := scenario.retained
		t.Run(fmt.Sprintf("retained=%v_version=%d", retained, scenario.version), func(t *testing.T) {
			// Given a legal compacted batch whose last offset survives without a record.
			versions := kversion.V3_9_0()
			versions.SetMaxKeyVersion(int16(kmsg.Fetch), scenario.version)
			cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "input", "output"), kfake.MaxVersions(versions))
			defer cluster.Close()
			producer, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
			require.NoError(t, err)
			defer producer.Close()
			for range 10 {
				require.NoError(t, producer.ProduceSync(componentContext(t), &kgo.Record{Topic: "input", Value: []byte("compacted")}).FirstErr())
			}
			batch := compactedBatch(retained...)
			metadata, err := kadm.NewClient(producer).Metadata(componentContext(t), "input")
			require.NoError(t, err)
			id := metadata.Topics["input"].ID
			cluster.ControlKey(int16(kmsg.Fetch), func(request kmsg.Request) (kmsg.Response, error, bool) {
				cluster.KeepControl()
				req := request.(*kmsg.FetchRequest)
				res := req.ResponseKind().(*kmsg.FetchResponse)
				for _, topic := range req.Topics {
					if topic.Topic != "input" && topic.TopicID != id {
						continue
					}
					result := kmsg.FetchResponseTopic{Topic: topic.Topic, TopicID: topic.TopicID}
					for _, part := range topic.Partitions {
						p := kmsg.NewFetchResponseTopicPartition()
						p.Partition, p.HighWatermark, p.LastStableOffset, p.LogStartOffset = part.Partition, 10, 10, 0
						if part.FetchOffset < 10 {
							p.RecordBatches = batch
						}
						result.Partitions = append(result.Partitions, p)
					}
					res.Topics = append(res.Topics, result)
				}
				return res, nil, len(res.Topics) > 0
			})
			p := boundedComponentPlan(t, cluster)
			deadline, cancel := context.WithTimeout(componentContext(t), 4*time.Second)
			defer cancel()
			// When reading through the compacted tail with a bounded time range.
			err = runComponentCopy(deadline, p, config.Defaults(t.TempDir()).Worker)
			// Then the batch's consumed end is saved, and every retained record is copied.
			if err != nil || deadline.Err() != nil {
				t.Fatalf("did not complete across compacted offsets: copy=%v context=%v", err, deadline.Err())
			}
			saved, err := kadm.NewClient(producer).FetchOffsets(componentContext(t), p.Group)
			if err != nil || saved["input"][0].At != 10 {
				t.Fatalf("wrong consumed end: %v %v", saved, err)
			}
			want := int64(len(retained))
			if want > 0 {
				want++
			} // committed output includes its transaction marker
			if got := cluster.PartitionInfo("output", 0).HighWatermark; got != want {
				t.Fatalf("copied %d records, wanted %d", got, want)
			}
			producer.AddConsumePartitions(map[string]map[int32]kgo.Offset{"output": {0: kgo.NewOffset().AtStart()}})
			for index, record := range readN(t, producer, len(retained)) {
				if got, want := string(record.Value), fmt.Sprintf("retained-%d", retained[index]); got != want {
					t.Fatalf("retained record %d changed: got %q, wanted %q", index, got, want)
				}
			}
			// When the same query restarts at the verified end.
			restart := componentContext(t)
			if err := runComponentCopy(restart, p, config.Defaults(t.TempDir()).Worker); err != nil || restart.Err() != nil {
				t.Fatalf("restart did not complete: copy=%v context=%v", err, restart.Err())
			}
			// Then completed compacted offsets do not produce another write.
			if got := cluster.PartitionInfo("output", 0).HighWatermark; got != want {
				t.Fatalf("restart duplicated retained data: %d", got)
			}
		})
	}
}

func TestProductionCopyClosesBlockedStartupAndFetchRequests(t *testing.T) {
	for _, operation := range []struct {
		name string
		key  kmsg.Key
	}{
		{"coordinator", kmsg.FindCoordinator}, {"join", kmsg.JoinGroup}, {"offset fetch", kmsg.OffsetFetch}, {"offset lookup", kmsg.ListOffsets}, {"initial commit", kmsg.OffsetCommit}, {"fetch", kmsg.Fetch},
	} {
		t.Run(operation.name, func(t *testing.T) {
			defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
			// Given a production copy whose selected request receives no response.
			cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "input", "output"), kfake.MaxVersions(kversion.V3_9_0()))
			defer cluster.Close()
			producer := componentClient(t, cluster)
			defer producer.Close()
			require.NoError(t, producer.ProduceSync(componentContext(t), &kgo.Record{Topic: "input", Value: []byte("one"), Timestamp: time.Now().Add(-time.Hour)}).FirstErr())
			observed := holdRequests(cluster, int16(operation.key))
			deadline, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
			defer cancel()
			result := make(chan error, 1)
			p, limits := boundedComponentPlan(t, cluster), config.Defaults("").Worker
			// When the query is cancelled while the request is blocked.
			go func() { result <- runComponentCopy(deadline, p, limits) }()
			await(t, observed, "request interception")
			// Then the runner and client cleanup return without a broker response.
			select {
			case err := <-result:
				require.Error(t, err, "blocked copy succeeded")
			case <-time.After(8 * time.Second):
				t.Fatal("blocked request prevented cleanup")
			}
		})
	}
}
