package franz

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/src/internal/config"
	core "github.com/pagrus7/kaferry/src/internal/copy"
	"github.com/pagrus7/kaferry/src/internal/plan"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
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
	if _, err = sourceConnection.Describe(ctx, false); err != nil {
		return err
	}
	destination, err := factory.Open(ctx, p.Destination)
	if err != nil {
		return err
	}
	defer destination.Close(context.Background())
	if _, err = destination.Describe(ctx, false); err != nil {
		return err
	}
	source, store, err := sourceConnection.Source(ctx, p, "component-fingerprint", limits, nil)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, source.Close()) }()
	sink, err := destination.Sink(ctx, limits)
	if err != nil {
		return err
	}
	defer sink.Close()
	return core.NewRunner(source, sink, store, core.RealClock{}, quietLogger(), p, componentRunnerSettings(limits), core.NewTracker(), core.NoopStopObserver{}, nil).Run(ctx)
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

func componentSink(t *testing.T, cluster *kfake.Cluster, limits config.Worker) core.Sink {
	t.Helper()
	p := componentPlan(t, cluster)
	connection, err := componentFactory().Open(componentContext(t), p.Destination)
	require.NoError(t, err)
	t.Cleanup(func() { _ = connection.Close(context.Background()) })
	_, err = connection.Describe(componentContext(t), false)
	require.NoError(t, err)
	sink, err := connection.Sink(componentContext(t), limits)
	require.NoError(t, err)
	t.Cleanup(sink.Close)
	return sink
}

func TestProductionSinkRetriesNegotiationEOFAfterConnectionLoss(t *testing.T) {
	// Given the production sink with an acknowledged write on its established connection.
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "input", "output"))
	t.Cleanup(cluster.Close)
	limits := config.Defaults("").Worker
	limits.DeliveryTimeout.Duration = 2 * time.Second
	sink := componentSink(t, cluster, limits)
	require.NoError(t, sink.Submit(t.Context(), core.Record{Value: []byte("warmup")}, core.Token{}))
	require.NoError(t, receiveDelivery(t, sink).Err)
	cut := make(chan struct{})
	cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		return nil, io.EOF, true
	})
	cluster.ControlKey(int16(kmsg.ApiVersions), func(kmsg.Request) (kmsg.Response, error, bool) {
		close(cut)
		return nil, io.EOF, true
	})
	// When the established connection and the replacement negotiation each close once,
	// submit each accepted record only once and let the native producer retry it.
	want := []string{"warmup", "first", "second", "third"}
	for index, value := range want[1:] {
		require.NoError(t, sink.Submit(t.Context(), core.Record{Value: []byte(value)}, core.Token{Sequence: uint64(index + 1)}))
	}
	await(t, cut, "replacement negotiation")
	for index := range want[1:] {
		delivery := receiveDelivery(t, sink)
		require.NoError(t, delivery.Err)
		require.Equal(t, core.Token{Sequence: uint64(index + 1)}, delivery.Token)
	}
	// Then the original offers are confirmed in order, with no extra destination writes.
	require.EqualValues(t, len(want), cluster.PartitionInfo("output", 0).HighWatermark)
	reader := componentClient(t, cluster, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{"output": {0: kgo.NewOffset().AtStart()}}))
	for index, record := range readN(t, reader, len(want)) {
		require.Equal(t, want[index], string(record.Value))
		require.EqualValues(t, index, record.Offset)
	}
}

func TestProductionSinkNegotiationEOFRetriesRemainBounded(t *testing.T) {
	for _, closing := range []bool{false, true} {
		t.Run(fmt.Sprintf("close=%t", closing), func(t *testing.T) {
			// Given an established production sink whose replacement negotiations all close.
			cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "input", "output"))
			t.Cleanup(cluster.Close)
			limits := config.Defaults("").Worker
			limits.DeliveryTimeout.Duration = 500 * time.Millisecond
			sink := componentSink(t, cluster, limits)
			require.NoError(t, sink.Submit(t.Context(), core.Record{Value: []byte("warmup")}, core.Token{}))
			require.NoError(t, receiveDelivery(t, sink).Err)
			cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
				return nil, io.EOF, true
			})
			cut := make(chan struct{}, 1)
			cluster.ControlKey(int16(kmsg.ApiVersions), func(kmsg.Request) (kmsg.Response, error, bool) {
				cluster.KeepControl()
				select {
				case cut <- struct{}{}:
				default:
				}
				return nil, io.EOF, true
			})
			// When negotiation cannot recover, the existing delivery deadline or close cancels it.
			started := time.Now()
			token := core.Token{Sequence: 1}
			require.NoError(t, sink.Submit(t.Context(), core.Record{Value: []byte("unconfirmed")}, token))
			await(t, cut, "replacement negotiation")
			if closing {
				// Release fake connections before sink cleanup if close does not return.
				t.Cleanup(cluster.Close)
				closed := make(chan struct{})
				go func() {
					sink.Close()
					close(closed)
				}()
				await(t, closed, "sink close")
			}
			// Then a failed delivery remains visible, does not append output and finishes promptly.
			delivery := receiveDelivery(t, sink)
			require.Equal(t, token, delivery.Token)
			if closing {
				require.True(t, errors.Is(delivery.Err, context.Canceled) || errors.Is(delivery.Err, kgo.ErrClientClosed), "close reported %v", delivery.Err)
			} else {
				require.ErrorIs(t, delivery.Err, context.DeadlineExceeded)
			}
			require.Less(t, time.Since(started), 2*time.Second)
			require.EqualValues(t, 1, cluster.PartitionInfo("output", 0).HighWatermark)
		})
	}
}

func TestProductionSinkDoesNotRetryPermanentRejection(t *testing.T) {
	// Given the production sink and a broker that rejects every produce with an authorization error.
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "input", "output"))
	t.Cleanup(cluster.Close)
	sink := componentSink(t, cluster, config.Defaults("").Worker)
	var attempts atomic.Int64
	cluster.ControlKey(int16(kmsg.Produce), func(request kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		attempts.Add(1)
		req := request.(*kmsg.ProduceRequest)
		response := req.ResponseKind().(*kmsg.ProduceResponse)
		for _, topic := range req.Topics {
			result := kmsg.ProduceResponseTopic{Topic: topic.Topic, TopicID: topic.TopicID}
			for _, partition := range topic.Partitions {
				result.Partitions = append(result.Partitions, kmsg.ProduceResponseTopicPartition{Partition: partition.Partition, ErrorCode: kerr.TopicAuthorizationFailed.Code})
			}
			response.Topics = append(response.Topics, result)
		}
		return response, nil, true
	})
	// When one original offer is accepted by the adapter, then its permanent failure is reported once.
	token := core.Token{Sequence: 1}
	require.NoError(t, sink.Submit(t.Context(), core.Record{Value: []byte("rejected")}, token))
	delivery := receiveDelivery(t, sink)
	require.Equal(t, token, delivery.Token)
	require.ErrorIs(t, delivery.Err, kerr.TopicAuthorizationFailed)
	require.EqualValues(t, 1, attempts.Load())
	require.Zero(t, cluster.PartitionInfo("output", 0).HighWatermark)
}

func TestAmbiguousDeliveryReplaysWithoutLosingProgress(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	// Given three source records and a broker whose second produce reply is lost.
	gate := &replyGate{}
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "input", "output"), kfake.MaxVersions(kversion.V3_9_0()), kfake.ListenFn(gate.listen))
	defer cluster.Close()
	producer, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	require.NoError(t, err)
	defer producer.Close()
	stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	for _, value := range []string{"confirmed", "uncertain", "later"} {
		require.NoError(t, producer.ProduceSync(componentContext(t), &kgo.Record{Topic: "input", Key: []byte("key"), Value: []byte(value), Timestamp: stamp}).FirstErr())
	}
	p := boundedComponentPlan(t, cluster)
	limits := config.Defaults(t.TempDir()).Worker
	limits.DeliveryTimeout.Duration = 200 * time.Millisecond
	limits.RecordCap = 1
	gate.after.Store(2)

	// When the copy times out after the destination has accepted its record.
	started := time.Now()
	err = runComponentCopy(componentContext(t), p, limits)
	require.ErrorIs(t, err, context.DeadlineExceeded, "wanted delivery deadline, got %v", err)
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("failed delivery delayed shutdown: %s", elapsed)
	}
	admin := kadm.NewClient(producer)
	saved, err := admin.FetchOffsets(componentContext(t), p.Group)
	if err != nil || saved["input"][0].At != 1 {
		t.Fatalf("ambiguous write advanced source progress: %v %v", saved, err)
	}
	if end := cluster.PartitionInfo("output", 0).HighWatermark; end != 2 || gate.dropped.Load() == 0 {
		t.Fatalf("fixture did not store the unacknowledged second record: end=%d dropped=%d", end, gate.dropped.Load())
	}
	// When the destination recovers and the same query restarts.
	gate.after.Store(0)
	limits.DeliveryTimeout.Duration = 5 * time.Second
	require.NoError(t, runComponentCopy(componentContext(t), p, limits))
	// Then the record replays, producing the permitted duplicate and saving its next offset.
	saved, err = admin.FetchOffsets(componentContext(t), p.Group)
	if err != nil || saved["input"][0].At != 3 {
		t.Fatalf("replay did not save confirmed progress: %v %v", saved, err)
	}
	if end := cluster.PartitionInfo("output", 0).HighWatermark; end != 4 {
		t.Fatalf("wanted three source records plus one replay, got %d", end)
	}
	reader, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{"output": {0: kgo.NewOffset().AtStart()}}))
	require.NoError(t, err)
	defer reader.Close()
	for i, record := range readN(t, reader, 4) {
		want := []string{"confirmed", "uncertain", "uncertain", "later"}[i]
		if string(record.Key) != "key" || string(record.Value) != want || record.Timestamp.UnixMilli() != stamp.UnixMilli() {
			t.Fatalf("replay changed record: %+v", record)
		}
	}
}

func TestCompactedOffsetsReachTheBoundedEnd(t *testing.T) {
	for _, scenario := range []struct {
		retained []int32
		version  int16
	}{{nil, 17}, {[]int32{0}, 17}, {[]int32{0, 5}, 17}, {nil, 12}} {
		retained := scenario.retained
		t.Run(fmt.Sprintf("retained=%v_version=%d", retained, scenario.version), func(t *testing.T) {
			compactedOffsetCompletion(t, retained, scenario.version)
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

func compactedOffsetCompletion(t *testing.T, retained []int32, version int16) {
	// Given a legal compacted batch whose last offset survives without a record.
	versions := kversion.V3_9_0()
	versions.SetMaxKeyVersion(int16(kmsg.Fetch), version)
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
		response := compactedFetchResponse(request, id, batch)
		return response, nil, len(response.Topics) > 0
	})
	p := boundedComponentPlan(t, cluster)
	deadline, cancel := context.WithTimeout(componentContext(t), 4*time.Second)
	defer cancel()
	// When reading through the compacted tail with a bounded time range.
	err = runComponentCopy(deadline, p, config.Defaults(t.TempDir()).Worker)
	// Then the batch's consumed end is saved, and every retained record is copied.
	require.NoError(t, err)
	require.NoError(t, deadline.Err())

	saved, err := kadm.NewClient(producer).FetchOffsets(componentContext(t), p.Group)
	require.NoError(t, err)
	require.EqualValues(t, 10, saved["input"][0].At)

	want := int64(len(retained))
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
}

func compactedFetchResponse(request kmsg.Request, id [16]byte, batch []byte) *kmsg.FetchResponse {
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
	return res
}
