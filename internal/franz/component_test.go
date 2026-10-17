package franz

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/commitmeta"
	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/goleak"
)

func componentFactory() *Factory {
	return NewFactory(commitmeta.NewCodec(), NewClients(), kafkaport.NewPositionerFactory(core.RealClock{}, quietLogger()), core.RealClock{}, quietLogger())
}
func componentPlan(t *testing.T, cluster *kfake.Cluster) plan.Plan {
	return testPlan(t, map[string]string{"bootstrap.servers": strings.Join(cluster.ListenAddrs(), ",")})
}
func componentConnection(t *testing.T, p plan.Plan) (*Factory, kafkaport.Connection) {
	t.Helper()
	factory := componentFactory()
	connection, err := factory.Open(t.Context(), p.Source)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close(context.Background()) })
	if _, err := connection.ClusterID(t.Context()); err != nil {
		t.Fatal(err)
	}
	return factory, connection
}
func componentClient(t *testing.T, cluster *kfake.Cluster, options ...kgo.Opt) *kgo.Client {
	t.Helper()
	client, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(cluster.ListenAddrs()...)}, options...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.CloseAllowingRebalance)
	return client
}
func componentContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestConsumerConstructionDoesNotJoinBeforeStartAndCanClose(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	// Given a real group client configured before the destination is opened.
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "input", "output"))
	defer cluster.Close()
	var joins atomic.Int64
	cluster.ControlKey(int16(kmsg.JoinGroup), func(kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		joins.Add(1)
		return nil, nil, false
	})
	p := componentPlan(t, cluster)
	factory := componentFactory()
	connection, err := factory.Open(t.Context(), p.Source)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(context.Background())
	input, _, err := connection.Source(t.Context(), p, "fp", config.Defaults("").Worker)
	if err != nil {
		t.Fatal(err)
	}
	source := input.(*Source)
	if source.topicID == "" {
		t.Fatal("source constructed without topic identity")
	}
	client := source.client.(*kgo.Client)
	closed := false
	defer func() {
		if !closed {
			_ = source.Close()
		}
	}()
	// Then the protected options are explicit and no subscription starts during construction.
	if !client.OptValue(kgo.DisableAutoCommit).(bool) || client.OptValue(kgo.FetchIsolationLevel) != int8(1) || !reflect.DeepEqual(client.OptValue(kgo.ConsumeResetOffset), kgo.NoResetOffset()) || client.OptValue(kgo.OnPartitionsLost) == nil || len(client.GetConsumeTopics()) != 0 {
		t.Fatal("unsafe consumer defaults")
	}
	balancers := client.OptValue(kgo.Balancers).([]kgo.GroupBalancer)
	if len(balancers) != 1 || balancers[0].ProtocolName() != "cooperative-sticky" {
		t.Fatal("unexpected group balancer", balancers)
	}
	// When output setup fails and cleanup runs before Start, then no group join or pump is needed.
	done := make(chan error, 1)
	go func() { done <- source.Close() }()
	select {
	case <-done:
		closed = true
	case <-time.After(3 * time.Second):
		t.Fatal("close-before-start blocked")
	}
	if joins.Load() != 0 {
		t.Fatal("consumer joined before the output was ready")
	}
}

func TestCorePositioningPrecedesClientOffsetAdjustment(t *testing.T) {
	// Given timestamped records and no saved group offset, including on a v10-capable broker.
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "input", "output"))
	defer cluster.Close()
	producer := componentClient(t, cluster)
	stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	for n := range 5 {
		if err := producer.ProduceSync(componentContext(t), &kgo.Record{Topic: "input", Value: []byte(fmt.Sprint(n)), Timestamp: stamp.Add(time.Duration(n) * time.Millisecond)}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	p := componentPlan(t, cluster)
	p.Start = ""
	p.Window.Lower = &plan.Expression{LiteralMS: stamp.Add(2 * time.Millisecond).UnixMilli()}
	_, connection := componentConnection(t, p)
	input, _, err := connection.Source(t.Context(), p, "fp", config.Defaults("").Worker)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	life := &lifecycleMock{}
	if err = input.Start(life); err != nil {
		t.Fatal(err)
	}
	ctx := componentContext(t)
	var first *core.Input
	// When the core resolves the assignment before the client's adjust hook runs.
	for first == nil && ctx.Err() == nil {
		event, err := input.Poll(20 * time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		first = event.Input
	}
	// Then both the first delivered record and the member's saved initial position are two.
	if first == nil || first.Cursor.Position != 2 || string(first.Record.Value) != "2" || len(life.assignments) != 1 || life.assignments[0].Cursor.Position != 2 {
		t.Fatal("client fetched before core positioning", first, life)
	}
	saved, err := kadm.NewClient(producer).FetchOffsetsForTopics(ctx, p.Group, "input")
	if err != nil || saved["input"][0].At != 2 {
		t.Fatal("initial member commit lost", saved, err)
	}
	text := saved["input"][0].Metadata
	if _, err := kafkaport.DecodeProgress(commitmeta.NewCodec(), &text, "fp", input.(*Source).topicID, false); err != nil {
		t.Fatal("wire metadata changed", err)
	}
}

func TestProductionAdaptersPreserveRawRecordsAndZeroNegativeTimestamps(t *testing.T) {
	// Given raw records with duplicate and arbitrary byte header names.
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "input", "output"))
	defer cluster.Close()
	producer := componentClient(t, cluster)
	want := []*kgo.Record{{Topic: "input", Key: nil, Value: nil, Timestamp: time.UnixMilli(0), Headers: []kgo.RecordHeader{{Key: "a\x00b", Value: []byte{}}, {Key: string([]byte{255}), Value: nil}}}, {Topic: "input", Key: []byte{}, Value: []byte{}, Timestamp: time.UnixMilli(-2), Headers: []kgo.RecordHeader{{Key: "dup", Value: nil}, {Key: "dup", Value: []byte{}}}}}
	for _, record := range want {
		if err := producer.ProduceSync(componentContext(t), record).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	p := componentPlan(t, cluster)
	p.Start = ""
	p.Window.Upper = &plan.Expression{LiteralMS: time.Now().Add(-time.Minute).UnixMilli()}
	factory, sourceConnection := componentConnection(t, p)
	destination, err := factory.Open(t.Context(), p.Destination)
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close(context.Background())
	if _, err = destination.ClusterID(t.Context()); err != nil {
		t.Fatal(err)
	}
	limits := config.Defaults("").Worker
	source, store, err := sourceConnection.Source(t.Context(), p, "fp", limits)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	sink, err := destination.Sink(t.Context(), limits)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	// When the actual Source/Committer/Sink run through the shared copy engine.
	runner := core.NewRunner(source, sink, store, core.RealClock{}, quietLogger(), p, limits, core.NewTracker(), core.NoopStopObserver{})
	if err := runner.Run(componentContext(t)); err != nil {
		t.Fatal(err)
	}
	reader := componentClient(t, cluster, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{"output": {0: kgo.NewOffset().AtStart()}}))
	ctx := componentContext(t)
	var got []*kgo.Record
	for len(got) < len(want) && ctx.Err() == nil {
		fetch := reader.PollRecords(ctx, len(want)-len(got))
		if err := fetch.Err(); err != nil {
			t.Fatal(err)
		}
		got = append(got, fetch.Records()...)
	}
	// Then nil/empty bytes, duplicate/raw header names, and timestamps survive unchanged.
	if len(got) != len(want) {
		t.Fatal("missing copied records", len(got))
	}
	for n, record := range got {
		if !reflect.DeepEqual(record.Key, want[n].Key) || !reflect.DeepEqual(record.Value, want[n].Value) || !reflect.DeepEqual(record.Headers, want[n].Headers) || record.Timestamp.UnixMilli() != want[n].Timestamp.UnixMilli() {
			t.Fatal("record changed", n, record)
		}
	}
}
