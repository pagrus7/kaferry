//go:build integration && franzspike

package franzspike

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
	"go.uber.org/goleak"
)

func fakePlan(t *testing.T, cluster *kfake.Cluster, query string) plan.Plan {
	t.Helper()
	return spikePlan(t, fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s') %s", strings.Join(cluster.ListenAddrs(), ","), query))
}

func TestSpikeSinkReportsInflightDeadline(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	// Given an adapter with a 200ms write deadline and an unreachable produce reply.
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "output"))
	defer cluster.Close()
	p := fakePlan(t, cluster, "INSERT INTO TOPIC 'output' SELECT * FROM TOPIC 'source' OPTIONS(GROUP='deadline',START=EARLIEST)")
	limits := config.Defaults(t.TempDir()).Worker
	limits.DeliveryTimeout = "200ms"
	sink, err := NewSink(p, limits)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	if err := sink.client.Ping(ctx(t)); err != nil {
		t.Fatal(err)
	}
	observed, release := holdRequests(cluster, int16(kmsg.Produce))
	defer release()
	token := core.Token{Partition: 0, Generation: 1, Sequence: 1}
	// When the in-flight write exceeds its submission-based deadline.
	stamp := time.Now().Add(-time.Hour).UnixMilli()
	if err := sink.Submit(core.Record{Value: []byte("old"), TimestampMS: &stamp}, token); err != nil {
		t.Fatal(err)
	}
	await(t, observed, "produce request")
	// Then the adapter reports the failed record, allowing the runner to leave it unconfirmed.
	select {
	case delivery := <-sink.Deliveries():
		if delivery.Token != token || !errors.Is(delivery.Err, context.DeadlineExceeded) || delivery.Fatal {
			t.Fatalf("unexpected delivery: %+v", delivery)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("adapter ignored in-flight delivery deadline")
	}
}

func TestSpikeAmbiguousDeliveryReplaysWithoutLosingProgress(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	// Given three source records and a broker whose second produce reply is lost.
	gate := &replyGate{}
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "source", "output"), kfake.MaxVersions(kversion.V3_9_0()), kfake.ListenFn(gate.listen))
	defer cluster.Close()
	producer, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	for _, value := range []string{"confirmed", "uncertain", "later"} {
		if err := producer.ProduceSync(ctx(t), &kgo.Record{Topic: "source", Key: []byte("key"), Value: []byte(value), Timestamp: stamp}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	p := fakePlan(t, cluster, fmt.Sprintf("INSERT INTO TOPIC 'output' SELECT * FROM TOPIC 'source' WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' OPTIONS(GROUP='ambiguous',CUTOFF=INTERVAL '0' SECOND)", stamp.Add(time.Minute).Format(time.RFC3339Nano)))
	limits := config.Defaults(t.TempDir()).Worker
	limits.DeliveryTimeout = "200ms"
	limits.RecordCap = 1
	gate.after.Store(2)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	// When the copy times out after the destination has accepted its record.
	started := time.Now()
	err = Execute(ctx(t), p, limits, logger)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wanted delivery deadline, got %v", err)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("failed delivery delayed shutdown: %s", elapsed)
	}
	admin := kadm.NewClient(producer)
	saved, err := admin.FetchOffsets(ctx(t), p.Group)
	if err != nil || saved["source"][0].At != 1 {
		t.Fatalf("ambiguous write advanced source progress: %v %v", saved, err)
	}
	if end := cluster.PartitionInfo("output", 0).HighWatermark; end != 2 || gate.dropped.Load() == 0 {
		t.Fatalf("fixture did not store the unacknowledged second record: end=%d dropped=%d", end, gate.dropped.Load())
	}
	// When the destination recovers and the same query restarts.
	gate.after.Store(0)
	limits.DeliveryTimeout = "5s"
	if err := Execute(ctx(t), p, limits, logger); err != nil {
		t.Fatal(err)
	}
	// Then the record replays, producing the permitted duplicate and saving its next offset.
	saved, err = admin.FetchOffsets(ctx(t), p.Group)
	if err != nil || saved["source"][0].At != 3 {
		t.Fatalf("replay did not save confirmed progress: %v %v", saved, err)
	}
	if end := cluster.PartitionInfo("output", 0).HighWatermark; end != 4 {
		t.Fatalf("wanted three source records plus one replay, got %d", end)
	}
	reader, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{"output": {0: kgo.NewOffset().AtStart()}}))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for i, record := range readN(t, reader, 4) {
		want := []string{"confirmed", "uncertain", "uncertain", "later"}[i]
		if string(record.Key) != "key" || string(record.Value) != want || record.Timestamp.UnixMilli() != stamp.UnixMilli() {
			t.Fatalf("replay changed record: %+v", record)
		}
	}
}

func TestSpikeSourceCloseDuringStartup(t *testing.T) {
	for _, operation := range []struct {
		name string
		key  kmsg.Key
	}{{"coordinator discovery", kmsg.FindCoordinator}, {"group join", kmsg.JoinGroup}, {"position lookup", kmsg.ListOffsets}, {"saved progress lookup", kmsg.OffsetFetch}} {
		t.Run(operation.name, func(t *testing.T) {
			defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
			// Given a source whose startup request never receives a reply.
			cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "source"), kfake.MaxVersions(kversion.V3_9_0()))
			defer cluster.Close()
			p := fakePlan(t, cluster, "INSERT INTO TOPIC 'output' SELECT * FROM TOPIC 'source' OPTIONS(GROUP='startup',START=EARLIEST)")
			_, topicID, err := EndpointIdentity(ctx(t), p.Source)
			if err != nil {
				t.Fatal(err)
			}
			observed, release := holdRequests(cluster, int16(operation.key))
			defer release()
			source, err := NewSource(p, "startup", topicID)
			if err != nil {
				t.Fatal(err)
			}
			if err := source.Start(&lifeProbe{src: source}); err != nil {
				t.Fatal(err)
			}
			await(t, observed, "startup interception")
			// When cleanup starts before startup can finish.
			closed := make(chan error, 1)
			go func() { closed <- source.Close() }()
			// Then cleanup joins the pump and client without needing the broker reply.
			select {
			case <-closed:
			case <-time.After(4 * time.Second):
				t.Fatal("source cleanup waited for a blocked startup request")
			}
		})
	}
}

func TestSpikeSourceCloseWithBlockedRequests(t *testing.T) {
	for _, operation := range []struct {
		name string
		key  kmsg.Key
	}{{"fetch", kmsg.Fetch}, {"leave group", kmsg.LeaveGroup}, {"commit", kmsg.OffsetCommit}} {
		t.Run(operation.name, func(t *testing.T) {
			defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
			// Given an assigned source and a broker that withholds a shutdown-related reply.
			cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "source"), kfake.MaxVersions(kversion.V3_9_0()))
			defer cluster.Close()
			p := fakePlan(t, cluster, "INSERT INTO TOPIC 'output' SELECT * FROM TOPIC 'source' OPTIONS(GROUP='cleanup',START=EARLIEST)")
			_, topicID, err := EndpointIdentity(ctx(t), p.Source)
			if err != nil {
				t.Fatal(err)
			}
			source, err := NewSource(p, "cleanup", topicID)
			if err != nil {
				t.Fatal(err)
			}
			probe := &lifeProbe{src: source}
			if err := source.Start(probe); err != nil {
				t.Fatal(err)
			}
			pollUntil(t, source, func() bool { return probe.assignments == 1 })
			observed, release := holdRequests(cluster, int16(operation.key))
			defer release()
			commit := make(chan error, 1)
			if operation.key == kmsg.OffsetCommit {
				go func() { _, err := source.Save(ctx(t), []core.Cursor{{Partition: 0, Position: 0}}); commit <- err }()
			}
			if operation.key != kmsg.LeaveGroup {
				await(t, observed, "request interception")
			}
			// When cleanup cancels pending requests and leaves the group.
			closed := make(chan error, 1)
			go func() { closed <- source.Close() }()
			if operation.key == kmsg.LeaveGroup {
				await(t, observed, "leave group interception")
			}
			// Then all adapter goroutines finish without waiting for the withheld reply.
			select {
			case <-closed:
			case <-time.After(4 * time.Second):
				t.Fatal("cleanup waited for withheld reply")
			}
			if operation.key == kmsg.OffsetCommit {
				select {
				case err := <-commit:
					if err == nil {
						t.Fatal("blocked commit reported success")
					}
				case <-time.After(time.Second):
					t.Fatal("cleanup left commit running")
				}
			}
		})
	}
}

func TestSpikeRejectedWriteDoesNotWaitForDrainTimeout(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	// Given a source record and a destination that rejects its write.
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "source", "output"), kfake.MaxVersions(kversion.V3_9_0()))
	defer cluster.Close()
	producer, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	if err := producer.ProduceSync(ctx(t), &kgo.Record{Topic: "source", Value: []byte("rejected")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	fault := cluster.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: "output", Err: kerr.MessageTooLarge, Count: -1})
	defer fault.Remove()
	p := fakePlan(t, cluster, "INSERT INTO TOPIC 'output' SELECT * FROM TOPIC 'source' OPTIONS(GROUP='rejected',START=EARLIEST)")
	// When the adapter receives a record-level rejection.
	started := time.Now()
	err = Execute(ctx(t), p, config.Defaults(t.TempDir()).Worker, slog.New(slog.NewTextHandler(io.Discard, nil)))
	// Then it reports the broker error promptly and leaves the failed record replayable.
	if !errors.Is(err, kerr.MessageTooLarge) || strings.Contains(err.Error(), "outstanding writes") || time.Since(started) > 3*time.Second {
		t.Fatal("record rejection caused a false drain timeout", err)
	}
	saved, err := kadm.NewClient(producer).FetchOffsets(ctx(t), p.Group)
	if err != nil || saved["source"][0].At != 0 || cluster.PartitionInfo("output", 0).HighWatermark != 0 {
		t.Fatal("rejected record was written or skipped", saved, err)
	}
}

func TestSpikeMetadataDeadlineAlsoClosesClient(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	// Given an endpoint whose metadata response never arrives.
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "source"))
	defer cluster.Close()
	p := fakePlan(t, cluster, "INSERT INTO TOPIC 'output' SELECT * FROM TOPIC 'source' OPTIONS(GROUP='metadata',START=EARLIEST)")
	_, release := holdRequests(cluster, int16(kmsg.Metadata))
	defer release()
	deadline, cancel := context.WithTimeout(ctx(t), 200*time.Millisecond)
	defer cancel()
	// When metadata lookup reaches its deadline and closes its temporary client.
	started := time.Now()
	_, _, err := EndpointIdentity(deadline, p.Source)
	// Then the caller receives the deadline without leaving client goroutines running.
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > 2*time.Second {
		t.Fatal("metadata cleanup ignored deadline", err)
	}
}
