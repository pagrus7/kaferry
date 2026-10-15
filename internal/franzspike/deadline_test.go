//go:build integration && franzspike

package franzspike

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
	"go.uber.org/goleak"
)

func holdRequests(c *kfake.Cluster, key int16) (<-chan struct{}, func()) {
	observed, release := make(chan struct{}, 1), make(chan struct{})
	var once sync.Once
	c.ControlKey(key, func(kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		select {
		case observed <- struct{}{}:
		default:
		}
		c.SleepControl(func() { <-release })
		return nil, nil, false
	})
	return observed, func() { once.Do(func() { close(release) }) }
}
func await(t *testing.T, done <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal(label + " did not finish within 2 seconds")
	}
}
func TestSpikeCoordinatorAndCommitDeadlines(t *testing.T) {
	for _, key := range []int16{10, 8} {
		t.Run(map[int16]string{10: "coordinator discovery", 8: "member commit"}[key], func(t *testing.T) {
			defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
			// Given a group client and a broker that withholds the selected response.
			cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "source"), kfake.MaxVersions(kversion.V3_9_0()))
			defer cluster.Close()
			lifetime, cancel := context.WithCancel(context.Background())
			c, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.WithContext(lifetime), kgo.ConsumerGroup("deadline"), kgo.ConsumeTopics("source"), kgo.DisableAutoCommit(), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.RequestTimeoutOverhead(time.Second))
			if err != nil {
				t.Fatal(err)
			}
			defer c.CloseAllowingRebalance()
			defer cancel()
			var record *kgo.Record
			if key == 8 {
				if err := c.ProduceSync(ctx(t), &kgo.Record{Topic: "source", Value: []byte("one")}).FirstErr(); err != nil {
					t.Fatal(err)
				}
				record = readN(t, c, 1)[0]
			}
			observed, release := holdRequests(cluster, key)
			defer release()
			// When the operation's 200ms context expires while the request is blocked.
			deadline, stop := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer stop()
			result := make(chan error, 1)
			go func() {
				if key == 10 {
					result <- c.PollRecords(deadline, 1).Err()
				} else {
					result <- c.CommitRecords(deadline, record)
				}
			}()
			await(t, observed, "request interception")
			select {
			case err := <-result:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("wanted deadline, got %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("operation ignored deadline")
			}
			// Then canceling the client also closes it without waiting for the broker response.
			cancel()
			closed := make(chan struct{})
			go func() { c.CloseAllowingRebalance(); close(closed) }()
			await(t, closed, "client close")
		})
	}
}

func TestSpikeProduceDeadlineNeedsCancellationOption(t *testing.T) {
	for _, bounded := range []bool{false, true} {
		name := "default idempotency"
		if bounded {
			name = "explicit bounded cancellation"
		}
		t.Run(name, func(t *testing.T) {
			defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
			// Given an in-flight produce whose reply never arrives.
			cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "output"))
			defer cluster.Close()
			lifetime, cancel := context.WithCancel(context.Background())
			opts := []kgo.Opt{kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.WithContext(lifetime)}
			if bounded {
				opts = append(opts, kgo.AllowIdempotentProduceCancellation())
			}
			c, err := kgo.NewClient(opts...)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			defer cancel()
			if err := c.Ping(ctx(t)); err != nil {
				t.Fatal(err)
			}
			observed, release := holdRequests(cluster, 0)
			defer release()
			delivery, stop := context.WithTimeout(context.Background(), 150*time.Millisecond)
			defer stop()
			result := make(chan error, 1)
			// When the per-record deadline expires.
			c.Produce(delivery, &kgo.Record{Topic: "output", Value: []byte("one")}, func(_ *kgo.Record, err error) { result <- err })
			await(t, observed, "produce interception")
			<-delivery.Done()
			if bounded {
				select {
				case err := <-result:
					if !errors.Is(err, context.DeadlineExceeded) {
						t.Fatal(err)
					}
				case <-time.After(time.Second):
					t.Fatal("produce ignored deadline")
				}
			} else {
				select {
				case err := <-result:
					t.Fatalf("default unexpectedly canceled ambiguous in-flight write: %v", err)
				case <-time.After(250 * time.Millisecond):
				}
			}
			// Then forced client cancellation closes even the default ambiguous write.
			cancel()
			closed := make(chan struct{})
			go func() { c.Close(); close(closed) }()
			await(t, closed, "producer close")
			t.Logf("bounded record cancellation=%t; forced close finished", bounded)
		})
	}
}

func TestSpikeDeliveryTimeoutUsesRecordTimestamp(t *testing.T) {
	// Given a valid copied record from one hour ago and a 1s delivery timeout.
	topicName := topic(t, 1)
	c := client(t, kgo.RecordDeliveryTimeout(time.Second))
	old := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	// When requesting a new write now.
	err := c.ProduceSync(ctx(t), &kgo.Record{Topic: topicName, Timestamp: old, Value: []byte("old")}).FirstErr()
	// Then this client version expires it before delivery; timestamp-preserving adapters must avoid this option.
	if !errors.Is(err, kgo.ErrRecordTimeout) {
		t.Fatalf("expected observed timeout defect, got %v", err)
	}
	c2 := client(t)
	deadline, cancel := context.WithTimeout(ctx(t), time.Second)
	defer cancel()
	if err := c2.ProduceSync(deadline, &kgo.Record{Topic: topicName, Timestamp: old, Value: []byte("old")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	reader := client(t, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topicName: {0: kgo.NewOffset().AtStart()}}))
	if got := readN(t, reader, 1)[0]; got.Timestamp.UnixMilli() != old.UnixMilli() {
		t.Fatal("timestamp changed")
	}
}
