package franz

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	"github.com/pagrus7/kaferry/internal/delivery"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestProgressPartitionGrowthWaitsForServingMetadata(t *testing.T) {
	for _, cancelBeforeReady := range []bool{false, true} {
		t.Run(map[bool]string{false: "publish after catchup", true: "cancel before catchup"}[cancelBeforeReady], func(t *testing.T) {
			// Given a growth acknowledgement while one serving broker still describes the old layout.
			cluster, log, client := progressLogFixture(t)
			request := kmsg.NewPtrMetadataRequest()
			request.Topics = []kmsg.MetadataRequestTopic{{Topic: new("progress")}}
			old, err := request.RequestWith(t.Context(), client)
			require.NoError(t, err)
			var grew, caughtUp atomic.Bool
			observed := make(chan struct{}, 2)
			cluster.ControlKey(int16(kmsg.CreatePartitions), func(kmsg.Request) (kmsg.Response, error, bool) {
				grew.Store(true)
				return nil, nil, false
			})
			cluster.ControlKey(int16(kmsg.Metadata), func(req kmsg.Request) (kmsg.Response, error, bool) {
				cluster.KeepControl()
				if !grew.Load() || caughtUp.Load() || cluster.CurrentNode() != 1 {
					return nil, nil, false
				}
				select {
				case observed <- struct{}{}:
				default:
				}
				response := *old
				response.SetVersion(req.GetVersion())
				return &response, nil, true
			})

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- log.Ensure(ctx, []int32{2}) }()
			// When stale metadata is served twice, then preparation must keep waiting rather than admit partition two.
			for range 2 {
				select {
				case <-observed:
				case err := <-done:
					t.Fatalf("preparation returned before metadata caught up: %v", err)
				case <-ctx.Done():
					t.Fatal("stale broker was not probed")
				}
			}
			if cancelBeforeReady {
				cancel()
				require.ErrorIs(t, <-done, context.Canceled)
				return
			}
			caughtUp.Store(true)
			require.NoError(t, <-done)
			// Then the new partition accepts and exposes its committed value.
			tx, err := log.Open(ctx, 2)
			require.NoError(t, err)
			defer tx.Close()
			require.NoError(t, tx.Write(ctx, "cursor", []byte("confirmed")))
			require.NoError(t, tx.Commit(ctx))
			value, err := log.ReadValue(ctx, 2, "cursor", false)
			require.NoError(t, err)
			require.Equal(t, []byte("confirmed"), value)
		})
	}
}

func progressLogFixture(t *testing.T) (*kfake.Cluster, *ProgressLog, *kgo.Client) {
	t.Helper()
	cluster := kfake.MustCluster(kfake.NumBrokers(3))
	t.Cleanup(cluster.Close)
	client := componentClient(t, cluster)
	created, err := kadm.NewClient(client).CreateTopics(t.Context(), 1, 1, map[string]*string{"cleanup.policy": kadm.StringPtr("compact")}, "progress")
	require.NoError(t, err)
	require.NoError(t, created.Error())
	session := delivery.New(plan.Plan{Group: "progress-test"}, config.Defaults("").Worker)
	session.Identity.ProgressTopic = [16]byte(created["progress"].ID)
	log := &ProgressLog{options: []kgo.Opt{kgo.SeedBrokers(cluster.ListenAddrs()...)}, topic: "progress", session: session, clients: NewClients()}
	return cluster, log, client
}

func TestProgressReadRetriesLeaderRoutingWithoutRepeatingBarrier(t *testing.T) {
	for _, failure := range []string{"transport", "leader", "authorization"} {
		t.Run(failure, func(t *testing.T) {
			// Given a committed value followed by one failed fetch from its former leader.
			cluster, log, _ := progressLogFixture(t)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			require.NoError(t, log.Ensure(ctx, []int32{0}))
			tx, err := log.Open(ctx, 0)
			require.NoError(t, err)
			defer tx.Close()
			require.NoError(t, tx.Write(ctx, "cursor", []byte("saved")))
			require.NoError(t, tx.Commit(ctx))
			oldLeader := cluster.LeaderFor("progress", 0)
			var fetches, barriers atomic.Int64
			cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
				cluster.KeepControl()
				barriers.Add(1)
				return nil, nil, false
			})
			cluster.ControlKey(int16(kmsg.Fetch), func(req kmsg.Request) (kmsg.Response, error, bool) {
				cluster.KeepControl()
				if fetches.Add(1) != 1 {
					return nil, nil, false
				}
				if failure == "transport" {
					return nil, io.EOF, true
				}
				response := req.ResponseKind().(*kmsg.FetchResponse)
				code := kerr.TopicAuthorizationFailed.Code
				if failure == "leader" {
					if err := cluster.MoveTopicPartition("progress", 0, (oldLeader+1)%3); err != nil {
						return nil, err, true
					}
					code = kerr.NotLeaderForPartition.Code
				}
				response.Topics = []kmsg.FetchResponseTopic{{Topic: "progress", TopicID: log.session.Identity.ProgressTopic, Partitions: []kmsg.FetchResponseTopicPartition{{Partition: 0, ErrorCode: code}}}}
				return response, nil, true
			})
			// When authority is read, then transient routing failures re-read the fixed prefix; authorization still fails immediately.
			value, err := log.ReadValue(ctx, 0, "cursor", true)
			if failure == "authorization" {
				require.ErrorIs(t, err, kerr.TopicAuthorizationFailed)
				require.EqualValues(t, 1, fetches.Load())
			} else {
				require.NoError(t, err)
				require.Equal(t, []byte("saved"), value)
				require.GreaterOrEqual(t, fetches.Load(), int64(2))
			}
			require.EqualValues(t, 1, barriers.Load(), "retry must preserve the original visibility boundary")
		})
	}
}
