package app

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestPartitionCLISelectsWithoutFetchingOrCommittingExcludedPartitions(t *testing.T) {
	for _, predicate := range []string{"PARTITION IN (2,0,2)", "PARTITION NOT IN (1,3)"} {
		t.Run(predicate, func(t *testing.T) {
			// Given records on four source partitions and a source predicate selecting only 0 and 2.
			cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(4, "input", "output"))
			defer cluster.Close()
			client, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
			require.NoError(t, err)
			defer client.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			stamp := time.Now().Add(-time.Minute).Truncate(time.Millisecond)
			for id := range int32(4) {
				for n := range 2 {
					require.NoError(t, client.ProduceSync(ctx, &kgo.Record{Topic: "input", Partition: id, Timestamp: stamp, Value: []byte(fmt.Sprint(id, n))}).FirstErr())
				}
			}
			var mu sync.Mutex
			fetched := map[int32]bool{}
			cluster.ControlKey(int16(kmsg.Fetch), func(request kmsg.Request) (kmsg.Response, error, bool) {
				cluster.KeepControl()
				mu.Lock()
				defer mu.Unlock()
				for _, topic := range request.(*kmsg.FetchRequest).Topics {
					for _, partition := range topic.Partitions {
						fetched[partition.Partition] = true
					}
				}
				return nil, nil, false
			})
			query := fmt.Sprintf("WITH CONNECTION p AS ('bootstrap.servers'='%s') INSERT INTO TOPIC 'output@p' SELECT * FROM TOPIC 'input@p' WITH(GROUP='selected-copy', CUTOFF=INTERVAL '0' SECOND) WHERE %s AND RECORD_TIMESTAMP < TIMESTAMP '%s'", strings.Join(cluster.ListenAddrs(), ","), predicate, stamp.Add(time.Second).Format(time.RFC3339Nano))
			var stdout, stderr bytes.Buffer
			// When the real CLI performs a bounded copy through consumer-group ownership.
			code := Execute(ctx, []string{"run", "-e", query}, nil, &stdout, &stderr)
			require.Zero(t, code, stderr.String())
			// Then only selected partitions are fetched, saved and copied to their original destination IDs.
			mu.Lock()
			seen := make([]int32, 0, len(fetched))
			for id := range fetched {
				seen = append(seen, id)
			}
			mu.Unlock()
			slices.Sort(seen)
			require.Equal(t, []int32{0, 2}, seen)
			admin := kadm.NewClient(client)
			saved, err := admin.FetchOffsetsForTopics(ctx, "selected-copy", "input")
			require.NoError(t, err)
			ends, err := admin.ListEndOffsets(ctx, "output")
			require.NoError(t, err)
			for id := range int32(4) {
				if id == 0 || id == 2 {
					require.EqualValues(t, 2, saved["input"][id].At)
					require.EqualValues(t, 2, ends["output"][id].Offset)
				} else {
					require.EqualValues(t, -1, saved["input"][id].At)
					require.Zero(t, ends["output"][id].Offset)
				}
			}
		})
	}
}
