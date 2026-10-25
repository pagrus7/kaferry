package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/archive/avro"
	"github.com/pagrus7/kaferry/internal/inspect"
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

func TestFilePartitionsLimitPreservedDestinationLayoutWithoutJoiningAGroup(t *testing.T) {
	// Given archive partitions 0 and 5 but a Kafka destination with only partition 0.
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "output"))
	defer cluster.Close()
	root := t.TempDir()
	src, cfg := filepath.Join(root, "input"), filepath.Join(root, "worker.toml")
	source := avro.Source{Cluster: "archive", Topic: "orders", TopicID: avro.TopicID{1}}
	archiveFixture(t, src, 1, []avro.Record{{Source: source, TimestampKind: "unknown", Value: []byte("selected")}})
	source.Partition = 5
	archiveFixture(t, src, 2, []avro.Record{{Source: source, TimestampKind: "unknown", Value: []byte("excluded")}})
	require.NoError(t, os.WriteFile(cfg, []byte(fmt.Sprintf("[worker]\ncheckpoint_dir=%q\n", root)), 0600))
	var joins atomic.Int64
	cluster.ControlKey(int16(kmsg.JoinGroup), func(kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		joins.Add(1)
		return nil, nil, false
	})
	query := fmt.Sprintf("WITH CONNECTION p AS ('bootstrap.servers'='%s') INSERT INTO TOPIC 'output@p' SELECT * FROM ARCHIVE '%s' WITH(GROUP='file-selected') WHERE PARTITION IN (0)", strings.Join(cluster.ListenAddrs(), ","), src)
	var out, errout bytes.Buffer
	// When explaining and copying, then only selected original IDs must fit the destination.
	require.Zero(t, Execute(t.Context(), []string{"explain", "-e", query, "-c", cfg, "--json"}, nil, &out, &errout), errout.String())
	var preview inspect.Report
	require.NoError(t, json.Unmarshal(out.Bytes(), &preview))
	require.Equal(t, 1, preview.Source.File.FileCount)
	out.Reset()
	errout.Reset()
	require.Zero(t, Execute(t.Context(), []string{"run", "-e", query, "-c", cfg}, nil, &out, &errout), errout.String())
	client, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	require.NoError(t, err)
	defer client.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ends, err := kadm.NewClient(client).ListEndOffsets(ctx, "output")
	require.NoError(t, err)
	require.EqualValues(t, 1, ends["output"][0].Offset)
	require.Zero(t, joins.Load(), "file replay uses its checkpoint lock, not Kafka consumer-group membership")
}
