package franz

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/commitmeta"
	"github.com/pagrus7/kaferry/internal/config"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestLatestFreezesReadableEndAndKeepsItAfterResume(t *testing.T) {
	// Given committed records and an unresolved transactional tail.
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "input", "output"))
	defer cluster.Close()
	producer := componentClient(t, cluster)
	for n := range 5 {
		require.NoError(t, producer.ProduceSync(componentContext(t), &kgo.Record{Topic: "input", Value: []byte(fmt.Sprint(n)), Timestamp: time.Now().Add(time.Hour)}).FirstErr())
	}
	transactional := componentClient(t, cluster, kgo.TransactionalID("latest-tail"))
	require.NoError(t, transactional.BeginTransaction())
	require.NoError(t, transactional.ProduceSync(componentContext(t), &kgo.Record{Topic: "input", Value: []byte("pending")}).FirstErr())
	p := componentPlan(t, cluster)
	p.Offsets = &plan.OffsetRange{Latest: true}
	// When copying to LATEST, then finish at the readable prefix without awaiting the transaction.
	require.NoError(t, runComponentCopy(componentContext(t), p, config.Defaults("").Worker))
	offsets, err := kadm.NewClient(producer).FetchOffsetsForTopics(componentContext(t), p.Group, "input")
	require.NoError(t, err)
	saved := offsets["input"][0]
	require.EqualValues(t, 5, saved.At)
	metadata, err := commitmeta.NewCodec().Decode(saved.Metadata)
	require.NoError(t, err)
	require.EqualValues(t, 5, *metadata.End)
	require.Equal(t, "tip", metadata.Stop.Reason)
	// When the transaction commits and more records arrive, then resume keeps the first captured end.
	require.NoError(t, transactional.EndTransaction(componentContext(t), kgo.TryCommit))
	require.NoError(t, producer.ProduceSync(componentContext(t), &kgo.Record{Topic: "input", Value: []byte("later")}).FirstErr())
	require.NoError(t, runComponentCopy(componentContext(t), p, config.Defaults("").Worker))
	end, err := kadm.NewClient(producer).ListEndOffsets(componentContext(t), "output")
	require.NoError(t, err)
	require.EqualValues(t, 6, end["output"][0].Offset, "five records and one transaction marker")
	reader := componentClient(t, cluster, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{"output": {0: kgo.NewOffset().AtStart()}}))
	got := readN(t, reader, 5)
	for n, r := range got {
		require.Equal(t, fmt.Sprint(n), string(r.Value))
	}
}

func TestNewPartitionAssignmentInitializesFromQueryAndSavedWindow(t *testing.T) {
	for _, windowed := range []bool{false, true} {
		t.Run(fmt.Sprint(windowed), func(t *testing.T) {
			// Given an active subscriber, either bootstrapped at LATEST or using a relative time range.
			cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "input", "output"))
			defer cluster.Close()
			producer := componentClient(t, cluster, kgo.RecordPartitioner(kgo.ManualPartitioner()))
			require.NoError(t, producer.ProduceSync(componentContext(t), &kgo.Record{Topic: "input", Partition: 0, Value: []byte("initial"), Timestamp: time.Now().Add(-time.Minute)}).FirstErr())
			p := testPlan(t, map[string]string{"bootstrap.servers": strings.Join(cluster.ListenAddrs(), ","), "metadata.max.age.ms": "10"})
			p.Start = "LATEST"
			if windowed {
				p.Start = ""
				p.Window = plan.WindowSpec{Lower: &plan.Expression{Now: true, AdjustmentMS: -3600000}, Upper: &plan.Expression{Now: true, AdjustmentMS: 3600000}}
			}
			_, connection := componentConnection(t, p)
			source, _, err := connection.Source(t.Context(), p, "fp", config.Defaults("").Worker, nil)
			require.NoError(t, err)
			defer source.Close()
			life := &lifecycleMock{}
			require.NoError(t, source.Start(life, nil))
			ctx := componentContext(t)
			for len(life.assignments) == 0 && ctx.Err() == nil {
				_, err := source.Poll(20 * time.Millisecond)
				require.NoError(t, err)
			}
			require.NotEmpty(t, life.assignments)
			first := life.assignments[0]
			// When a new partition is created and receives records before discovery.
			response, err := kadm.NewClient(producer).CreatePartitions(ctx, 1, "input")
			require.NoError(t, err)
			require.NoError(t, response.Error())
			newProducer := componentClient(t, cluster, kgo.RecordPartitioner(kgo.ManualPartitioner()))
			require.NoError(t, newProducer.ProduceSync(ctx, &kgo.Record{Topic: "input", Partition: 1, Value: []byte("new partition first"), Timestamp: time.Now().Add(-time.Minute)}).FirstErr())
			var position int64 = -1
			for position < 0 && ctx.Err() == nil {
				event, err := source.Poll(20 * time.Millisecond)
				require.NoError(t, err)
				if event.Input != nil && event.Input.Cursor.Partition == 1 {
					position = event.Input.Cursor.Position
					require.Equal(t, "new partition first", string(event.Input.Record.Value))
				}
			}
			// Then use the assignment path, preserve the resolved window, and start at the new partition's beginning.
			require.EqualValues(t, 0, position)
			var found bool
			for _, a := range life.assignments {
				if a.Cursor.Partition == 1 {
					found = true
					require.Equal(t, first.Window, a.Window)
				}
			}
			require.True(t, found)
			saved, err := kadm.NewClient(producer).FetchOffsetsForTopics(ctx, p.Group, "input")
			require.NoError(t, err)
			require.EqualValues(t, 0, saved["input"][1].At)
		})
	}
}
