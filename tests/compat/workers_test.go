//go:build integration

package compat

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/app"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type workerResult struct {
	code   int
	detail string
}

func waitMembers(t *testing.T, a *kadm.Client, group string, members int, partitions int, results <-chan workerResult) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		result, err := a.DescribeGroups(ctx, group)
		if err == nil && len(result) == 1 && result[group].Err == nil {
			g := result[group]
			assigned := 0
			for _, m := range g.Members {
				if assignment, ok := m.Assigned.AsConsumer(); ok {
					for _, topic := range assignment.Topics {
						assigned += len(topic.Partitions)
					}
				}
			}
			if len(g.Members) == members && assigned == partitions {
				return
			}
		}
		select {
		case r := <-results:
			t.Fatalf("worker exited during assignment: code=%d detail=%s", r.code, r.detail)
		case <-ctx.Done():
			t.Fatal("workers did not acquire expected assignments", err)
		case <-ticker.C:
		}
	}
}

func TestDistributedWorkersFinishIndependently(t *testing.T) {
	// Given
	a := admin(t)
	source := topic(t, a, 2)
	destination := topic(t, a, 3)
	group := source + "-workers"
	end := time.Now().Add(time.Hour).Truncate(time.Millisecond)
	statement := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s') INSERT INTO TOPIC '%s' WITH (PARTITIONING = KEY) SELECT * FROM TOPIC '%s' WITH (GROUP='%s', CUTOFF=INTERVAL '0' SECOND) WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' ", broker(t), destination, source, group, end.Format(time.RFC3339Nano))
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	results := make(chan workerResult, 2)
	for range 2 {
		go func() {
			var output bytes.Buffer

			// When
			code := app.Execute(ctx, []string{"run", "--sql", statement}, nil, io.Discard, &output)
			results <- workerResult{code, output.String()}
		}()
	}
	waitMembers(t, a, group, 2, 2, results)
	p := producer(t)
	for i := range 60 {
		publish(t, p, &kgo.Record{Topic: source, Partition: int32(i % 2), Key: []byte(fmt.Sprintf("key-%d", i%7)), Value: []byte(fmt.Sprintf("record-%d", i)), Timestamp: time.Now()})
	}
	// When only partition zero receives its timestamp stop trigger.
	publish(t, p, &kgo.Record{Topic: source, Partition: 0, Value: []byte("trigger"), Timestamp: end.Add(time.Second)})
	select {
	case result := <-results:
		// Then its worker finishes while the other partition still needs its trigger.
		require.Zero(t, result.code, result.detail)
	case <-ctx.Done():
		t.Fatal("first worker waited for the other partition's stop trigger")
	}
	select {
	case result := <-results:
		t.Fatalf("remaining worker exited before its stop trigger: %+v", result)
	case <-ctx.Done():
		t.Fatal("workers exceeded their completion budget")
	case <-time.After(300 * time.Millisecond):
	}
	// When the remaining partition receives its trigger, then its worker also completes.
	publish(t, p, &kgo.Record{Topic: source, Partition: 1, Value: []byte("trigger"), Timestamp: end.Add(time.Second)})
	select {
	case result := <-results:
		require.Zero(t, result.code, result.detail)
	case <-ctx.Done():
		t.Fatal("remaining worker failed to finish")
	}

	c := consumer(t)
	assign(t, c, destination, 3)
	seen := make(map[string]int)
	keyPartitions := make(map[string]int32)
	for len(seen) < 60 {
		r := read(t, c)
		require.NotEqual(t, "trigger", string(r.Value), "stop trigger was published")
		seen[string(r.Value)]++
		if part, ok := keyPartitions[string(r.Key)]; ok && part != r.Partition {
			t.Fatal("same key routed to different partitions")
		}
		keyPartitions[string(r.Key)] = r.Partition
	}
	for i := range 60 {
		require.NotZero(t, seen[fmt.Sprintf("record-%d", i)], "record lost during worker completion")
	}
}

func TestWorkerRecordThresholdAcrossTenPartitionsAndThreeWorkers(t *testing.T) {
	// Given three workers sharing ten source partitions, each with its own 100-record allowance.
	a := admin(t)
	source, destination := topic(t, a, 10), topic(t, a, 3)
	group := source + "-worker-stop"
	config := configuration(t)
	statement := fmt.Sprintf("INSERT INTO TOPIC '%s' WITH (PARTITIONING = KEY) SELECT * FROM TOPIC '%s' WITH (GROUP='%s', START=EARLIEST) OPTIONS (WORKER_STOP_AFTER_RECORDS=100)", destination, source, group)
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	results := make(chan workerResult, 3)
	for range 3 {
		go func() {
			var output bytes.Buffer
			code := app.Execute(ctx, []string{"run", "-e", statement, "-c", config}, nil, io.Discard, &output)
			results <- workerResult{code, output.String()}
		}()
	}
	waitMembers(t, a, group, 3, 10, results)
	producer := producer(t)
	// When each partition has enough input for every worker to reach its threshold.
	for i := range 1000 {
		publish(t, producer, &kgo.Record{Topic: source, Partition: int32(i % 10), Value: []byte(fmt.Sprint(i))})
	}
	for range 3 {
		select {
		case result := <-results:
			require.Zero(t, result.code, result.detail)
		case <-ctx.Done():
			t.Fatal("workers did not stop at their record thresholds")
		}
	}
	// Then the group copies 300 records, saves stops for all partitions and needs no other worker to exit.
	require.NoError(t, ctx.Err(), "workers must finish before cancellation")
	require.EqualValues(t, 300, topicRecordCount(t, a, destination, 3))
	waitMembers(t, a, group, 0, 0, nil)
	assertSavedWorkerStop(t, statement, config)
}
