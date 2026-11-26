//go:build integration

package compat

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/src/internal/app"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

//kaferry:guarantee exit
func TestDistributedWorkersFinishIndependently(t *testing.T) {
	// Given
	a := admin(t)
	source := topic(t, a, 2)
	destination := topic(t, a, 3)
	group := source + "-workers"

	statement := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s') INSERT INTO TOPIC '%s' WITH (PARTITIONING = KEY) SELECT * FROM TOPIC '%s' WITH (GROUP='%s', FOLLOW=ON) OPTIONS(WORKER_STOP_AFTER_RECORDS=30) ", broker(t), destination, source, group)
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
	publishPartition := func(id int32) {
		for i := range 30 {
			n := i*2 + int(id)
			publish(t, p, &kgo.Record{Topic: source, Partition: id, Key: []byte(fmt.Sprintf("key-%d", n%7)), Value: []byte(fmt.Sprintf("record-%d", n))})
		}
	}
	publishPartition(0)
	select {
	case result := <-results:
		// Then its worker finishes while the other partition still has no input.
		require.Zero(t, result.code, result.detail)
	case <-ctx.Done():
		t.Fatal("first worker waited for the other partition's worker allowance")
	}
	select {
	case result := <-results:
		t.Fatalf("remaining worker exited before its worker allowance: %+v", result)
	case <-ctx.Done():
		t.Fatal("workers exceeded their completion budget")
	case <-time.After(300 * time.Millisecond):
	}
	// When the remaining partition receives its trigger, then its worker also completes.
	publishPartition(1)
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

//kaferry:guarantee worker-stop
func TestWorkerRecordThresholdAcrossTenPartitionsAndThreeWorkers(t *testing.T) {
	// Given three workers sharing ten source partitions, each with its own 100-record allowance.
	a := admin(t)
	source, destination := topic(t, a, 10), topic(t, a, 3)
	group := source + "-worker-stop"
	config := configuration(t)
	statement := fmt.Sprintf("INSERT INTO TOPIC '%s' WITH (PARTITIONING = KEY) SELECT * FROM TOPIC '%s' WITH (GROUP='%s', FOLLOW=ON) OPTIONS (WORKER_STOP_AFTER_RECORDS=100)", destination, source, group)
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
