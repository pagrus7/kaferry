//go:build integration

package compat

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pagrus7/kaferry/internal/app"
)

type workerResult struct {
	code   int
	detail string
}

func waitMembers(t *testing.T, a *k.AdminClient, group string, members int, partitions int, results <-chan workerResult) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		result, err := a.DescribeConsumerGroups(ctx, []string{group})
		if err == nil && len(result.ConsumerGroupDescriptions) == 1 {
			g := result.ConsumerGroupDescriptions[0]
			assigned := 0
			for _, m := range g.Members {
				assigned += len(m.Assignment.TopicPartitions)
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

func TestDistributedWorkersFinishAndSoleWorkerConfirms(t *testing.T) {
	// Given
	a := admin(t)
	source := topic(t, a, 2)
	destination := topic(t, a, 3)
	group := source + "-workers"
	end := time.Now().Add(time.Hour).Truncate(time.Millisecond)
	statement := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s') INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' OPTIONS(GROUP='%s',CUTOFF=INTERVAL '0' SECOND)", broker(t), destination, source, end.Format(time.RFC3339Nano), group)
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
	p := producer(t, nil)
	for i := range 60 {
		publish(t, p, &k.Message{TopicPartition: k.TopicPartition{Topic: &source, Partition: int32(i % 2)}, Key: []byte(fmt.Sprintf("key-%d", i%7)), Value: []byte(fmt.Sprintf("record-%d", i)), Timestamp: time.Now()})
	}
	for i := range 2 {
		publish(t, p, &k.Message{TopicPartition: k.TopicPartition{Topic: &source, Partition: int32(i)}, Value: []byte("trigger"), Timestamp: end.Add(time.Second)})
	}
	for range 2 {
		select {
		case r := <-results:

			// Then
			if r.code != 0 {
				t.Fatalf("worker exit=%d: %s", r.code, r.detail)
			}
		case <-ctx.Done():
			t.Fatal("distributed workers failed to finish independently")
		}
	}
	var output bytes.Buffer
	if code := app.Execute(ctx, []string{"run", "--sql", statement}, nil, io.Discard, &output); code != 0 {
		t.Fatalf("sole-worker confirmation exit=%d: %s", code, output.String())
	}
	c := consumer(t, destination+"-reader")
	var assignments []k.TopicPartition
	for i := range 3 {
		assignments = append(assignments, k.TopicPartition{Topic: &destination, Partition: int32(i), Offset: 0})
	}
	if err := c.Assign(assignments); err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]int)
	keyPartitions := make(map[string]int32)
	for len(seen) < 60 {
		r := read(t, c)
		if string(r.Value) == "trigger" {
			t.Fatal("stop trigger was published")
		}
		seen[string(r.Value)]++
		if part, ok := keyPartitions[string(r.Key)]; ok && part != r.TopicPartition.Partition {
			t.Fatal("same key routed to different partitions")
		}
		keyPartitions[string(r.Key)] = r.TopicPartition.Partition
	}
	for i := range 60 {
		if seen[fmt.Sprintf("record-%d", i)] == 0 {
			t.Fatal("record lost during worker completion")
		}
	}
}
