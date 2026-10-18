//go:build integration

package compat

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pagrus7/kaferry/internal/app"
	"github.com/pagrus7/kaferry/internal/inspect"
	"github.com/stretchr/testify/require"
)

func TestSavedStopsDescribeEveryCurrentKafkaPartition(t *testing.T) {
	// Given two partitions: one reaches the timestamp rule, the other stops at the tip.
	a := admin(t)
	source, destination := topic(t, a, 2), topic(t, a, 1)
	producer := producer(t, nil)
	stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	end := stamp.Add(time.Minute)
	for _, record := range []struct {
		partition int32
		time      time.Time
		value     string
	}{{0, stamp, "included"}, {0, end.Add(time.Second), "stop"}, {1, stamp, "included"}} {
		publish(t, producer, &k.Message{TopicPartition: k.TopicPartition{Topic: &source, Partition: record.partition}, Timestamp: record.time, Value: []byte(record.value)})
	}
	group := source + "-status"
	config := configuration(t)
	query := fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' OPTIONS(GROUP='%s',CUTOFF=INTERVAL '0' SECOND)", destination, source, end.Format(time.RFC3339Nano), group)
	// When a worker finishes both partitions and saves progress.
	execute(t, query, config)
	waitMembers(t, a, group, 0, 0, nil)
	before := committedPartitions(t, a, group, source, 2)
	var out, errout bytes.Buffer
	code := app.Execute(t.Context(), []string{"status", "-e", query, "-c", config, "--json"}, nil, &out, &errout)
	// Then status reports both durable reasons and leaves offsets/metadata unchanged.
	var report inspect.StatusReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil || code != 0 || report.Verdict != "stopped" || len(report.Partitions) != 2 {
		t.Fatal(code, out.String(), errout.String(), err)
	}
	if report.Partitions[0].Stop == nil || report.Partitions[0].Stop.Reason != "timestamp" || report.Partitions[1].Stop == nil || report.Partitions[1].Stop.Reason != "tip" {
		t.Fatal(report.Partitions)
	}
	after := committedPartitions(t, a, group, source, 2)
	for n := range before {
		if before[n].Offset != after[n].Offset || before[n].Metadata == nil || after[n].Metadata == nil || *before[n].Metadata != *after[n].Metadata {
			t.Fatal("status changed progress", before, after)
		}
	}
	// When a current partition has no saved progress, then the same query is incomplete.
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	result, err := a.CreatePartitions(ctx, []k.PartitionsSpecification{{Topic: source, IncreaseTo: 3}})
	if err != nil || len(result) != 1 || result[0].Error.Code() != k.ErrNoError {
		t.Fatal(result, err)
	}
	waitTopicReady(t, a, source, 3)
	out.Reset()
	errout.Reset()
	code = app.Execute(t.Context(), []string{"status", "-e", query, "-c", config, "--json"}, nil, &out, &errout)
	if err := json.Unmarshal(out.Bytes(), &report); err != nil || code != 4 || report.Verdict != "incomplete" || len(report.Partitions) != 3 || report.Partitions[2].Position != nil {
		t.Fatal(code, out.String(), errout.String(), err)
	}
	// Then no status observation produced more destination records.
	consumer := consumer(t, destination+"-verify")
	require.NoError(t, consumer.Assign([]k.TopicPartition{{Topic: &destination, Partition: 0, Offset: 0}}))
	for range 2 {
		if got := read(t, consumer); string(got.Value) != "included" {
			t.Fatal(got)
		}
	}
}
