//go:build integration

package compat

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaArchiveConcurrentCapture(t *testing.T) {
	// Given a finite four-partition source and two processes sharing a group and archive directory.
	const partitions, count = 4, 400
	a := admin(t)
	source, destination := topic(t, a, partitions), topic(t, a, partitions)
	group := source + "-archive"
	archive := filepath.Join(t.TempDir(), "archive")
	p := producer(t)
	expected := make(map[string]int, count)
	for id := range count {
		value := fmt.Sprintf("record-%d", id)
		expected[value] = 1
		publish(t, p, &kgo.Record{Topic: source, Partition: int32(id % partitions), Value: []byte(value)})
	}
	// A one-record cap forces durability work throughout capture, keeping concurrent ownership observable.
	config := configuration(t, "record_cap=1")
	statement := fmt.Sprintf("INSERT INTO ARCHIVE '%s' SELECT * FROM TOPIC '%s' WITH (GROUP='%s', START=EARLIEST) WHERE RECORD_OFFSET < LATEST", archive, source, group)
	binary := testBinary(t)
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	// When both capture processes start before either is awaited.
	workers := []*cliProcess{
		startCLI(t, ctx, binary, statement, "run", "--file", "-", "--config", config),
		startCLI(t, ctx, binary, statement, "run", "--file", "-", "--config", config),
	}
	waitMembers(t, a, group, 2, partitions, nil)
	confirmed := 0
	for _, worker := range workers {
		require.NoError(t, worker.wait(), worker.logs())
		require.NoError(t, ctx.Err(), "capture must finish before cancellation: %s", worker.logs())
		summary := summaryFrom(t, worker.logs())
		require.Positive(t, summary.Confirmed, "each concurrent process must capture records")
		require.Equal(t, summary.Submitted, summary.Confirmed)
		confirmed += summary.Confirmed
	}

	// Then both exit successfully after capturing exactly the pinned source range.
	require.Equal(t, count, confirmed)
	for _, partition := range committedPartitions(t, a, group, source, partitions) {
		require.EqualValues(t, count/partitions, partition.At)
	}
	// When replaying their shared archive into a fresh topic.
	replay := fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM ARCHIVE '%s' WITH (GROUP='restore')", destination, archive)
	result := binaryWorker(t, ctx, replay, config)
	require.Zero(t, result.code, result.detail)
	require.Equal(t, count, summaryFrom(t, result.detail).Confirmed)

	// Then every source record ID occurs exactly once, with no extra records.
	require.EqualValues(t, count, topicRecordCount(t, a, destination, partitions))
	reader := consumer(t)
	assign(t, reader, destination, partitions)
	actual := make(map[string]int, count)
	for range count {
		value := string(read(t, reader).Value)
		require.Contains(t, expected, value, "replay must contain only source record IDs")
		actual[value]++
		require.Equal(t, 1, actual[value], "source record ID must not be duplicated")
	}
	require.Equal(t, expected, actual, "every source record ID must be replayed exactly once")
}
