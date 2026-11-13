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
	// Given an uncommitted four-partition source and two processes sharing a group and archive directory.
	const partitions, count = 4, 400
	a := admin(t)
	source, destination := topic(t, a, partitions), topic(t, a, partitions)
	group := source + "-archive"
	archive := filepath.Join(t.TempDir(), "archive")
	p := producer(t, kgo.TransactionalID(source+"-capture"))
	require.NoError(t, p.BeginTransaction())
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = p.EndTransaction(cleanup, kgo.TryAbort)
	}()
	expected := make(map[string]int, count)
	for id := range count {
		value := fmt.Sprintf("record-%d", id)
		expected[value] = 1
		publish(t, p, &kgo.Record{Topic: source, Partition: int32(id % partitions), Value: []byte(value)})
	}
	// A one-record cap exercises shared archive durability throughout capture.
	config := configuration(t, "record_cap=1")
	// The literal end is already replicated, but read-committed capture must wait for the transaction.
	statement := fmt.Sprintf("INSERT INTO ARCHIVE '%s' SELECT * FROM TOPIC '%s' WITH (GROUP='%s', START=EARLIEST) WHERE RECORD_OFFSET < %d", archive, source, group, count/partitions)
	binary := testBinary(t)
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	// When both capture processes acquire partitions before the transaction makes records readable.
	workers := []*cliProcess{
		startCLI(t, ctx, binary, statement, "run", "--file", "-", "--config", config),
		startCLI(t, ctx, binary, statement, "run", "--file", "-", "--config", config),
	}
	waitMembers(t, a, group, 2, partitions, nil)
	groups, err := a.DescribeGroups(ctx, group)
	require.NoError(t, err)
	require.NoError(t, groups[group].Err)
	require.Len(t, groups[group].Members, len(workers))
	for _, member := range groups[group].Members {
		assignment, ok := member.Assigned.AsConsumer()
		require.True(t, ok, "capture member must have a consumer assignment")
		owned := 0
		for _, topic := range assignment.Topics {
			owned += len(topic.Partitions)
		}
		require.Positive(t, owned, "each capture process must own partitions before commit")
	}
	require.NoError(t, p.EndTransaction(ctx, kgo.TryCommit))
	confirmed := 0
	for _, worker := range workers {
		require.NoError(t, worker.wait(), worker.logs())
		require.NoError(t, ctx.Err(), "capture must finish before cancellation: %s", worker.logs())
		summary := summaryFrom(t, worker.logs())
		require.Positive(t, summary.Confirmed, "each concurrent process must capture records")
		require.Equal(t, summary.Submitted, summary.Confirmed)
		confirmed += summary.Confirmed
	}

	// Then both exit successfully after capturing exactly the fixed source range.
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
