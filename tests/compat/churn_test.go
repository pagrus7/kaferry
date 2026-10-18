//go:build integration && fault

package compat

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/require"
)

func TestRebalanceChurnPreservesConfirmedProgressUnderFailFastCommits(t *testing.T) {
	// Given a committing worker with small caps and an ongoing source stream.
	a := admin(t)
	src, dst := topic(t, a, 2), topic(t, a, 2)
	group := src + "-churn"
	path := configuration(t, "record_cap=32\npartition_record_cap=8\n")
	statement := fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' OPTIONS(GROUP='%s',START=EARLIEST)", dst, src, group)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	results := make(chan workerResult, 8)
	start := func() context.CancelFunc {
		workerCtx, stop := context.WithCancel(ctx)
		go func() { results <- loggedWorker(workerCtx, statement, path) }()
		return stop
	}
	stopA := start()
	defer stopA()
	waitMembers(t, a, group, 1, 2, results)
	p := producer(t, nil)
	for cycle := range 3 {

		// When a second worker joins, records flow, and that worker leaves again.
		stopB := start()
		defer stopB()
		waitMembers(t, a, group, 2, 2, results)
		for i := range 96 {
			id := cycle*96 + i
			publish(t, p, &k.Message{TopicPartition: k.TopicPartition{Topic: &src, Partition: int32(id % 2)}, Key: []byte(fmt.Sprint(id % 7)), Value: []byte(fmt.Sprint(id)), Timestamp: time.Now()})
		}
		waitTopicCount(t, a, dst, 2, int64((cycle+1)*96))
		stopB()
		select {
		case result := <-results:

			// Then this measured run exits cleanly and the survivor reacquires ownership.
			require.Zero(t, result.code, "churn worker failed: %s", result.detail)
		case <-ctx.Done():
			t.Fatal("churn worker shutdown timed out")
		}
		waitMembers(t, a, group, 1, 2, results)
	}
	stopA()
	select {
	case result := <-results:
		require.Zero(t, result.code, "committing worker failed: %s", strings.TrimSpace(result.detail))
	case <-ctx.Done():
		t.Fatal("final churn shutdown timed out")
	}

	// Then all consumed offsets are committed despite repeated ownership changes.
	for _, part := range committedPartitions(t, a, group, src, 2) {
		require.Equal(t, k.Offset(144), part.Offset, "churn lost source progress")
	}
	reader := consumer(t, dst+"-verify")
	require.NoError(t, reader.Assign([]k.TopicPartition{{Topic: &dst, Partition: 0, Offset: 0}, {Topic: &dst, Partition: 1, Offset: 0}}))
	seen := make(map[string]bool)
	for len(seen) < 288 {
		seen[string(read(t, reader).Value)] = true
	}
	for id := range 288 {
		if !seen[fmt.Sprint(id)] {
			t.Fatal("record lost through churn", id)
		}
	}
}

func waitTopicCount(t *testing.T, a *k.AdminClient, topic string, partitions int, count int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	request := make(map[k.TopicPartition]k.OffsetSpec)
	for id := range partitions {
		request[k.TopicPartition{Topic: &topic, Partition: int32(id)}] = k.LatestOffsetSpec
	}
	for {
		result, err := a.ListOffsets(ctx, request)
		total := int64(0)
		if err == nil && len(result.ResultInfos) == partitions {
			for _, info := range result.ResultInfos {
				require.Equal(t, k.ErrNoError, info.Error.Code())
				total += int64(info.Offset)
			}
			if total >= count {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("destination did not catch up", total, count, err)
		case <-time.After(20 * time.Millisecond):
		}
	}
}
