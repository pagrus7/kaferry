//go:build integration && fault

package compat

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestRebalanceChurnPreservesConfirmedProgressUnderFailFastCommits(t *testing.T) {
	// Given a committing worker with small caps and an ongoing source stream.
	a := admin(t)
	src, dst := topic(t, a, 2), topic(t, a, 2)
	group := src + "-churn"
	path := configuration(t, "record_cap=32\npartition_record_cap=8\n")
	statement := fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' WITH (GROUP='%s', START=EARLIEST) ", dst, src, group)
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
	p := producer(t)
	for cycle := range 3 {

		// When a second worker joins, records flow, and that worker leaves again.
		stopB := start()
		defer stopB()
		waitMembers(t, a, group, 2, 2, results)
		for i := range 96 {
			id := cycle*96 + i
			publish(t, p, &kgo.Record{Topic: src, Partition: int32(id % 2), Key: []byte(fmt.Sprint(id % 7)), Value: []byte(fmt.Sprint(id)), Timestamp: time.Now()})
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
		require.Equal(t, int64(144), part.At, "churn lost source progress")
	}
	reader := consumer(t)
	assign(t, reader, dst, 2)
	seen := make(map[string]bool)
	for len(seen) < 288 {
		record := read(t, reader)
		id, err := strconv.Atoi(string(record.Value))
		require.NoError(t, err)
		require.EqualValues(t, id%2, record.Partition, "preserved routing survives reassignment")
		seen[string(record.Value)] = true
	}
	for id := range 288 {
		if !seen[fmt.Sprint(id)] {
			t.Fatal("record lost through churn", id)
		}
	}
}
