//go:build integration

package compat

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/src/internal/inspect"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestLatestEndRemainsPinnedAfterCompletion(t *testing.T) {
	// Given a finite source and a named group with no saved progress.
	a := admin(t)
	source, destination := topic(t, a, 1), topic(t, a, 1)
	p := producer(t)
	const initial, appended = 4, 3
	for offset := range initial {
		publish(t, p, &kgo.Record{Topic: source, Partition: 0, Value: []byte(fmt.Sprintf("initial-%d", offset))})
	}
	group := source + "-latest"
	config := configuration(t)
	query := fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' WITH (GROUP='%s') WHERE RECORD_OFFSET < LATEST", destination, source, group)
	run := func() copySummary {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		result := binaryWorker(t, ctx, query, config)
		require.Zero(t, result.code, result.detail)
		waitMembers(t, a, group, 0, 0, nil)
		return summaryFrom(t, result.detail)
	}

	// When the first copy finishes at LATEST.
	first := run()
	// Then all original records are confirmed and their exclusive end is saved.
	require.Equal(t, initial, first.Submitted)
	require.Equal(t, initial, first.Confirmed)
	require.EqualValues(t, initial, topicRecordCount(t, a, destination, 1))
	require.EqualValues(t, initial, committedPartitions(t, a, group, source, 1)[0].At)

	// Given later appends beyond the completed group's captured end.
	for offset := range appended {
		publish(t, p, &kgo.Record{Topic: source, Partition: 0, Value: []byte(fmt.Sprintf("appended-%d", offset))})
	}
	require.EqualValues(t, initial+appended, topicRecordCount(t, a, source, 1))
	// When the exact same query and group run again.
	second := run()
	// Then no records are submitted or confirmed, and only the original records remain in the destination.
	require.Zero(t, second.Submitted)
	require.Zero(t, second.Confirmed)
	require.EqualValues(t, initial, topicRecordCount(t, a, destination, 1))
	reader := consumer(t)
	assign(t, reader, destination, 1)
	for offset := range initial {
		require.Equal(t, fmt.Sprintf("initial-%d", offset), string(read(t, reader).Value))
	}

	// When inspecting the saved job through the CLI.
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	status := startCLI(t, ctx, testBinary(t), "", "status", "-e", query, "-c", config, "--json")
	require.NoError(t, status.wait(), status.logs())
	require.NoError(t, ctx.Err(), status.logs())
	// Then it is complete at the original end, even though the source now extends beyond it.
	var report inspect.StatusReport
	require.NoError(t, json.Unmarshal([]byte(status.logs()), &report))
	require.Equal(t, "complete", report.Verdict)
	require.Equal(t, group, report.Group)
	require.Len(t, report.Partitions, 1)
	require.EqualValues(t, 0, report.Partitions[0].ID)
	require.Equal(t, new(int64(initial)), report.Partitions[0].Position)
	require.Equal(t, new(int64(initial)), report.Partitions[0].End)
}
