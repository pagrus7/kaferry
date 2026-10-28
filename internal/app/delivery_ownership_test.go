package app

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestLiveOwnerFencedAfterAdvisorySourceCommit(t *testing.T) {
	// Given an old owner paused after its source-group offset advances, before output commits.
	source, destination := clusters(t, 1)
	seed(t, source, 1, 6)
	p := query(t, source, destination, "WHERE RECORD_OFFSET < LATEST", "")
	cfg := config.Defaults("")
	cfg.Worker.RecordCap = 1
	cfg.Worker.PartitionRecordCap = 1
	prepared, resume := make(chan struct{}), make(chan struct{})
	var data atomic.Bool
	firstDone := make(chan error, 1)
	firstCtx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	go func() {
		firstDone <- Run(firstCtx, p, cfg, Options{ProgressTopic: "progress", Hook: func(point string, _ int32) {
			if point == "flushed" {
				data.Store(true)
			}
			if point == "prepared" && data.CompareAndSwap(true, false) {
				close(prepared)
				<-resume
			}
		}})
	}()
	released := false
	t.Cleanup(func() {
		if !released {
			close(resume)
		}
		cancel()
		select {
		case <-firstDone:
		case <-time.After(5 * time.Second):
			t.Error("old worker did not stop")
		}
	})
	select {
	case <-prepared:
	case <-time.After(10 * time.Second):
		t.Fatal("old worker never prepared output")
	}
	offsets, err := kadm.NewClient(client(t, source)).FetchOffsets(testContext(t), p.Group)
	require.NoError(t, err)
	require.Equal(t, int64(1), offsets["input"][0].At)
	// When the live member is evicted, a replacement fences its destination transaction.
	removeDeadMember(t, source, p.Group)
	require.NoError(t, Run(testContext(t), p, cfg, Options{ProgressTopic: "progress"}))
	close(resume)
	released = true
	// Then the zombie fails and the replacement copies from the destination's earlier position.
	select {
	case err := <-firstDone:
		require.Error(t, err)
		firstDone <- nil
	case <-time.After(10 * time.Second):
		t.Fatal("fenced owner did not finish")
	}
	got := output(t, destination, 1)
	require.Len(t, got, 6)
	for offset := range 6 {
		require.Equal(t, 1, got[fmt.Sprintf("0:%d", offset)])
	}
}

func TestLegacyGroupAndRecreatedDestinationFailClosed(t *testing.T) {
	for _, mode := range []string{"legacy", "destination-recreated", "progress-recreated", "delete-policy"} {
		t.Run(mode, func(t *testing.T) {
			// Given an existing group or a completed exactly-once copy.
			source, destination := clusters(t, 1)
			seed(t, source, 1, 3)
			p := query(t, source, destination, "WHERE RECORD_OFFSET < LATEST", "")
			cfg := config.Defaults("")
			if mode == "legacy" {
				// A legacy offset with no destination checkpoint cannot establish output completeness.
				offsets := kadm.Offsets{}
				offsets.Add(kadm.Offset{Topic: "input", Partition: 0, At: 1})
				response, err := kadm.NewClient(client(t, source)).CommitOffsets(testContext(t), p.Group, offsets)
				require.NoError(t, err)
				require.NoError(t, response.Error())
			} else {
				require.NoError(t, Run(testContext(t), p, cfg, Options{ProgressTopic: "progress"}))
			}
			admin := kadm.NewClient(client(t, destination))
			if mode == "destination-recreated" || mode == "progress-recreated" {
				name := "output"
				if mode == "progress-recreated" {
					name = "progress"
				}
				deleted, err := admin.DeleteTopics(testContext(t), name)
				require.NoError(t, err)
				require.NoError(t, deleted.Error())
				topic(t, destination, name, 1, name == "progress")
			}
			if mode == "delete-policy" {
				value := "compact,delete"
				changed, err := admin.AlterTopicConfigs(testContext(t), []kadm.AlterConfig{{Op: kadm.SetConfig, Name: "cleanup.policy", Value: &value}}, "progress")
				require.NoError(t, err)
				require.NoError(t, changed[0].Err)
			}
			// When resumed, then incompatible or missing authorities stop the run.
			require.Error(t, Run(testContext(t), p, cfg, Options{ProgressTopic: "progress"}))
		})
	}
}

func TestMixedExecutionProtocolsCannotJoin(t *testing.T) {
	// Given a live ordinary consumer using the same group as an exactly-once job.
	source, destination := clusters(t, 1)
	seed(t, source, 1, 3)
	p := query(t, source, destination, "WHERE RECORD_OFFSET < LATEST", "")
	assigned := make(chan struct{}, 1)
	native := client(t, source, kgo.ConsumerGroup(p.Group), kgo.ConsumeTopics("input"), kgo.OnPartitionsAssigned(func(context.Context, *kgo.Client, map[string][]int32) {
		select {
		case assigned <- struct{}{}:
		default:
		}
	}))
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			native.PollRecords(ctx, 1)
		}
	}()
	defer func() { cancel(); <-done }()
	select {
	case <-assigned:
	case <-time.After(5 * time.Second):
		t.Fatal("native consumer did not join")
	}
	// When an exactly-once member attempts to join, then the broker rejects mixed semantics before output.
	require.Error(t, Run(testContext(t), p, config.Defaults(""), Options{ProgressTopic: "progress"}))
	require.Empty(t, output(t, destination, 1))
}

func TestIndependentGroupsDoNotFenceEachOther(t *testing.T) {
	// Given group/partition pairs that have the same undelimited text suffix.
	source, destination := clusters(t, 13)
	seed(t, source, 13, 1)
	first := query(t, source, destination, "WHERE PARTITION IN(2) AND RECORD_OFFSET < LATEST", "")
	prefix := fmt.Sprintf("group-%d", time.Now().UnixNano())
	first.Group = prefix + "1"
	second := query(t, source, destination, "WHERE PARTITION IN(12) AND RECORD_OFFSET < LATEST", "")
	second.Group = prefix
	cfg := config.Defaults("")
	cfg.Worker.RecordCap = 1
	cfg.Worker.PartitionRecordCap = 1
	prepared, resume := make(chan struct{}), make(chan struct{})
	released, finished := false, false
	var data atomic.Bool
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	go func() {
		done <- Run(ctx, first, cfg, Options{ProgressTopic: "progress", Hook: func(point string, _ int32) {
			if point == "flushed" {
				data.Store(true)
			}
			if point == "prepared" && data.CompareAndSwap(true, false) {
				close(prepared)
				<-resume
			}
		}})
	}()
	t.Cleanup(func() {
		if !released {
			close(resume)
		}
		cancel()
		if !finished {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("first group did not stop")
			}
		}
	})
	select {
	case <-prepared:
	case err := <-done:
		finished = true
		t.Fatalf("first group stopped before preparing output: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("first group did not prepare output")
	}
	// When the independent second group commits, then the first producer remains valid.
	require.NoError(t, Run(testContext(t), second, cfg, Options{ProgressTopic: "progress"}))
	close(resume)
	released = true
	err := <-done
	finished = true
	require.NoError(t, err)
	require.Equal(t, map[string]int{"2:0": 1, "12:0": 1}, output(t, destination, 13))
}
