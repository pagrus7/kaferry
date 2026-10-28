package app

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/franz"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/pagrus7/kaferry/internal/sql"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

type missingBrokerClients struct {
	franz.Clients
	failed        atomic.Bool
	created       atomic.Int64
	beforeFailure func()
}

func (c *missingBrokerClients) Transaction(options ...kgo.Opt) (franz.TransactionClient, error) {
	client, err := c.Clients.Transaction(options...)
	if err != nil {
		return nil, err
	}
	c.created.Add(1)
	return missingBrokerProducer{TransactionClient: client, owner: c}, nil
}

type missingBrokerProducer struct {
	franz.TransactionClient
	owner *missingBrokerClients
}

func (p missingBrokerProducer) Produce(ctx context.Context, record *kgo.Record, callback func(*kgo.Record, error)) {
	if record.Topic == "output" && p.owner.failed.CompareAndSwap(false, true) {
		if p.owner.beforeFailure != nil {
			p.owner.beforeFailure()
		}
		callback(record, errors.New("unknown broker"))
		return
	}
	p.TransactionClient.Produce(ctx, record, callback)
}

func TestPendingKafkaBatchRecoversMissingBrokerWithoutDuplicatingOccurrences(t *testing.T) {
	for _, sourceKind := range []string{"TOPIC", "ARCHIVE"} {
		t.Run(sourceKind, func(t *testing.T) {
			// Given a pending output batch in which one record fails while its peers may already be appended.
			source, destination := clusters(t, 2)
			input := "TOPIC 'input@src'"
			expected := map[string]int{}
			owned := int64(2)
			if sourceKind == "TOPIC" {
				seed(t, source, 2, 4)
				for part := range 2 {
					for offset := range 4 {
						expected[fmt.Sprintf("%d:%d", part, offset)] = 1
					}
				}
			} else {
				root := t.TempDir()
				expected = archiveInput(t, root)
				input = "ARCHIVE '" + root + "'"
				owned = 1
			}
			ast, err := sql.NewParser().Parse(fmt.Sprintf("INSERT INTO TOPIC 'output@dst' SELECT * FROM %s WITH(GROUP='producer-recovery') WHERE RECORD_OFFSET<LATEST", input))
			require.NoError(t, err)
			p, err := plan.NewPlanner(environment{}).Build(ast, plan.Profiles{"src": {"bootstrap.servers": strings.Join(source, ",")}, "dst": {"bootstrap.servers": strings.Join(destination, ",")}})
			require.NoError(t, err)
			cfg := config.Defaults("")
			state := t.TempDir()
			cfg.Worker.CheckpointDir = &state
			cfg.Worker.RecordCap, cfg.Worker.PartitionRecordCap = 8, 8
			clients := &missingBrokerClients{Clients: franz.NewClients()}
			// When the adapter fences and reopens its failed producer, then only committed authority decides whether the batch can retry.
			require.NoError(t, Run(testContext(t), p, cfg, Options{ProgressTopic: "progress", Clients: clients}))
			require.True(t, clients.failed.Load())
			require.Greater(t, clients.created.Load(), owned)
			require.NoError(t, Run(testContext(t), p, cfg, Options{ProgressTopic: "progress"}))
			require.Equal(t, expected, output(t, destination, 2))
		})
	}
}

func TestProducerRecoveryRejectsProgressAdvancedByReplacementOwner(t *testing.T) {
	// Given an old owner blocked before its first data-bearing preparation fails.
	source, destination := clusters(t, 1)
	seed(t, source, 1, 6)
	p := query(t, source, destination, "WHERE RECORD_OFFSET<LATEST", "")
	cfg := config.Defaults("")
	cfg.Worker.RecordCap, cfg.Worker.PartitionRecordCap = 1, 1
	ready, resume := make(chan struct{}), make(chan struct{})
	var release sync.Once
	clients := &missingBrokerClients{Clients: franz.NewClients(), beforeFailure: func() { close(ready); <-resume }}
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, p, cfg, Options{ProgressTopic: "progress", Clients: clients}) }()
	finished := false
	t.Cleanup(func() {
		release.Do(func() { close(resume) })
		cancel()
		if !finished {
			<-done
		}
	})
	select {
	case <-ready:
	case <-time.After(10 * time.Second):
		t.Fatal("old owner did not prepare output")
	}
	// When a replacement completes the job, then the old worker must not retry its stale batch after reloading the advanced cursor.
	removeDeadMember(t, source, p.Group)
	require.NoError(t, Run(testContext(t), p, cfg, Options{ProgressTopic: "progress"}))
	release.Do(func() { close(resume) })
	err := <-done
	finished = true
	require.ErrorContains(t, err, "output authority changed during producer recovery")
	got := output(t, destination, 1)
	require.Len(t, got, 6)
	for offset := range 6 {
		require.Equal(t, 1, got[fmt.Sprintf("0:%d", offset)])
	}
}

func TestMissingBrokerDoesNotDiscardEarlierPreparedBatch(t *testing.T) {
	// Given A was prepared, while a fixed clock keeps its progress commit pending.
	source, destination := clusters(t, 1)
	seed(t, source, 1, 1)
	p := query(t, source, destination, "", "")
	p.WorkerStopAfterRecords = 2
	cfg := config.Defaults("")
	cfg.Worker.RecordCap, cfg.Worker.PartitionRecordCap = 8, 8
	clock := &controlledClock{}
	clock.milliseconds.Store(time.Now().UnixMilli())
	clients := &missingBrokerClients{Clients: franz.NewClients()}
	clients.failed.Store(true)
	producer := client(t, source)
	var batches atomic.Int64
	var seedErr error
	// When B arrives after A's flush and fails preparation, then the adapter must not replace the producer and discard A.
	err := Run(testContext(t), p, cfg, Options{ProgressTopic: "progress", Clients: clients, Clock: clock, Hook: func(point string, _ int32) {
		if point == "flushed" && batches.Add(1) == 1 {
			clients.failed.Store(false)
			seedErr = producer.ProduceSync(testContext(t), recordFor("input", 0, "0:1")).FirstErr()
		}
	}})
	require.NoError(t, seedErr)
	require.ErrorContains(t, err, "unknown broker")
	require.EqualValues(t, 1, batches.Load())
	require.EqualValues(t, 1, clients.created.Load(), "an earlier prepared batch prevents producer replacement")
	require.True(t, clients.failed.Load(), "B must encounter the injected failure")
	require.Empty(t, output(t, destination, 1))
	report, err := (Inspection{Options: Options{ProgressTopic: "progress"}}).Explain(testContext(t), p, cfg.Worker, core.NoopStopObserver{})
	require.NoError(t, err)
	require.Len(t, report.Partitions, 1)
	require.True(t, report.Partitions[0].Saved)
	require.Zero(t, report.Partitions[0].Start, "authority must not advance past the aborted A+B transaction")
	// When the job restarts, then both A and B are committed exactly once.
	require.NoError(t, Run(testContext(t), p, cfg, Options{ProgressTopic: "progress"}))
	require.Equal(t, map[string]int{"0:0": 1, "0:1": 1}, output(t, destination, 1))
}

type fencingRetryClients struct {
	franz.Clients
	failures atomic.Int64
}

func (c *fencingRetryClients) Transaction(options ...kgo.Opt) (franz.TransactionClient, error) {
	client, err := c.Clients.Transaction(options...)
	if err != nil {
		return nil, err
	}
	return &fencingRetryProducer{TransactionClient: client, owner: c}, nil
}

type fencingRetryProducer struct {
	franz.TransactionClient
	owner  *fencingRetryClients
	failed bool
}

func (p *fencingRetryProducer) ProducerID(ctx context.Context) (int64, int16, error) {
	if !p.failed {
		p.failed = true
		p.owner.failures.Add(1)
		return 0, 0, net.ErrClosed
	}
	return p.TransactionClient.ProducerID(ctx)
}
func TestResumeRetriesProducerFencingBeforeLoadingProgress(t *testing.T) {
	for _, archive := range []bool{false, true} {
		t.Run(fmt.Sprint("archive=", archive), func(t *testing.T) {
			// Given an acknowledged prefix and a restarted connection failing the first producer initialization.
			source, destination := clusters(t, 1)
			seed(t, source, 1, 6)
			p := query(t, source, destination, "WHERE RECORD_OFFSET<LATEST", "")
			root := ""
			if archive {
				root = t.TempDir()
				p, _ = capturePlan(t, source, root, "fencing-retry", false)
			}
			p.WorkerStopAfterRecords = 2
			cfg := config.Defaults("")
			require.NoError(t, Run(testContext(t), p, cfg, Options{ProgressTopic: "progress"}))
			clients := &fencingRetryClients{Clients: franz.NewClients()}
			p.WorkerStopAfterRecords = 0
			// When the job resumes, then producer fencing retries before authority is loaded and every occurrence commits once.
			require.NoError(t, Run(testContext(t), p, cfg, Options{ProgressTopic: "progress", Clients: clients}))
			require.Positive(t, clients.failures.Load())
			var got map[string]int
			if archive {
				got = archiveValues(t, root)
			} else {
				got = output(t, destination, 1)
			}
			expected := map[string]int{}
			for offset := range 6 {
				expected[fmt.Sprintf("0:%d", offset)] = 1
			}
			require.Equal(t, expected, got)
		})
	}
}
