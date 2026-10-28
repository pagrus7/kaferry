package app

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
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
