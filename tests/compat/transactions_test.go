//go:build integration

package compat

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/require"
)

func tipSamples(samples chan<- int64) func(slog.Record) {
	return func(record slog.Record) {
		if record.Message != "tip watermark sampled" {
			return
		}
		record.Attrs(func(attr slog.Attr) bool {
			if attr.Key == "high_watermark" {
				select {
				case samples <- attr.Value.Int64():
				default:
				}
			}
			return true
		})
	}
}

func TestNoMatchLookupOpenTransactionAndTrailingMarker(t *testing.T) {
	// Given
	for _, abort := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit", true: "abort"}[abort], func(t *testing.T) {
			a := admin(t)
			src := topic(t, a, 1)
			p := producer(t, k.ConfigMap{"transactional.id": src + "-open"})
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			require.NoError(t, p.InitTransactions(ctx))
			require.NoError(t, p.BeginTransaction())
			stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
			publish(t, p, &k.Message{TopicPartition: k.TopicPartition{Topic: &src, Partition: 0}, Value: []byte("transactional"), Timestamp: stamp})
			path := filepath.Join(t.TempDir(), "out.cbor")
			group := src + "-copy"
			cfg := configuration(t)
			statement := fmt.Sprintf("INSERT INTO FILE cbor('%s') SELECT * FROM TOPIC '%s' WHERE RECORD_TIMESTAMP >= TIMESTAMP '%s' AND RECORD_TIMESTAMP < TIMESTAMP '%s' OPTIONS(GROUP='%s',CUTOFF=INTERVAL '0' SECOND)", path, src, stamp.Add(-time.Second).Format(time.RFC3339Nano), stamp.Add(time.Second).Format(time.RFC3339Nano), group)
			result := make(chan workerResult, 1)
			samples := make(chan int64, 1)
			go func() {
				// When
				result <- loggedWorker(ctx, statement, cfg, tipSamples(samples))
			}()
			finished := false
			t.Cleanup(func() {
				cancel()
				if !finished {
					<-result
				}
			})
			deadline := time.Now().Add(10 * time.Second)
			initialized := false
			for time.Now().Before(deadline) {
				select {
				case r := <-result:
					finished = true
					t.Fatalf("copy ended before transaction resolution: %d %s", r.code, r.detail)
				default:
				}
				positions, err := a.ListConsumerGroupOffsets(ctx, []k.ConsumerGroupTopicPartitions{{Group: group, Partitions: []k.TopicPartition{{Topic: &src, Partition: 0}}}})
				if err == nil && len(positions.ConsumerGroupsTopicPartitions) == 1 && len(positions.ConsumerGroupsTopicPartitions[0].Partitions) == 1 && positions.ConsumerGroupsTopicPartitions[0].Partitions[0].Offset == 0 {
					initialized = true
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal("initialization timed out")
				case <-time.After(20 * time.Millisecond):
				}
			}

			// Then
			if !initialized {
				t.Fatal("no-match lookup did not initialize at LSO")
			}
			// When the worker samples the tip while the transaction remains open.
			select {
			case high := <-samples:
				// Then H includes the unread transaction, rather than the stable offset 0.
				require.Equal(t, int64(1), high, "sampled high watermark=%d, want 1", high)
			case r := <-result:
				finished = true
				t.Fatalf("copy ended before sampling the open transaction: %d %s", r.code, r.detail)
			case <-ctx.Done():
				t.Fatal("open transaction was not included in the tip sample")
			}
			if abort {
				require.NoError(t, p.AbortTransaction(ctx))
			} else {
				require.NoError(t, p.CommitTransaction(ctx))
			}
			select {
			case r := <-result:
				finished = true
				require.Zero(t, r.code, "transaction tail exit=%d: %s", r.code, r.detail)
			case <-ctx.Done():
				t.Fatal("control marker did not complete tip")
			}
			want := 1
			if abort {
				want = 0
			}
			if records := archiveRecords(t, path); len(records) != want {
				t.Fatalf("visible=%d want=%d", len(records), want)
			}
			execute(t, statement, cfg)
			if records := archiveRecords(t, path); len(records) != want {
				t.Fatal("control cursor failed to prevent recopy")
			}
		})
	}
}

func TestOpenTransactionReachesTipWaitInsteadOfSuccessfulCompletion(t *testing.T) {
	// Given an in-window record in a transaction that stays open past tip_wait.
	a := admin(t)
	src, dst := topic(t, a, 1), topic(t, a, 1)
	p := producer(t, k.ConfigMap{"transactional.id": src + "-open"})
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	require.NoError(t, p.InitTransactions(ctx))
	require.NoError(t, p.BeginTransaction())
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = p.AbortTransaction(cleanup)
	}()
	stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	publish(t, p, &k.Message{TopicPartition: k.TopicPartition{Topic: &src, Partition: 0}, Value: []byte("transactional"), Timestamp: stamp})
	statement := fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' WHERE RECORD_TIMESTAMP >= TIMESTAMP '%s' AND RECORD_TIMESTAMP < TIMESTAMP '%s' OPTIONS(GROUP='%s',CUTOFF=INTERVAL '0' SECOND)", dst, src, stamp.Add(-time.Second).Format(time.RFC3339Nano), stamp.Add(time.Second).Format(time.RFC3339Nano), src+"-wait")
	cfg := configuration(t, "tip_wait='100ms'\n")

	// When the worker drains toward the sampled log end without a transaction commit.
	r := loggedWorker(ctx, statement, cfg)

	// Then it fails with the pending end/stable positions, without publishing.
	if r.code != 1 || !strings.Contains(r.detail, "tip wait exceeded: H=1 stable=0 consumed=0") || !strings.Contains(r.detail, `"submitted":0,"confirmed":0`) {
		t.Fatalf("open transaction falsely completed: %d %s", r.code, r.detail)
	}
}
