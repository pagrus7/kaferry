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

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
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
			p := producer(t, kgo.TransactionalID(src+"-open"))
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			require.NoError(t, p.BeginTransaction())
			stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
			publish(t, p, &kgo.Record{Topic: src, Partition: 0, Value: []byte("transactional"), Timestamp: stamp})
			path := filepath.Join(t.TempDir(), "out.avro")
			group := src + "-copy"
			cfg := configuration(t)
			statement := fmt.Sprintf("INSERT INTO FILE avro('%s') SELECT * FROM TOPIC '%s' WITH (GROUP='%s', MAX_STALENESS=INTERVAL '0' SECOND) WHERE RECORD_TIMESTAMP >= TIMESTAMP '%s' AND RECORD_TIMESTAMP < TIMESTAMP '%s' ", path, src, group, stamp.Add(-time.Second).Format(time.RFC3339Nano), stamp.Add(time.Second).Format(time.RFC3339Nano))
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
				positions, err := a.FetchOffsets(ctx, group)
				if err == nil && positions.Error() == nil && len(positions[src]) == 1 && positions[src][0].At == 0 {
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
				require.NoError(t, p.EndTransaction(ctx, kgo.TryAbort))
			} else {
				require.NoError(t, p.EndTransaction(ctx, kgo.TryCommit))
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
	p := producer(t, kgo.TransactionalID(src+"-open"))
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	require.NoError(t, p.BeginTransaction())
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = p.EndTransaction(cleanup, kgo.TryAbort)
	}()
	stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	publish(t, p, &kgo.Record{Topic: src, Partition: 0, Value: []byte("transactional"), Timestamp: stamp})
	statement := fmt.Sprintf("INSERT INTO TOPIC '%s' WITH (PARTITIONING = KEY) SELECT * FROM TOPIC '%s' WITH (GROUP='%s', MAX_STALENESS=INTERVAL '0' SECOND) WHERE RECORD_TIMESTAMP >= TIMESTAMP '%s' AND RECORD_TIMESTAMP < TIMESTAMP '%s' ", dst, src, src+"-wait", stamp.Add(-time.Second).Format(time.RFC3339Nano), stamp.Add(time.Second).Format(time.RFC3339Nano))
	cfg := configuration(t, "tip_wait='100ms'\n")

	// When the worker drains toward the sampled log end without a transaction commit.
	r := loggedWorker(ctx, statement, cfg)

	// Then it fails with the pending end/stable positions, without publishing.
	if r.code != 1 || !strings.Contains(r.detail, "tip wait exceeded: H=1 stable=0 consumed=0") || !strings.Contains(r.detail, `"submitted":0,"confirmed":0`) {
		t.Fatalf("open transaction falsely completed: %d %s", r.code, r.detail)
	}
}
