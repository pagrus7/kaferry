//go:build integration

package compat

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pagrus7/kaferry/internal/app"
)

func TestNoMatchLookupOpenTransactionAndTrailingMarker(t *testing.T) {
	// Given
	for _, abort := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit", true: "abort"}[abort], func(t *testing.T) {
			a := admin(t)
			src := topic(t, a, 1)
			p := producer(t, k.ConfigMap{"transactional.id": src + "-open"})
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			if err := p.InitTransactions(ctx); err != nil {
				t.Fatal(err)
			}
			if err := p.BeginTransaction(); err != nil {
				t.Fatal(err)
			}
			stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
			publish(t, p, &k.Message{TopicPartition: k.TopicPartition{Topic: &src, Partition: 0}, Value: []byte("transactional"), Timestamp: stamp})
			path := filepath.Join(t.TempDir(), "out.cbor")
			group := src + "-copy"
			cfg := configuration(t)
			statement := fmt.Sprintf("INSERT INTO FILE cbor('%s') SELECT * FROM TOPIC '%s' WHERE RECORD_TIMESTAMP >= TIMESTAMP '%s' AND RECORD_TIMESTAMP < TIMESTAMP '%s' OPTIONS(GROUP='%s',CUTOFF=INTERVAL '0' SECOND)", path, src, stamp.Add(-time.Second).Format(time.RFC3339Nano), stamp.Add(time.Second).Format(time.RFC3339Nano), group)
			result := make(chan workerResult, 1)
			go func() {
				var output bytes.Buffer

				// When
				code := app.Execute(ctx, []string{"run", "-e", statement, "-c", cfg}, nil, &output)
				result <- workerResult{code, output.String()}
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
			if abort {
				if err := p.AbortTransaction(ctx); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := p.CommitTransaction(ctx); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case r := <-result:
				finished = true
				if r.code != 0 {
					t.Fatalf("transaction tail exit=%d: %s", r.code, r.detail)
				}
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
