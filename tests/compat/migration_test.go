//go:build integration && migration

package compat

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Supply binaries built from the previous and candidate revisions.
func TestPreviousBinaryProgressAndArchiveCompatibility(t *testing.T) {
	previous := os.Getenv("KAFERRY_PREVIOUS_BINARY")
	if previous == "" {
		t.Fatal("set KAFERRY_PREVIOUS_BINARY for migration checks")
	}
	candidate := testBinary(t)
	for _, direction := range []struct{ name, first, second string }{
		{"previous to candidate", previous, candidate}, {"candidate to previous", candidate, previous},
	} {
		t.Run(direction.name, func(t *testing.T) {
			// Given an archive and committed Kafka progress created by one binary.
			admin := admin(t)
			source := topic(t, admin, 1)
			producer := producer(t, nil)
			stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
			publish(t, producer, &k.Message{TopicPartition: k.TopicPartition{Topic: &source, Partition: 0}, Key: []byte{0, 255}, Value: []byte{}, Headers: []k.Header{{Key: "dup", Value: nil}, {Key: "dup", Value: []byte{}}}, Timestamp: stamp})
			archive := filepath.Join(t.TempDir(), "archive.cbor")
			clone := filepath.Join(t.TempDir(), "clone.cbor")
			config := configuration(t)
			sql := fmt.Sprintf("INSERT INTO FILE cbor('%s') SELECT * FROM TOPIC '%s' WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' OPTIONS(GROUP='%s', CUTOFF=INTERVAL '0' SECOND)", archive, source, stamp.Add(time.Second).Format(time.RFC3339Nano), source+"-migration")
			runMigrationBinary(t, direction.first, sql, config)
			before := archiveRecords(t, archive)
			if len(before) != 1 || !bytes.Equal(before[0].Key, []byte{0, 255}) || before[0].Value == nil {
				t.Fatal("invalid archive fixture", before)
			}
			progress := committedPartitions(t, admin, source+"-migration", source, 1)[0]
			if progress.Offset != 1 {
				t.Fatal("first binary did not save progress", progress)
			}
			// When the other binary resumes the same query and reads that archive.
			runMigrationBinary(t, direction.second, sql, config)
			if got := archiveRecords(t, archive); !reflect.DeepEqual(got, before) {
				t.Fatal("Kafka progress/archive resume changed records", got)
			}
			cloneSQL := fmt.Sprintf("INSERT INTO FILE cbor('%s') SELECT * FROM FILE cbor('%s') OPTIONS(GROUP='file-migration')", clone, archive)
			runMigrationBinary(t, direction.first, cloneSQL, config)
			runMigrationBinary(t, direction.second, cloneSQL, config)
			// Then both Kafka metadata and file checkpoints resume without duplicating records.
			if got := archiveRecords(t, clone); !reflect.DeepEqual(got, before) {
				t.Fatal("file checkpoint/archive compatibility failed", got)
			}
		})
	}
}
func runMigrationBinary(t *testing.T, binary, statement, config string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	process := startCLI(t, ctx, binary, "", "run", "-e", statement, "-c", config)
	if err := process.wait(); err != nil {
		t.Fatalf("migration binary failed: %v %s", err, process.logs())
	}
}
