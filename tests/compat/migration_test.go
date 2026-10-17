//go:build integration && migration

package compat

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pagrus7/kaferry/internal/commitmeta"
)

// Supply binaries built from the previous and candidate revisions.
func TestPreviousBinaryProgressAndArchiveCompatibility(t *testing.T) {
	previous := os.Getenv("KAFERRY_PREVIOUS_BINARY")
	if previous == "" {
		t.Fatal("set KAFERRY_PREVIOUS_BINARY for migration checks")
	}
	candidate := testBinary(t)
	previousFormat := ""
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
			if progress.Metadata == nil {
				t.Fatal("missing metadata")
			}
			format := strings.SplitN(*progress.Metadata, ";", 2)[0]
			want := 0
			if direction.first == previous {
				previousFormat = format
				if _, err := commitmeta.NewCodec().Decode(*progress.Metadata); err != nil {
					want = 2
				}
			} else if format != previousFormat {
				want = 2
			}
			// When another format resumes this group, then it rejects the save before changing output.
			runMigrationExpect(t, direction.second, sql, config, want)
			if got := archiveRecords(t, archive); !reflect.DeepEqual(got, before) {
				t.Fatal("Kafka progress/archive resume changed records", got)
			}
			cloneSQL := fmt.Sprintf("INSERT INTO FILE cbor('%s') SELECT * FROM FILE cbor('%s') OPTIONS(GROUP='file-migration')", clone, archive)
			runMigrationBinary(t, direction.first, cloneSQL, config)
			runMigrationExpect(t, direction.second, cloneSQL, config, want)
			// Then matching formats resume, incompatible formats fail closed, and neither duplicates output.
			if got := archiveRecords(t, clone); !reflect.DeepEqual(got, before) {
				t.Fatal("file checkpoint/archive compatibility failed", got)
			}
			// When using a fresh group/output, then the other binary can still read the unchanged archive format.
			freshClone := filepath.Join(t.TempDir(), "read.cbor")
			freshSQL := fmt.Sprintf("INSERT INTO FILE cbor('%s') SELECT * FROM FILE cbor('%s') OPTIONS(GROUP='archive-read')", freshClone, archive)
			runMigrationBinary(t, direction.second, freshSQL, config)
			if got := archiveRecords(t, freshClone); !reflect.DeepEqual(got, before) {
				t.Fatal("archive format changed", got)
			}
		})
	}
}
func runMigrationBinary(t *testing.T, binary, statement, config string) {
	t.Helper()
	runMigrationExpect(t, binary, statement, config, 0)
}
func runMigrationExpect(t *testing.T, binary, statement, config string, want int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	process := startCLI(t, ctx, binary, "", "run", "-e", statement, "-c", config)
	err := process.wait()
	code := 0
	if err != nil {
		if e, ok := err.(*exec.ExitError); ok {
			code = e.ExitCode()
		} else {
			t.Fatal(err)
		}
	}
	if code != want {
		t.Fatalf("migration exit=%d want=%d: %s", code, want, process.logs())
	}
}
