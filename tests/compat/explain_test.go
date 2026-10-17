//go:build integration

package compat

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pagrus7/kaferry/internal/inspect"
)

func TestExplainBinaryResumesWithoutChangingKafkaOrCreatingFiles(t *testing.T) {
	// Given a completed copy and native-client observations of its saved progress.
	a := admin(t)
	source, destination := topic(t, a, 2), topic(t, a, 3)
	producer := producer(t, nil)
	stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	for partition := range int32(2) {
		publish(t, producer, &k.Message{TopicPartition: k.TopicPartition{Topic: &source, Partition: partition}, Timestamp: stamp, Value: []byte("record")})
	}
	group := source + "-explain"
	query := fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' OPTIONS(GROUP='%s',CUTOFF=INTERVAL '0' SECOND)", destination, source, stamp.Add(time.Minute).Format(time.RFC3339Nano), group)
	config := configuration(t)
	execute(t, query, config)
	reader := consumer(t, group)
	parts := []k.TopicPartition{{Topic: &source, Partition: 0}, {Topic: &source, Partition: 1}}
	before, err := reader.Committed(parts, 10000)
	if err != nil {
		t.Fatal(err)
	}
	binary := testBinary(t)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, "explain", "-e", query, "-c", config, "--json")
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	// When the actual executable explains a resume against the real broker.
	if err := command.Run(); err != nil {
		t.Fatal(err, stderr.String())
	}
	// Then it reports the saved offsets and leaves positions and metadata unchanged.
	var report inspect.Report
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil || len(report.Partitions) != 2 {
		t.Fatal(err, stdout.String())
	}
	for n, part := range report.Partitions {
		if part.ID != int32(n) || !part.Saved || part.Start != int64(before[n].Offset) {
			t.Fatal("preview differs from committed progress", report, before)
		}
	}
	after, err := reader.Committed(parts, 10000)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("inspection changed saved progress", before, after, err)
	}

	// Given a fresh query into a file, when explaining, then no output or lock is created.
	directory := t.TempDir()
	archive := filepath.Join(directory, "out.cbor")
	query = fmt.Sprintf("INSERT INTO FILE cbor('%s') SELECT * FROM TOPIC '%s' OPTIONS(GROUP=ONE_OFF,START=EARLIEST)", archive, source)
	command = exec.CommandContext(ctx, binary, "explain", "-e", query, "-c", config)
	output, err := command.CombinedOutput()
	if err != nil || !bytes.Contains(output, []byte("ONE_OFF")) {
		t.Fatal(err, string(output))
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 0 {
		t.Fatal("preview created output or temporary files", entries, err)
	}
}
