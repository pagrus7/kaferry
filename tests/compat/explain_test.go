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

	"github.com/pagrus7/kaferry/internal/inspect"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestExplainBinaryResumesWithoutChangingKafkaOrCreatingFiles(t *testing.T) {
	// Given a completed copy and broker observations of its saved progress.
	a := admin(t)
	source, destination := topic(t, a, 2), topic(t, a, 3)
	producer := producer(t)
	stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	for partition := range int32(2) {
		publish(t, producer, &kgo.Record{Topic: source, Partition: partition, Timestamp: stamp, Value: []byte("record")})
	}
	group := source + "-explain"
	query := fmt.Sprintf("INSERT INTO TOPIC '%s' WITH (PARTITIONING = KEY) SELECT * FROM TOPIC '%s' WITH (GROUP='%s', MAX_STALENESS=INTERVAL '0' SECOND) WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' ", destination, source, group, stamp.Add(time.Minute).Format(time.RFC3339Nano))
	config := configuration(t)
	execute(t, query, config)
	before, err := a.FetchOffsets(t.Context(), group)
	require.NoError(t, err)
	require.NoError(t, before.Error())
	require.Len(t, before[source], 2)
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
		if part.ID != int32(n) || !part.Saved || part.Start != before[source][int32(n)].At {
			t.Fatal("preview differs from committed progress", report, before)
		}
	}
	after, err := a.FetchOffsets(t.Context(), group)
	if err != nil || after.Error() != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("inspection changed saved progress", before, after, err)
	}

	// Given a fresh query into a file, when explaining, then no output or lock is created.
	directory := t.TempDir()
	archive := filepath.Join(directory, "out.avro")
	query = fmt.Sprintf("INSERT INTO FILE avro('%s') SELECT * FROM TOPIC '%s' WITH (GROUP=ONE_OFF, START=EARLIEST) ", archive, source)
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
