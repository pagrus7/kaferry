//go:build integration

package compat

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/app"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/inspect"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func executeWithWorkerStop(t *testing.T, statement, config string, stopAfter int) {
	t.Helper()
	execute(t, statement+fmt.Sprintf(" OPTIONS(WORKER_STOP_AFTER_RECORDS=%d)", stopAfter), config)
}

func assertSavedWorkerStop(t *testing.T, statement, config string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := app.Execute(t.Context(), []string{"status", "-e", statement, "-c", config, "--json"}, nil, &stdout, &stderr)
	require.Zero(t, code, "status: %s %s", stdout.String(), stderr.String())
	var report inspect.StatusReport
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &report))
	require.Equal(t, "stopped", report.Verdict)
	require.Equal(t, "worker record threshold reached", report.Reason)
	for _, partition := range report.Partitions {
		require.NotNil(t, partition.Stop, "partition %d stop", partition.ID)
		require.Equal(t, "worker_record_cap", partition.Stop.Reason)
	}
}

func TestWorkerStopAcrossEndpointDirections(t *testing.T) {
	// Given a retained topic with seven full logical records in one partition.
	a := admin(t)
	source := topic(t, a, 1)
	producer := producer(t)
	identity := brokerSourceIdentity(t, a, source)
	stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	ms := stamp.UnixMilli()
	var expected []core.Record
	for i := range 7 {
		key, value := []byte{0, byte(i)}, []byte{255, byte(i)}
		if i == 0 {
			value = nil
		} else if i == 1 {
			value = []byte{}
		}
		headers := []kgo.RecordHeader{{Key: "dup", Value: nil}, {Key: "dup", Value: []byte{}}}
		publish(t, producer, &kgo.Record{Topic: source, Partition: 0, Key: key, Value: value, Headers: headers, Timestamp: stamp})
		expected = append(expected, core.Record{Key: key, Value: value, Headers: []core.Header{{Name: "dup", Value: nil}, {Name: "dup", Value: []byte{}}}, TimestampMS: &ms, TimestampType: "create_time", Source: &core.Provenance{Cluster: identity.Cluster, TopicID: identity.TopicID, Topic: source, Partition: 0, Offset: int64(i)}})
	}
	config := configuration(t)
	archive := filepath.Join(t.TempDir(), "sample.avro")

	// When a topic-to-file copy resumes with a different cap.
	statement := fmt.Sprintf("INSERT INTO FILE avro('%s') SELECT * FROM TOPIC '%s' WITH (GROUP='%s', START=EARLIEST) ", archive, source, source+"-archive-limit")
	executeWithWorkerStop(t, statement, config, 3)
	// Then it saves only that record prefix, with unchanged bytes and metadata.
	require.Equal(t, expected[:3], archiveRecords(t, archive))
	waitMembers(t, a, source+"-archive-limit", 0, 0, nil)
	assertSavedWorkerStop(t, statement, config)
	executeWithWorkerStop(t, statement, config, 2)
	require.Equal(t, expected[:5], archiveRecords(t, archive))

	t.Run("topic to topic", func(t *testing.T) {
		// Given a destination with a different partition count.
		destination := topic(t, a, 3)
		statement := fmt.Sprintf("INSERT INTO TOPIC '%s' WITH (PARTITIONING = KEY) SELECT * FROM TOPIC '%s' WITH (GROUP='%s', START=EARLIEST) ", destination, source, source+"-topic-limit")
		// When resuming with a fresh cap.
		executeWithWorkerStop(t, statement, config, 2)
		// Then each invocation advances saved progress without copying the rest.
		require.EqualValues(t, 2, topicRecordCount(t, a, destination, 3))
		assertTopicRecords(t, destination, 3, expected[:2])
		waitMembers(t, a, source+"-topic-limit", 0, 0, nil)
		assertSavedWorkerStop(t, statement, config)
		executeWithWorkerStop(t, statement, config, 1)
		require.EqualValues(t, 3, topicRecordCount(t, a, destination, 3))
		assertTopicRecords(t, destination, 3, expected[:3])
	})
	t.Run("file to topic", func(t *testing.T) {
		// Given the five-record sample and a Kafka destination.
		destination := topic(t, a, 3)
		statement := fmt.Sprintf("INSERT INTO TOPIC '%s' WITH (PARTITIONING = KEY) SELECT * FROM FILE avro('%s') WITH (GROUP='restore-limit') ", destination, archive)
		// When a file checkpoint resumes under a changed cap.
		executeWithWorkerStop(t, statement, config, 2)
		// Then progress limits each run, and a later EOF run adds no duplicates.
		require.EqualValues(t, 2, topicRecordCount(t, a, destination, 3))
		assertTopicRecords(t, destination, 3, expected[:2])
		assertSavedWorkerStop(t, statement, config)
		executeWithWorkerStop(t, statement, config, 3)
		require.EqualValues(t, 5, topicRecordCount(t, a, destination, 3))
		assertTopicRecords(t, destination, 3, expected[:5])
		executeWithWorkerStop(t, statement, config, 10)
		require.EqualValues(t, 5, topicRecordCount(t, a, destination, 3))
	})
	t.Run("file to file", func(t *testing.T) {
		// Given the same sample and a new archive.
		clone := filepath.Join(t.TempDir(), "clone.avro")
		statement := fmt.Sprintf("INSERT INTO FILE avro('%s') SELECT * FROM FILE avro('%s') WITH (GROUP='clone-limit') ", clone, archive)
		// When reaching the cap and then EOF in separate runs.
		executeWithWorkerStop(t, statement, config, 4)
		// Then preserve order and avoid recopying previously saved records.
		require.Equal(t, expected[:4], archiveRecords(t, clone))
		executeWithWorkerStop(t, statement, config, 10)
		require.Equal(t, expected[:5], archiveRecords(t, clone))
	})
}
