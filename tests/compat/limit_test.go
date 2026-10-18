//go:build integration

package compat

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/stretchr/testify/require"
)

func executeWithLimit(t *testing.T, statement, config string, limit int) {
	t.Helper()
	execute(t, strings.Replace(statement, "OPTIONS", fmt.Sprintf("LIMIT %d OPTIONS", limit), 1), config)
}

func TestLimitAcrossEndpointDirections(t *testing.T) {
	// Given a retained topic with seven full logical records in one partition.
	a := admin(t)
	source := topic(t, a, 1)
	producer := producer(t, nil)
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
		headers := []k.Header{{Key: "dup", Value: nil}, {Key: "dup", Value: []byte{}}}
		publish(t, producer, &k.Message{TopicPartition: k.TopicPartition{Topic: &source, Partition: 0}, Key: key, Value: value, Headers: headers, Timestamp: stamp})
		expected = append(expected, core.Record{Key: key, Value: value, Headers: []core.Header{{Name: "dup", Value: nil}, {Name: "dup", Value: []byte{}}}, TimestampMS: &ms, TimestampType: "create_time", Source: &core.Provenance{Topic: source, Partition: 0, Offset: int64(i)}})
	}
	config := configuration(t)
	archive := filepath.Join(t.TempDir(), "sample.cbor")

	// When a topic-to-file copy resumes with a different cap.
	statement := fmt.Sprintf("INSERT INTO FILE cbor('%s') SELECT * FROM TOPIC '%s' OPTIONS(GROUP='%s',START=EARLIEST)", archive, source, source+"-archive-limit")
	executeWithLimit(t, statement, config, 3)
	// Then it saves only that record prefix, with unchanged bytes and metadata.
	require.Equal(t, expected[:3], archiveRecords(t, archive))
	executeWithLimit(t, statement, config, 2)
	require.Equal(t, expected[:5], archiveRecords(t, archive))

	t.Run("topic to topic", func(t *testing.T) {
		// Given a destination with a different partition count.
		destination := topic(t, a, 3)
		statement := fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' OPTIONS(GROUP='%s',START=EARLIEST)", destination, source, source+"-topic-limit")
		// When resuming with a fresh cap.
		executeWithLimit(t, statement, config, 2)
		// Then each invocation advances saved progress without copying the rest.
		require.EqualValues(t, 2, topicRecordCount(t, a, destination, 3))
		executeWithLimit(t, statement, config, 1)
		require.EqualValues(t, 3, topicRecordCount(t, a, destination, 3))
	})
	t.Run("file to topic", func(t *testing.T) {
		// Given the five-record sample and a Kafka destination.
		destination := topic(t, a, 3)
		statement := fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM FILE cbor('%s') OPTIONS(GROUP='restore-limit')", destination, archive)
		// When a file checkpoint resumes under a changed cap.
		executeWithLimit(t, statement, config, 2)
		// Then progress limits each run, and a later EOF run adds no duplicates.
		require.EqualValues(t, 2, topicRecordCount(t, a, destination, 3))
		executeWithLimit(t, statement, config, 3)
		require.EqualValues(t, 5, topicRecordCount(t, a, destination, 3))
		executeWithLimit(t, statement, config, 10)
		require.EqualValues(t, 5, topicRecordCount(t, a, destination, 3))
	})
	t.Run("file to file", func(t *testing.T) {
		// Given the same sample and a new archive.
		clone := filepath.Join(t.TempDir(), "clone.cbor")
		statement := fmt.Sprintf("INSERT INTO FILE cbor('%s') SELECT * FROM FILE cbor('%s') OPTIONS(GROUP='clone-limit')", clone, archive)
		// When reaching the cap and then EOF in separate runs.
		executeWithLimit(t, statement, config, 4)
		// Then preserve order and avoid recopying previously saved records.
		require.Equal(t, expected[:4], archiveRecords(t, clone))
		executeWithLimit(t, statement, config, 10)
		require.Equal(t, expected[:5], archiveRecords(t, clone))
	})
}
