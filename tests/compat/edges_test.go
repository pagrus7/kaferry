//go:build integration

package compat

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"testing"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pagrus7/kaferry/internal/app"
	"github.com/pagrus7/kaferry/internal/commitmeta"
	"github.com/stretchr/testify/require"
)

func TestTimeBoundariesOutOfOrderAndTriggerReplay(t *testing.T) {
	// Given
	a := admin(t)
	src := topic(t, a, 1)
	p := producer(t, nil)
	lo := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	hi := lo.Add(time.Second)
	for i, stamp := range []time.Time{lo.Add(-time.Millisecond), lo, hi.Add(-time.Millisecond), lo.Add(time.Millisecond), hi, hi.Add(time.Millisecond), hi.Add(2 * time.Millisecond), lo.Add(time.Millisecond)} {
		publish(t, p, &k.Message{TopicPartition: k.TopicPartition{Topic: &src, Partition: 0}, Value: []byte(fmt.Sprint(i)), Timestamp: stamp})
	}
	cfg := configuration(t)
	for _, test := range []struct {
		name, filter string
		want         []string
	}{{"halfopen", fmt.Sprintf("RECORD_TIMESTAMP >= TIMESTAMP '%s' AND RECORD_TIMESTAMP < TIMESTAMP '%s'", lo.Format(time.RFC3339Nano), hi.Format(time.RFC3339Nano)), []string{"1", "2", "3"}}, {"between", fmt.Sprintf("RECORD_TIMESTAMP BETWEEN TIMESTAMP '%s' AND TIMESTAMP '%s'", lo.Format(time.RFC3339Nano), hi.Format(time.RFC3339Nano)), []string{"1", "2", "3", "4"}}} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "out.cbor")
			statement := fmt.Sprintf("INSERT INTO FILE cbor('%s') SELECT * FROM TOPIC '%s' WHERE %s OPTIONS(GROUP='%s-%s',CUTOFF=INTERVAL '0' SECOND)", path, src, test.filter, src, test.name)

			// When
			execute(t, statement, cfg)
			execute(t, statement, cfg)
			records := archiveRecords(t, path)

			// Then
			require.Len(t, records, len(test.want), "records=%d want=%d", len(records), len(test.want))
			for i, r := range records {
				require.Equal(t, test.want[i], string(r.Value), "value=%s want=%s", r.Value, test.want[i])
			}
		})
	}
}

func TestForeignProgressAndSelfCopyFailBeforeJoining(t *testing.T) {
	// Given
	a := admin(t)
	src, dst := topic(t, a, 1), topic(t, a, 1)
	group := src + "-foreign"
	metadata := "another application's offsets"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result, err := a.AlterConsumerGroupOffsets(ctx, []k.ConsumerGroupTopicPartitions{{Group: group, Partitions: []k.TopicPartition{{Topic: &src, Partition: 0, Offset: 0, Metadata: &metadata}}}})
	if err != nil || len(result.ConsumerGroupsTopicPartitions) != 1 {
		t.Fatal("fixture offsets", err)
	}
	statement := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s') INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' OPTIONS(GROUP='%s',START=EARLIEST)", broker(t), dst, src, group)
	var out bytes.Buffer

	// When
	executeCode := app.Execute(ctx, []string{"run", "-e", statement}, nil, io.Discard, &out)

	// Then
	require.Equal(t, 2, executeCode, "foreign progress exit=%d: %s", executeCode, out.String())
	description, err := a.DescribeConsumerGroups(ctx, []string{group})
	require.NoError(t, err)
	require.Len(t, description.ConsumerGroupDescriptions[0].Members, 0, "foreign group joined before validation")
	statement = fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s') INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' OPTIONS(GROUP='%s-self',START=EARLIEST)", broker(t), src, src, src)
	if code := app.Execute(ctx, []string{"run", "-e", statement}, nil, io.Discard, &out); code != 2 {
		t.Fatal("Kafka self-copy permitted")
	}
}

func TestExpiredAndAboveEndCommits(t *testing.T) {
	// Given
	a := admin(t)
	src := topic(t, a, 1)
	p := producer(t, nil)
	stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	for i := range 6 {
		publish(t, p, &k.Message{TopicPartition: k.TopicPartition{Topic: &src, Partition: 0}, Value: []byte(fmt.Sprint(i)), Timestamp: stamp})
	}
	cfg := configuration(t)
	path := filepath.Join(t.TempDir(), "out.cbor")
	group := src + "-expired"
	statement := fmt.Sprintf("INSERT INTO FILE cbor('%s') SELECT * FROM TOPIC '%s' WHERE RECORD_TIMESTAMP >= TIMESTAMP '%s' AND RECORD_TIMESTAMP < TIMESTAMP '%s' OPTIONS(GROUP='%s',CUTOFF=INTERVAL '0' SECOND)", path, src, stamp.Add(-time.Millisecond).Format(time.RFC3339Nano), stamp.Add(time.Second).Format(time.RFC3339Nano), group)

	// When
	execute(t, statement, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	positions, err := a.ListConsumerGroupOffsets(ctx, []k.ConsumerGroupTopicPartitions{{Group: group, Partitions: []k.TopicPartition{{Topic: &src, Partition: 0}}}})

	// Then
	require.NoError(t, err)
	part := positions.ConsumerGroupsTopicPartitions[0].Partitions[0]
	if _, err := a.DeleteRecords(ctx, []k.TopicPartition{{Topic: &src, Partition: 0, Offset: 3}}); err != nil {
		t.Fatal(err)
	}
	// Rewinding an offset also clears the saved stop that referred to its old position.
	require.NotNil(t, part.Metadata, "missing metadata")
	saved, err := commitmeta.NewCodec().Decode(*part.Metadata)
	require.NoError(t, err)
	saved.Stop = nil
	text, err := commitmeta.NewCodec().Encode(saved)
	require.NoError(t, err)
	part.Metadata = &text
	part.Offset = 1
	if _, err := a.AlterConsumerGroupOffsets(ctx, []k.ConsumerGroupTopicPartitions{{Group: group, Partitions: []k.TopicPartition{part}}}); err != nil {
		t.Fatal(err)
	}
	execute(t, statement, cfg)
	if records := archiveRecords(t, path); len(records) != 9 {
		t.Fatal("expired commit did not replay from log start", len(records))
	}
	part.Offset = 999999
	if _, err := a.AlterConsumerGroupOffsets(ctx, []k.ConsumerGroupTopicPartitions{{Group: group, Partitions: []k.TopicPartition{part}}}); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if code := app.Execute(ctx, []string{"run", "-e", statement, "-c", cfg}, nil, io.Discard, &out); code != 2 {
		t.Fatalf("above-end offset exit=%d: %s", code, out.String())
	}
}
