//go:build integration

package compat

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/require"
)

func TestCrossClusterOpaqueSchemaBytes(t *testing.T) {
	// Given
	dr := os.Getenv("KAFKA_TEST_DEST_BROKERS")
	if dr == "" {
		t.Skip("set KAFKA_TEST_DEST_BROKERS for cross-cluster verification")
	}
	a := admin(t)
	name := topic(t, a, 2)
	destination, err := k.NewAdminClient(&k.ConfigMap{"bootstrap.servers": dr})
	require.NoError(t, err)
	t.Cleanup(func() { closeAdmin(t, destination) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sourceID, err := a.ClusterID(ctx)
	require.NoError(t, err)
	destinationID, err := destination.ClusterID(ctx)
	if err != nil || sourceID == destinationID {
		t.Fatal("cross-cluster fixture requires different cluster IDs", err)
	}
	created, err := destination.CreateTopics(ctx, []k.TopicSpecification{{Topic: name, NumPartitions: 3, ReplicationFactor: 1, Config: map[string]string{"message.timestamp.before.max.ms": "9223372036854775807"}}})
	if err != nil || created[0].Error.Code() != k.ErrNoError {
		t.Fatal("destination creation", err, created)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = destination.DeleteTopics(ctx, []string{name})
	})
	waitTopicReady(t, destination, name, 3)
	key := []byte{0, 0, 0, 0, 123, 255, 0}
	value := []byte{0, 0, 0, 1, 200, 255, 0, 17}
	headers := []k.Header{{Key: "schema", Value: []byte{0, 123}}, {Key: "null", Value: nil}}
	stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	p := producer(t, nil)
	for i := range 12 {
		publish(t, p, &k.Message{TopicPartition: k.TopicPartition{Topic: &name, Partition: int32(i % 2)}, Key: key, Value: value, Headers: headers, Timestamp: stamp})
	}
	statement := fmt.Sprintf("WITH CONNECTION source AS ('bootstrap.servers'='%s'), CONNECTION destination AS ('bootstrap.servers'='%s') INSERT INTO TOPIC '%s@destination' SELECT * FROM TOPIC '%s@source' WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' OPTIONS(GROUP='%s-cross',CUTOFF=INTERVAL '0' SECOND)", broker(t), dr, name, name, stamp.Add(time.Second).Format(time.RFC3339Nano), name)

	// When
	execute(t, statement, configuration(t))
	c, err := k.NewConsumer(&k.ConfigMap{"bootstrap.servers": dr, "group.id": name + "-reader", "enable.auto.commit": false, "isolation.level": "read_committed"})

	// Then
	require.NoError(t, err)
	defer c.Close()
	var parts []k.TopicPartition
	for i := range 3 {
		parts = append(parts, k.TopicPartition{Topic: &name, Partition: int32(i), Offset: 0})
	}
	require.NoError(t, c.Assign(parts))
	for range 12 {
		r := read(t, c)
		if !bytes.Equal(r.Key, key) || !bytes.Equal(r.Value, value) || !reflect.DeepEqual(r.Headers, headers) || !r.Timestamp.Equal(stamp) {
			t.Fatal("cross-cluster copy changed opaque schema bytes")
		}
	}
}
