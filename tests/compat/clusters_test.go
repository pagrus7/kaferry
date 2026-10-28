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

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestCrossClusterOpaqueSchemaBytes(t *testing.T) {
	// Given
	dr := os.Getenv("KAFKA_TEST_DEST_BROKERS")
	if dr == "" {
		t.Skip("set KAFKA_TEST_DEST_BROKERS for cross-cluster verification")
	}
	a := admin(t)
	name := topic(t, a, 2)
	destination := kadm.NewClient(fixtureClient(t, dr))
	ensureProgressTopic(t, destination)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sourceMetadata, err := a.BrokerMetadata(ctx)
	require.NoError(t, err)
	destinationMetadata, err := destination.BrokerMetadata(ctx)
	if err != nil || sourceMetadata.Cluster == destinationMetadata.Cluster {
		t.Fatal("cross-cluster fixture requires different cluster IDs", err)
	}
	created, err := destination.CreateTopic(ctx, 3, 1, map[string]*string{"message.timestamp.before.max.ms": kadm.StringPtr("9223372036854775807")}, name)
	if err != nil {
		t.Fatal("destination creation", err, created)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = destination.DeleteTopics(ctx, name)
	})
	waitTopicReady(t, destination, name, 3)
	key := []byte{0, 0, 0, 0, 123, 255, 0}
	value := []byte{0, 0, 0, 1, 200, 255, 0, 17}
	headers := []kgo.RecordHeader{{Key: "schema", Value: []byte{0, 123}}, {Key: "null", Value: nil}}
	stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	p := producer(t)
	for i := range 12 {
		publish(t, p, &kgo.Record{Topic: name, Partition: int32(i % 2), Key: key, Value: value, Headers: headers, Timestamp: stamp})
	}
	statement := fmt.Sprintf("WITH CONNECTION source AS ('bootstrap.servers'='%s'), CONNECTION destination AS ('bootstrap.servers'='%s') INSERT INTO TOPIC '%s@destination' WITH (PARTITIONING = KEY) SELECT * FROM TOPIC '%s@source' WITH (GROUP='%s-cross', MAX_STALENESS=INTERVAL '0' SECOND) WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' ", broker(t), dr, name, name, name, stamp.Add(time.Second).Format(time.RFC3339Nano))

	// When
	execute(t, statement, configuration(t))
	c := fixtureClient(t, dr, kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	// Then a separate client observes the unchanged destination records.
	assign(t, c, name, 3)

	for range 12 {
		r := read(t, c)
		if !bytes.Equal(r.Key, key) || !bytes.Equal(r.Value, value) || !reflect.DeepEqual(r.Headers, headers) || !r.Timestamp.Equal(stamp) {
			t.Fatal("cross-cluster copy changed opaque schema bytes")
		}
	}
}
