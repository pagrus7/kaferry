//go:build integration

package compat

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/app"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func broker(t *testing.T) string {
	t.Helper()
	value := os.Getenv("KAFKA_TEST_BROKERS")
	if value == "" {
		t.Skip("set KAFKA_TEST_BROKERS to a disposable broker")
	}
	return value
}

func TestTopicToTopicSlice(t *testing.T) {
	// Given
	a := admin(t)
	source := topic(t, a, 2)
	destination := topic(t, a, 3)
	p := producer(t)
	stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	for i := range 40 {
		publish(t, p, &kgo.Record{Topic: source, Partition: int32(i % 2), Key: []byte(fmt.Sprintf("key-%d", i%7)), Value: []byte(fmt.Sprintf("value-%d", i)), Headers: []kgo.RecordHeader{{Key: "dup", Value: nil}, {Key: "dup", Value: []byte{}}}, Timestamp: stamp})
	}
	statement := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s') INSERT INTO TOPIC '%s' WITH (PARTITIONING = KEY) SELECT * FROM TOPIC '%s' WITH (GROUP='%s', MAX_STALENESS=INTERVAL '0' SECOND) WHERE RECORD_TIMESTAMP >= TIMESTAMP '%s' AND RECORD_TIMESTAMP < TIMESTAMP '%s' ", broker(t), destination, source, source+"-copy", stamp.Add(-time.Second).Format(time.RFC3339Nano), stamp.Add(time.Second).Format(time.RFC3339Nano))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var output bytes.Buffer

	// When
	executeCode := app.Execute(ctx, []string{"run", "--sql", statement}, nil, io.Discard, &output)

	// Then
	require.Zero(t, executeCode, "copy exit=%d: %s", executeCode, output.String())
	c := consumer(t)
	assign(t, c, destination, 3)
	seen := make(map[string]bool)
	for range 40 {
		record := read(t, c)
		if seen[string(record.Value)] {
			t.Fatal("unexpected duplicate on first copy")
		}
		seen[string(record.Value)] = true
		if !record.Timestamp.Equal(stamp) || !reflect.DeepEqual(record.Headers, []kgo.RecordHeader{{Key: "dup", Value: nil}, {Key: "dup", Value: []byte{}}}) {
			t.Fatal("copy changed timestamp or headers")
		}
	}
	output.Reset()
	if code := app.Execute(ctx, []string{"run", "--sql", statement}, nil, io.Discard, &output); code != 0 {
		t.Fatalf("resume exit=%d: %s", code, output.String())
	}
	require.Equal(t, int64(40), topicRecordCount(t, a, destination, 3), "completed resume published extra records")
}

func TestRecordFidelityAndTopicID(t *testing.T) {
	// Given
	a := admin(t)
	name := topic(t, a, 1)
	p := producer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	description, err := a.ListTopics(ctx, name)
	if err != nil || len(description) != 1 || description[name].Err != nil {
		t.Fatalf("DescribeTopics: %v", err)
	}
	t.Logf("topic ID=%s", description[name].ID.String())
	c := consumer(t)
	assign(t, c, name, 1)
	for _, data := range []struct{ key, value []byte }{{nil, nil}, {[]byte{}, []byte{}}, {[]byte{0, 255}, []byte{1, 0, 255}}} {
		m := &kgo.Record{Topic: name, Partition: 0, Key: data.key, Value: data.value, Timestamp: time.UnixMilli(1791000000123), Headers: []kgo.RecordHeader{{Key: "x", Value: nil}, {Key: "x", Value: []byte{}}, {Key: "x", Value: []byte{0, 255}}}}
		publish(t, p, m)

		// When
		got := read(t, c)

		// Then
		require.Equal(t, m.Key, got.Key, "key bytes and nullness")
		require.Equal(t, m.Value, got.Value, "value bytes and nullness")
		require.Equal(t, m.Headers, got.Headers, "headers and nullness")
		require.True(t, got.Timestamp.Equal(m.Timestamp), "timestamp: got %v, want %v", got.Timestamp, m.Timestamp)
	}
}

func TestTransactionVisibilityAndCommittedProgress(t *testing.T) {
	for _, abort := range []bool{false, true} {
		t.Run(fmt.Sprintf("abort=%v", abort), func(t *testing.T) {
			// Given a record in an unresolved transaction.
			a := admin(t)
			name := topic(t, a, 1)
			p := producer(t, kgo.TransactionalID(name+"-tx"))
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			require.NoError(t, p.BeginTransaction())
			publish(t, p, &kgo.Record{Topic: name, Value: []byte("txn"), Timestamp: time.UnixMilli(1791000000000)})
			high, err := a.ListEndOffsets(ctx, name)
			require.NoError(t, err)
			require.NoError(t, high.Error())
			stable, err := a.ListCommittedOffsets(ctx, name)
			require.NoError(t, err)
			require.NoError(t, stable.Error())
			require.EqualValues(t, 1, high[name][0].Offset, "open transaction is in the log")
			require.Zero(t, stable[name][0].Offset, "open transaction is not readable")
			// When resolving the transaction and waiting for its control marker.
			require.NoError(t, p.EndTransaction(ctx, kgo.TransactionEndTry(!abort)))
			waitEnd(t, a, name, 2)
			stable, err = a.ListCommittedOffsets(ctx, name)
			require.NoError(t, err)
			require.NoError(t, stable.Error())
			require.EqualValues(t, 2, stable[name][0].Offset, "readable end includes the control marker")
			c := consumer(t)
			assign(t, c, name, 1)
			// Then read-committed consumers see only committed data.
			publish(t, producer(t), &kgo.Record{Topic: name, Value: []byte("after transaction")})
			if !abort {
				require.Equal(t, "txn", string(read(t, c).Value))
			}
			sentinel := read(t, c)
			require.Equal(t, "after transaction", string(sentinel.Value), "consumer must advance past transactional data and control marker")
			require.EqualValues(t, 2, sentinel.Offset)
			const end = int64(3)

			// Then the log-end offset and opaque commit metadata round-trip through the broker.
			const metadata = "kaferry-probe;yes"
			saved, err := a.CommitOffsets(ctx, name+"-g", kadm.Offsets{name: {0: {Topic: name, At: end, Metadata: metadata, LeaderEpoch: -1}}})
			require.NoError(t, err)
			require.NoError(t, saved.Error())
			fetched, err := a.FetchOffsets(ctx, name+"-g")
			require.NoError(t, err)
			require.NoError(t, fetched.Error())
			require.Equal(t, end, fetched[name][0].At)
			require.Equal(t, metadata, fetched[name][0].Metadata)
		})
	}
}

func TestTimestampLookupOutOfOrder(t *testing.T) {
	// Given
	a := admin(t)
	name := topic(t, a, 1)
	p := producer(t)
	base := int64(1791000000000)
	for _, delta := range []int64{2000, 0, 1000, 2000} {
		publish(t, p, &kgo.Record{Topic: name, Partition: 0, Value: []byte("x"), Timestamp: time.UnixMilli(base + delta)})
	}
	for _, delta := range []int64{0, 1000, 2000, 3000} {

		// When
		positions, err := a.ListOffsetsAfterMilli(t.Context(), base+delta, name)

		// Then
		require.NoError(t, err)
		require.NoError(t, positions.Error())
		position, ok := positions.Lookup(name, 0)
		require.True(t, ok, "timestamp lookup partition")
		t.Logf("timestamp delta=%d lookup=%v", delta, positions)
		if delta <= 2000 && position.Offset != 0 {
			t.Fatal("lookup skipped earlier qualifying record")
		}
	}
}

func TestMixedTimestampBatchHasNoRecordAttribution(t *testing.T) {
	// Given
	a := admin(t)
	name := topic(t, a, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	alterTopicConfig(t, a, name, map[string]string{"message.timestamp.after.max.ms": "1000"})
	p := producer(t, kgo.ProducerLinger(500*time.Millisecond))
	reports := make(chan kgo.ProduceResult, 2)
	for i, when := range []time.Time{time.Now(), time.Now().Add(time.Hour)} {
		// When valid and invalid timestamps share one producer batch.
		p.Produce(ctx, &kgo.Record{Topic: name, Value: []byte(fmt.Sprint(i)), Timestamp: when}, deliveryReporter(ctx, reports))
	}

	for range 2 {
		select {
		case event := <-reports:
			require.ErrorIs(t, event.Err, kerr.InvalidTimestamp, "broker must reject the mixed timestamp batch")
		case <-time.After(15 * time.Second):
			t.Fatal("delivery timed out")
		}
	}
}
