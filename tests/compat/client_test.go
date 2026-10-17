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

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pagrus7/kaferry/internal/app"
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
	p := producer(t, nil)
	stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	for i := range 40 {
		publish(t, p, &k.Message{TopicPartition: k.TopicPartition{Topic: &source, Partition: int32(i % 2)}, Key: []byte(fmt.Sprintf("key-%d", i%7)), Value: []byte(fmt.Sprintf("value-%d", i)), Headers: []k.Header{{Key: "dup", Value: nil}, {Key: "dup", Value: []byte{}}}, Timestamp: stamp})
	}
	statement := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s') INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' WHERE RECORD_TIMESTAMP >= TIMESTAMP '%s' AND RECORD_TIMESTAMP < TIMESTAMP '%s' OPTIONS(GROUP='%s',CUTOFF=INTERVAL '0' SECOND)", broker(t), destination, source, stamp.Add(-time.Second).Format(time.RFC3339Nano), stamp.Add(time.Second).Format(time.RFC3339Nano), source+"-copy")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var output bytes.Buffer

	// When
	executeCode := app.Execute(ctx, []string{"run", "--sql", statement}, nil, io.Discard, &output)

	// Then
	if executeCode != 0 {
		t.Fatalf("copy exit=%d: %s", executeCode, output.String())
	}
	c := consumer(t, destination+"-reader")
	parts := make([]k.TopicPartition, 3)
	for i := range parts {
		parts[i] = k.TopicPartition{Topic: &destination, Partition: int32(i), Offset: 0}
	}
	if err := c.Assign(parts); err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]bool)
	for range 40 {
		record := read(t, c)
		if seen[string(record.Value)] {
			t.Fatal("unexpected duplicate on first copy")
		}
		seen[string(record.Value)] = true
		if !record.Timestamp.Equal(stamp) || !reflect.DeepEqual(record.Headers, []k.Header{{Key: "dup", Value: nil}, {Key: "dup", Value: []byte{}}}) {
			t.Fatal("copy changed timestamp or headers")
		}
	}
	output.Reset()
	if code := app.Execute(ctx, []string{"run", "--sql", statement}, nil, io.Discard, &output); code != 0 {
		t.Fatalf("resume exit=%d: %s", code, output.String())
	}
}

func admin(t *testing.T) *k.AdminClient {
	t.Helper()
	a, err := k.NewAdminClient(&k.ConfigMap{"bootstrap.servers": broker(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeAdmin(t, a) })
	return a
}

func topic(t *testing.T, a *k.AdminClient, partitions int) string {
	t.Helper()
	name := fmt.Sprintf("kaferry-compat-%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	results, err := a.CreateTopics(ctx, []k.TopicSpecification{{Topic: name, NumPartitions: partitions, ReplicationFactor: 1, Config: map[string]string{"message.timestamp.before.max.ms": "9223372036854775807", "message.timestamp.after.max.ms": "9223372036854775807"}}})
	if err != nil || len(results) != 1 || results[0].Error.Code() != k.ErrNoError {
		t.Fatalf("create topic: %v %+v", err, results)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = a.DeleteTopics(ctx, []string{name})
	})
	waitTopicReady(t, a, name, partitions)
	return name
}

// Topic creation acknowledgement and leader metadata can precede offset availability.
func waitTopicReady(t *testing.T, a *k.AdminClient, name string, partitions int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	var last string
	for {
		request, done := context.WithTimeout(ctx, 5*time.Second)
		result, err := a.DescribeTopics(request, k.NewTopicCollectionOfTopicNames([]string{name}), k.SetAdminRequestTimeout(4*time.Second))
		done()
		ready := err == nil && len(result.TopicDescriptions) == 1
		for _, topic := range result.TopicDescriptions {
			ready = ready && topic.Error.Code() == k.ErrNoError && len(topic.Partitions) == partitions
			for _, partition := range topic.Partitions {
				ready = ready && partition.Leader != nil && partition.Leader.ID >= 0
			}
		}
		if ready {
			request, done := context.WithTimeout(ctx, 5*time.Second)
			queries := make(map[k.TopicPartition]k.OffsetSpec, partitions)
			for id := range partitions {
				queries[k.TopicPartition{Topic: &name, Partition: int32(id)}] = k.EarliestOffsetSpec
			}
			offsets, offsetErr := a.ListOffsets(request, queries, k.SetAdminRequestTimeout(4*time.Second))
			done()
			ready = offsetErr == nil && len(offsets.ResultInfos) == partitions
			for _, offset := range offsets.ResultInfos {
				ready = ready && offset.Error.Code() == k.ErrNoError && offset.Offset >= 0
			}
			if ready {
				return
			}
			last = fmt.Sprintf("offset error=%v results=%+v", offsetErr, offsets.ResultInfos)
		} else {
			last = fmt.Sprintf("error=%v description=%+v", err, result.TopicDescriptions)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("topic %s did not become ready: %s", name, last)
		case <-tick.C:
		}
	}
}

func waitTopicConfig(t *testing.T, a *k.AdminClient, name, key, value string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	var lastErr error
	var observed string
	for {
		request, done := context.WithTimeout(ctx, 5*time.Second)
		result, err := a.DescribeConfigs(request, []k.ConfigResource{{Type: k.ResourceTopic, Name: name}}, k.SetAdminRequestTimeout(4*time.Second))
		done()
		lastErr = err
		if err == nil && len(result) == 1 && result[0].Error.Code() == k.ErrNoError {
			observed = result[0].Config[key].Value
			if observed == value {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("topic %s config %s not visible: got=%q want=%q error=%v", name, key, observed, value, lastErr)
		case <-tick.C:
		}
	}
}

// Context cancellation does not cancel native admin requests; contain fixture cleanup.
func closeAdmin(t *testing.T, a *k.AdminClient) {
	t.Helper()
	done := make(chan struct{})
	go func() { a.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Error("native fixture AdminClient.Close exceeded 30s; client may leak until test process exits")
	}
}

func producer(t *testing.T, extra k.ConfigMap) *k.Producer {
	t.Helper()
	cfg := k.ConfigMap{"bootstrap.servers": broker(t), "enable.idempotence": true, "message.timeout.ms": 10000, "linger.ms": 10}
	for key, value := range extra {
		cfg[key] = value
	}
	p, err := k.NewProducer(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

func consumer(t *testing.T, group string) *k.Consumer {
	t.Helper()
	c, err := k.NewConsumer(&k.ConfigMap{"bootstrap.servers": broker(t), "group.id": group, "client.id": "kaferry-compat", "enable.auto.commit": false, "enable.auto.offset.store": false, "auto.offset.reset": "error", "isolation.level": "read_committed", "enable.partition.eof": true, "partition.assignment.strategy": "cooperative-sticky"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func publish(t *testing.T, p *k.Producer, message *k.Message) {
	t.Helper()
	reports := make(chan k.Event, 1)
	if err := p.Produce(message, reports); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-reports:
		if err := event.(*k.Message).TopicPartition.Error; err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("delivery timed out")
	}
}

func read(t *testing.T, c *k.Consumer) *k.Message {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		switch event := c.Poll(100).(type) {
		case *k.Message:
			if event.TopicPartition.Error != nil {
				t.Fatal(event.TopicPartition.Error)
			}
			return event
		case k.Error:
			t.Fatal(event)
		}
	}
	t.Fatal("record read timed out")
	return nil
}

func TestRecordFidelityAndTopicID(t *testing.T) {
	// Given
	a := admin(t)
	name := topic(t, a, 1)
	p := producer(t, nil)
	version, label := k.LibraryVersion()
	t.Logf("librdkafka=%s (%d)", label, version)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	description, err := a.DescribeTopics(ctx, k.NewTopicCollectionOfTopicNames([]string{name}))
	if err != nil || len(description.TopicDescriptions) != 1 {
		t.Fatalf("DescribeTopics: %v", err)
	}
	t.Logf("topic ID=%s", description.TopicDescriptions[0].TopicID.String())
	c := consumer(t, name+"-g")
	if err := c.Assign([]k.TopicPartition{{Topic: &name, Partition: 0, Offset: 0}}); err != nil {
		t.Fatal(err)
	}
	for _, data := range []struct{ key, value []byte }{{nil, nil}, {[]byte{}, []byte{}}, {[]byte{0, 255}, []byte{1, 0, 255}}} {
		m := &k.Message{TopicPartition: k.TopicPartition{Topic: &name, Partition: 0}, Key: data.key, Value: data.value, Timestamp: time.UnixMilli(1791000000123), Headers: []k.Header{{Key: "x", Value: nil}, {Key: "x", Value: []byte{}}, {Key: "x", Value: []byte{0, 255}}}}
		publish(t, p, m)

		// When
		got := read(t, c)

		// Then
		if !reflect.DeepEqual(got.Key, m.Key) || !reflect.DeepEqual(got.Value, m.Value) || !reflect.DeepEqual(got.Headers, m.Headers) || !got.Timestamp.Equal(m.Timestamp) {
			t.Fatalf("fidelity mismatch: key nil=%v/%v, value nil=%v/%v, headers=%v/%v, timestamp=%v/%v", got.Key == nil, m.Key == nil, got.Value == nil, m.Value == nil, got.Headers, m.Headers, got.Timestamp, m.Timestamp)
		}
	}
}

func TestTransactionalTailAndTimestampLookup(t *testing.T) {
	// Given
	for _, abort := range []bool{false, true} {
		t.Run(fmt.Sprintf("abort=%v", abort), func(t *testing.T) {
			a := admin(t)
			name := topic(t, a, 1)
			p := producer(t, k.ConfigMap{"transactional.id": name + "-tx"})
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			if err := p.InitTransactions(ctx); err != nil {
				t.Fatal(err)
			}
			if err := p.BeginTransaction(); err != nil {
				t.Fatal(err)
			}
			publish(t, p, &k.Message{TopicPartition: k.TopicPartition{Topic: &name, Partition: 0}, Value: []byte("txn"), Timestamp: time.UnixMilli(1791000000000)})
			c := consumer(t, name+"-g")

			// When
			lookup, err := c.OffsetsForTimes([]k.TopicPartition{{Topic: &name, Partition: 0, Offset: k.Offset(1791000000000)}}, 5000)

			// Then
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("lookup during open transaction=%v", lookup)
			ends, err := a.ListOffsets(ctx, map[k.TopicPartition]k.OffsetSpec{{Topic: &name, Partition: 0}: k.LatestOffsetSpec}, k.SetAdminIsolationLevel(k.IsolationLevelReadCommitted))
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("read-committed end while open=%+v", ends.ResultInfos)
			if abort {
				err = p.AbortTransaction(ctx)
			} else {
				err = p.CommitTransaction(ctx)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err = c.Assign([]k.TopicPartition{{Topic: &name, Partition: 0, Offset: 0}}); err != nil {
				t.Fatal(err)
			}
			var eof k.Offset
			var visible int
			deadline := time.Now().Add(10 * time.Second)
			for eof == 0 && time.Now().Before(deadline) {
				switch event := c.Poll(100).(type) {
				case *k.Message:
					visible++
				case k.PartitionEOF:
					eof = event.Offset
				case k.Error:
					t.Fatal(event)
				}
			}
			_, high, err := c.QueryWatermarkOffsets(name, 0, 5000)
			if err != nil {
				t.Fatal(err)
			}
			positions, err := c.Position([]k.TopicPartition{{Topic: &name, Partition: 0}})
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("visible=%d H=%d EOF=%d Position=%d", visible, high, eof, positions[0].Offset)
			if int64(eof) != high {
				t.Fatal("EOF did not reach high watermark")
			}
			if abort && visible != 0 || !abort && visible != 1 {
				t.Fatal("transaction isolation failed")
			}
			metadata := "kaferry-probe;yes"
			committed, err := c.CommitOffsets([]k.TopicPartition{{Topic: &name, Partition: 0, Offset: eof, Metadata: &metadata}})
			if err != nil || committed[0].Error != nil {
				t.Fatalf("commit EOF: %v %+v", err, committed)
			}
			other := consumer(t, name+"-g")
			saved, err := other.Committed([]k.TopicPartition{{Topic: &name, Partition: 0}}, 5000)
			if err != nil || saved[0].Offset != eof || saved[0].Metadata == nil || *saved[0].Metadata != metadata {
				t.Fatalf("resume metadata: %v %+v", err, saved)
			}
		})
	}
}

func TestTimestampLookupOutOfOrder(t *testing.T) {
	// Given
	a := admin(t)
	name := topic(t, a, 1)
	p := producer(t, nil)
	base := int64(1791000000000)
	for _, delta := range []int64{2000, 0, 1000, 2000} {
		publish(t, p, &k.Message{TopicPartition: k.TopicPartition{Topic: &name, Partition: 0}, Value: []byte("x"), Timestamp: time.UnixMilli(base + delta)})
	}
	c := consumer(t, name+"-g")
	for _, delta := range []int64{0, 1000, 2000, 3000} {

		// When
		positions, err := c.OffsetsForTimes([]k.TopicPartition{{Topic: &name, Partition: 0, Offset: k.Offset(base + delta)}}, 5000)

		// Then
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("timestamp delta=%d lookup=%v", delta, positions)
		if delta <= 2000 && positions[0].Offset != 0 {
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
	result, err := a.IncrementalAlterConfigs(ctx, []k.ConfigResource{{Type: k.ResourceTopic, Name: name, Config: []k.ConfigEntry{{Name: "message.timestamp.after.max.ms", Value: "1000", IncrementalOperation: k.AlterConfigOpTypeSet}}}})
	if err != nil || result[0].Error.Code() != k.ErrNoError {
		t.Fatalf("configure timestamp bound: %v %+v", err, result)
	}
	waitTopicConfig(t, a, name, "message.timestamp.after.max.ms", "1000")
	p := producer(t, k.ConfigMap{"linger.ms": 500})
	reports := make(chan k.Event, 2)
	for i, when := range []time.Time{time.Now(), time.Now().Add(time.Hour)} {

		// When
		produceErr := p.Produce(&k.Message{TopicPartition: k.TopicPartition{Topic: &name, Partition: 0}, Value: []byte("x"), Timestamp: when, Opaque: i}, reports)

		// Then
		if produceErr != nil {
			t.Fatal(produceErr)
		}
	}
	for range 2 {
		select {
		case event := <-reports:
			message := event.(*k.Message)
			t.Logf("record=%v error=%v", message.Opaque, message.TopicPartition.Error)
			if message.TopicPartition.Error == nil {
				t.Fatal("expected rejected mixed batch")
			}
		case <-time.After(15 * time.Second):
			t.Fatal("delivery timed out")
		}
	}
}
