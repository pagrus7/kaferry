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
	"kafka-fwd2/internal/app"
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
	if code := app.Execute(ctx, []string{"run", "--sql", statement}, nil, &output); code != 0 {
		t.Fatalf("copy exit=%d: %s", code, output.String())
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
	if code := app.Execute(ctx, []string{"run", "--sql", statement}, nil, &output); code != 0 {
		t.Fatalf("resume exit=%d: %s", code, output.String())
	}
}

func admin(t *testing.T) *k.AdminClient {
	t.Helper()
	a, err := k.NewAdminClient(&k.ConfigMap{"bootstrap.servers": broker(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Close)
	return a
}

func topic(t *testing.T, a *k.AdminClient, partitions int) string {
	t.Helper()
	name := fmt.Sprintf("kfwd2-compat-%d", time.Now().UnixNano())
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
	return name
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
	c, err := k.NewConsumer(&k.ConfigMap{"bootstrap.servers": broker(t), "group.id": group, "client.id": "kafka-fwd2-compat", "enable.auto.commit": false, "enable.auto.offset.store": false, "auto.offset.reset": "error", "isolation.level": "read_committed", "enable.partition.eof": true, "partition.assignment.strategy": "cooperative-sticky"})
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
		got := read(t, c)
		if !reflect.DeepEqual(got.Key, m.Key) || !reflect.DeepEqual(got.Value, m.Value) || !reflect.DeepEqual(got.Headers, m.Headers) || !got.Timestamp.Equal(m.Timestamp) {
			t.Fatalf("fidelity mismatch: key nil=%v/%v, value nil=%v/%v, headers=%v/%v, timestamp=%v/%v", got.Key == nil, m.Key == nil, got.Value == nil, m.Value == nil, got.Headers, m.Headers, got.Timestamp, m.Timestamp)
		}
	}
}

func TestTransactionalTailAndTimestampLookup(t *testing.T) {
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
			lookup, err := c.OffsetsForTimes([]k.TopicPartition{{Topic: &name, Partition: 0, Offset: k.Offset(1791000000000)}}, 5000)
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
			metadata := "kfwd2:1;probe=yes"
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
	a := admin(t)
	name := topic(t, a, 1)
	p := producer(t, nil)
	base := int64(1791000000000)
	for _, delta := range []int64{2000, 0, 1000, 2000} {
		publish(t, p, &k.Message{TopicPartition: k.TopicPartition{Topic: &name, Partition: 0}, Value: []byte("x"), Timestamp: time.UnixMilli(base + delta)})
	}
	c := consumer(t, name+"-g")
	for _, delta := range []int64{0, 1000, 2000, 3000} {
		positions, err := c.OffsetsForTimes([]k.TopicPartition{{Topic: &name, Partition: 0, Offset: k.Offset(base + delta)}}, 5000)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("timestamp delta=%d lookup=%v", delta, positions)
		if delta <= 2000 && positions[0].Offset != 0 {
			t.Fatal("lookup skipped earlier qualifying record")
		}
	}
}

func TestSynchronousSizeLimit(t *testing.T) {
	p := producer(t, k.ConfigMap{"message.max.bytes": 1000})
	name := "not-published-size-probe"
	if err := p.Produce(&k.Message{TopicPartition: k.TopicPartition{Topic: &name, Partition: k.PartitionAny}, Value: bytes.Repeat([]byte{1}, 1100)}, nil); err == nil {
		t.Fatal("oversized record did not fail synchronously")
	} else {
		t.Logf("synchronous oversized rejection=%v", err)
	}
}

func TestCooperativeRevocationCommitAndGroupInspection(t *testing.T) {
	a := admin(t)
	name := topic(t, a, 2)
	group := name + "-group"
	c1 := consumer(t, group)
	c2 := consumer(t, group)
	assigned := false
	revoked := false
	closing := false
	t.Cleanup(func() { closing = true })
	callback := func(c *k.Consumer, event k.Event) error {
		switch event := event.(type) {
		case k.AssignedPartitions:
			for i := range event.Partitions {
				event.Partitions[i].Offset = 0
			}
			if err := c.IncrementalAssign(event.Partitions); err != nil {
				t.Fatal(err)
			}
			assigned = true
		case k.RevokedPartitions:
			for i := range event.Partitions {
				metadata := "kfwd2:1;probe=revocation"
				event.Partitions[i].Offset = 0
				event.Partitions[i].Metadata = &metadata
			}
			if !closing && !c.AssignmentLost() {
				result, err := c.CommitOffsets(event.Partitions)
				if err != nil {
					t.Fatal(err)
				}
				for _, part := range result {
					if part.Error != nil {
						t.Fatal(part.Error)
					}
				}
			}
			if err := c.IncrementalUnassign(event.Partitions); err != nil {
				t.Fatal(err)
			}
			revoked = true
		}
		return nil
	}
	if err := c1.Subscribe(name, callback); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for !assigned && time.Now().Before(deadline) {
		c1.Poll(50)
	}
	if !assigned {
		t.Fatal("initial assignment timed out")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	description, err := a.DescribeConsumerGroups(ctx, []string{group})
	if err != nil || len(description.ConsumerGroupDescriptions) != 1 {
		t.Fatalf("group description: %v", err)
	}
	t.Logf("group members=%+v", description.ConsumerGroupDescriptions[0].Members)
	if err = c2.Subscribe(name, func(c *k.Consumer, event k.Event) error {
		switch event := event.(type) {
		case k.AssignedPartitions:
			for i := range event.Partitions {
				event.Partitions[i].Offset = 0
			}
			return c.IncrementalAssign(event.Partitions)
		case k.RevokedPartitions:
			return c.IncrementalUnassign(event.Partitions)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(15 * time.Second)
	for !revoked && time.Now().Before(deadline) {
		c1.Poll(50)
		c2.Poll(50)
	}
	if !revoked {
		t.Fatal("cooperative revocation timed out")
	}
	result, err := a.ListConsumerGroupOffsets(ctx, []k.ConsumerGroupTopicPartitions{{Group: group, Partitions: []k.TopicPartition{{Topic: &name, Partition: 0}, {Topic: &name, Partition: 1}}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("non-owned progress=%+v", result.ConsumerGroupsTopicPartitions)
}

func TestMixedTimestampBatchHasNoRecordAttribution(t *testing.T) {
	a := admin(t)
	name := topic(t, a, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result, err := a.AlterConfigs(ctx, []k.ConfigResource{{Type: k.ResourceTopic, Name: name, Config: []k.ConfigEntry{{Name: "message.timestamp.after.max.ms", Value: "1000"}}}})
	if err != nil || result[0].Error.Code() != k.ErrNoError {
		t.Fatalf("configure timestamp bound: %v %+v", err, result)
	}
	p := producer(t, k.ConfigMap{"linger.ms": 500})
	reports := make(chan k.Event, 2)
	for i, when := range []time.Time{time.Now(), time.Now().Add(time.Hour)} {
		if err := p.Produce(&k.Message{TopicPartition: k.TopicPartition{Topic: &name, Partition: 0}, Value: []byte("x"), Timestamp: when, Opaque: i}, reports); err != nil {
			t.Fatal(err)
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

func TestOutOfRangeFailsInsteadOfResetting(t *testing.T) {
	a := admin(t)
	name := topic(t, a, 1)
	c := consumer(t, name+"-g")
	if err := c.Assign([]k.TopicPartition{{Topic: &name, Partition: 0, Offset: 999999}}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if event, ok := c.Poll(100).(k.Error); ok {
			t.Logf("out-of-range error=%v", event)
			if event.Code() != k.ErrAutoOffsetReset {
				t.Fatal("unexpected error class")
			}
			return
		}
	}
	t.Fatal("out-of-range assignment silently reset or stalled")
}

func TestSizeBoundariesWithCompression(t *testing.T) {
	for _, codec := range []string{"none", "gzip", "snappy", "lz4", "zstd"} {
		t.Run(codec, func(t *testing.T) {
			a := admin(t)
			name := topic(t, a, 1)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_, err := a.AlterConfigs(ctx, []k.ConfigResource{{Type: k.ResourceTopic, Name: name, Config: []k.ConfigEntry{{Name: "max.message.bytes", Value: "2048"}}}})
			if err != nil {
				t.Fatal(err)
			}
			p := producer(t, k.ConfigMap{"message.max.bytes": 2048, "compression.type": codec, "linger.ms": 100})
			reports := make(chan k.Event, 8)
			for i := range 8 {
				value := make([]byte, 600)
				for j := range value {
					value[j] = byte(j*31 + i)
				}
				if err := p.Produce(&k.Message{TopicPartition: k.TopicPartition{Topic: &name, Partition: 0}, Key: []byte("key"), Value: value, Headers: []k.Header{{Key: "h", Value: []byte("v")}}}, reports); err != nil {
					t.Fatal(err)
				}
			}
			for range 8 {
				select {
				case event := <-reports:
					if err := event.(*k.Message).TopicPartition.Error; err != nil {
						t.Fatal(err)
					}
				case <-time.After(15 * time.Second):
					t.Fatal("delivery timed out")
				}
			}
		})
	}
}
