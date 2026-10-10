package kafka

import (
	"bytes"
	"errors"
	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSinkRoutesRawRecordsAndAttributesDeliveryFailures(t *testing.T) {
	// Given
	producer := &producerMock{events: make(chan k.Event, 4)}
	clients := &mockClients{producer: producer}
	clock := &fixedClock{now: time.UnixMilli(12345)}
	metadata := destinationMetadataMock{cfg: map[string]string{"message.timestamp.type": "CreateTime", "max.message.bytes": "10000"}, count: 5}
	sink, err := NewSink(clients, metadata, plan.Endpoint{Name: "orders"}, config.Defaults("home").Worker, clock, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	if clients.cfg["enable.idempotence"] != true || clients.cfg["partitioner"] != "murmur2_random" || clients.cfg["message.max.bytes"] != "10000" || sink.partitionCount != 5 {
		t.Fatal("unsafe destination configuration")
	}
	timestamp := int64(12345)
	token := core.Token{Partition: 2, Generation: 3, Sequence: 4}
	record := core.Record{Key: []byte{0, 255}, Value: nil, TimestampMS: &timestamp, Headers: []core.Header{{Name: "dup", Value: nil}, {Name: "dup", Value: []byte{}}}}

	// When
	submitErr := sink.Submit(record, token)

	// Then
	if submitErr != nil {
		t.Fatal(submitErr)
	}
	message := producer.messages[0]
	if message.TopicPartition.Partition != k.PartitionAny || !bytes.Equal(message.Key, record.Key) || message.Value != nil || message.Opaque != token || !message.Timestamp.Equal(clock.Now()) || !reflect.DeepEqual(message.Headers, []k.Header{{Key: "dup", Value: nil}, {Key: "dup", Value: []byte{}}}) {
		t.Fatal("raw record or routing changed", message)
	}
	failure := k.NewError(k.ErrTopicAuthorizationFailed, "denied", false)
	message.TopicPartition.Error = failure
	producer.events <- message
	producer.events <- k.NewError(k.ErrAllBrokersDown, "fatal", true)
	for _, fatal := range []bool{false, true} {
		select {
		case delivery := <-sink.Deliveries():
			if delivery.Fatal != fatal || delivery.Err == nil || !fatal && delivery.Token != token {
				t.Fatal("delivery attributed incorrectly", delivery)
			}
		case <-time.After(time.Second):
			t.Fatal("delivery report missing")
		}
	}
	producer.err = k.NewError(k.ErrQueueFull, "full", false)
	var busy *core.Busy
	if err := sink.Submit(record, token); !errors.As(err, &busy) {
		t.Fatal("queue-full error lost", err)
	}
}

func TestDestinationPartitionChangeWarnsWithoutChangingRecordRouting(t *testing.T) {
	// Given
	producer := &producerMock{events: make(chan k.Event), metadata: &k.Metadata{Topics: map[string]k.TopicMetadata{"orders": {Partitions: make([]k.PartitionMetadata, 7)}}}}
	clock := &fixedClock{now: time.UnixMilli(12345)}
	var logs bytes.Buffer
	sink, err := NewSink(&mockClients{producer: producer}, destinationMetadataMock{count: 5}, plan.Endpoint{Name: "orders"}, config.Defaults("home").Worker, clock, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	clock.now = clock.now.Add(31 * time.Second)
	record := core.Record{Key: []byte{0, 255}, Value: []byte("opaque")}

	// When
	submitErr := sink.Submit(record, core.Token{})

	// Then
	if submitErr != nil {
		t.Fatal(submitErr)
	}
	message := producer.messages[0]
	if sink.partitionCount != 7 || !strings.Contains(logs.String(), "destination partition count changed; key routing may change") || !bytes.Equal(message.Key, record.Key) || message.TopicPartition.Partition != k.PartitionAny {
		t.Fatal("partition expansion silently changed routing or record bytes", logs.String(), message)
	}
	producer.metadataErr = errors.New("metadata unavailable")
	clock.now = clock.now.Add(31 * time.Second)
	if err := sink.Submit(record, core.Token{}); err != nil || !strings.Contains(logs.String(), "destination partition count check failed") {
		t.Fatal("metadata failure unexpectedly stopped copying or was not logged", err, logs.String())
	}
}
