package kafka

import (
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pagrus7/kaferry/internal/config"
	copy "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
)

type Sink struct {
	producer       ProducerClient
	topic          string
	configuration  map[string]string
	clock          copy.Clock
	log            *slog.Logger
	deliveries     chan copy.Delivery
	done           chan struct{}
	stamped        int64
	partitionCount int
	lastMetadata   time.Time
}

func NewSink(clients Clients, inspector DestinationMetadata, endpoint plan.Endpoint, limits config.Worker, clock copy.Clock, logger *slog.Logger) (*Sink, error) {
	properties, err := producerConfig(endpoint, limits)
	if err != nil {
		return nil, err
	}
	topicConfig := inspector.Config()
	messageLimit, err := sizeProducer(properties, topicConfig, limits)
	if err != nil {
		return nil, err
	}
	p, err := clients.Producer(properties)
	if err != nil {
		return nil, conflict("invalid Kafka producer configuration")
	}
	s := &Sink{producer: p, topic: endpoint.Name, configuration: topicConfig, clock: clock, log: logger, deliveries: make(chan copy.Delivery, limits.RecordCap+1), done: make(chan struct{})}
	s.partitionCount = inspector.PartitionCount()
	s.lastMetadata = clock.Now()
	logger.Info("producer configured", "topic", s.topic, "message.max.bytes", messageLimit, "partitioner", properties["partitioner"])
	go s.reports()
	return s, nil
}

func (s *Sink) Submit(record copy.Record, token copy.Token) error {
	if err := s.validate(record); err != nil {
		return err
	}
	s.inspectRouting()
	message := &k.Message{TopicPartition: k.TopicPartition{Topic: &s.topic, Partition: k.PartitionAny}, Key: record.Key, Value: record.Value, Opaque: token}
	if record.TimestampMS != nil && s.configuration["message.timestamp.type"] != "LogAppendTime" {
		message.Timestamp = time.UnixMilli(*record.TimestampMS)
	}
	for _, header := range record.Headers {
		message.Headers = append(message.Headers, k.Header{Key: header.Name, Value: header.Value})
	}
	err := s.producer.Produce(message, nil)
	if err == nil && record.TimestampMS == nil {
		s.stamped++
	}
	if native, ok := err.(k.Error); ok {
		if native.Code() == k.ErrQueueFull {
			return &copy.Busy{}
		}
		if native.Code() == k.ErrMsgSizeTooLarge {
			return &copy.RecordError{Class: "validation", Err: fmt.Errorf("record exceeds producer size limit")}
		}
	}
	return err
}

func (s *Sink) inspectRouting() {
	if s.clock.Now().Sub(s.lastMetadata) < 30*time.Second {
		return
	}
	s.lastMetadata = s.clock.Now()
	metadata, err := s.producer.GetMetadata(&s.topic, false, 1000)
	if err != nil {
		s.log.Warn("destination partition count check failed")
		return
	}
	topic, ok := metadata.Topics[s.topic]
	if !ok || topic.Error.Code() != k.ErrNoError {
		return
	}
	if count := len(topic.Partitions); count != s.partitionCount {
		s.log.Warn("destination partition count changed; key routing may change", "topic", s.topic, "previous", s.partitionCount, "current", count)
		s.partitionCount = count
	}
}
func (s *Sink) validate(record copy.Record) error {
	for _, header := range record.Headers {
		if strings.ContainsRune(header.Name, 0) {
			return &copy.RecordError{Class: "validation", Err: fmt.Errorf("Kafka client cannot preserve NUL in header names")}
		}
	}
	if record.TimestampMS == nil {
		return nil
	}
	if s.configuration["message.timestamp.type"] == "LogAppendTime" {
		return nil
	}
	ts := *record.TimestampMS
	// Go client's conversion uses UnixNano; zero asks librdkafka to supply now.
	if ts <= 0 || ts > math.MaxInt64/1000000 {
		return &copy.RecordError{Class: "rejected_timestamp", Err: fmt.Errorf("timestamp cannot be preserved by the Kafka client")}
	}
	if s.configuration["message.timestamp.type"] != "CreateTime" {
		return nil
	}
	now := s.clock.Now().UnixMilli()
	before, beforeKnown := timestampLimit(s.configuration, "before")
	after, afterKnown := timestampLimit(s.configuration, "after")
	if ts < now && beforeKnown && now-ts > before || ts > now && afterKnown && ts-now > after {
		return &copy.RecordError{Class: "rejected_timestamp", Err: fmt.Errorf("timestamp exceeds destination acceptance bounds")}
	}
	return nil
}

func timestampLimit(configuration map[string]string, direction string) (int64, bool) {
	value, err := strconv.ParseInt(configuration["message.timestamp."+direction+".max.ms"], 10, 64)
	if err != nil || value == math.MaxInt64 {
		value, err = strconv.ParseInt(configuration["message.timestamp.difference.max.ms"], 10, 64)
	}
	return value, err == nil && value >= 0
}
func (s *Sink) reports() {
	for event := range s.producer.Events() {
		var report copy.Delivery
		switch event := event.(type) {
		case *k.Message:
			token, ok := event.Opaque.(copy.Token)
			if !ok {
				continue
			}
			report = copy.Delivery{Token: token, Err: event.TopicPartition.Error}
		case k.Error:
			if !event.IsFatal() {
				continue
			}
			report = copy.Delivery{Err: event, Fatal: true}
		default:
			continue
		}
		select {
		case s.deliveries <- report:
		case <-s.done:
			return
		}
	}
}
func (s *Sink) Deliveries() <-chan copy.Delivery { return s.deliveries }
func (s *Sink) Flush() error                     { return nil } // Kafka batches internally; no producer-wide drain.
func (s *Sink) Close() {
	close(s.done)
	s.producer.Close()
	if s.stamped > 0 {
		s.log.Warn("records received producer timestamps", "count", s.stamped)
	}
}
