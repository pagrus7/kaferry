package kafka

import (
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"kafka-fwd2/internal/config"
	copy "kafka-fwd2/internal/copy"
	"kafka-fwd2/internal/plan"
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
	properties := native(endpoint.Properties)
	if err := canonicalTimeout(properties); err != nil {
		return nil, err
	}
	for key, value := range map[string]any{"go.delivery.reports": true, "go.batch.producer": false, "allow.auto.create.topics": false} {
		if old, ok := properties[key]; ok && fmt.Sprint(old) != fmt.Sprint(value) {
			return nil, conflict("protected Kafka property %q cannot be overridden", key)
		}
		properties[key] = value
	}
	if value, ok := properties["transactional.id"]; ok && fmt.Sprint(value) != "" {
		return nil, conflict("transactional.id is incompatible with the copy producer")
	}
	if err := boundQueue(properties, "go.events.channel.size", int64(limits.RecordCap)+1); err != nil {
		return nil, err
	}
	if value, ok := properties["enable.idempotence"]; ok && fmt.Sprint(value) != "true" {
		return nil, conflict("enable.idempotence is protected")
	}
	properties["enable.idempotence"] = true
	if _, ok := properties["partitioner"]; !ok {
		properties["partitioner"] = "murmur2_random"
	}
	delivery, _ := time.ParseDuration(limits.DeliveryTimeout)
	if _, ok := properties["message.timeout.ms"]; !ok {
		properties["message.timeout.ms"] = strconv.FormatInt(delivery.Milliseconds(), 10)
	}
	topicConfig := inspector.Config()
	messageLimit, err := producerLimit(properties, topicConfig)
	if err != nil {
		return nil, err
	}
	properties["message.max.bytes"] = strconv.FormatInt(messageLimit, 10)
	if err := boundQueue(properties, "queue.buffering.max.messages", int64(limits.RecordCap)); err != nil {
		return nil, err
	}
	queueBytes := limits.ByteCap
	if messageLimit > queueBytes {
		queueBytes = messageLimit
	}
	if err := boundQueue(properties, "queue.buffering.max.kbytes", (queueBytes+1023)/1024); err != nil {
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

func canonicalTimeout(properties k.ConfigMap) error {
	alias, present := properties["delivery.timeout.ms"]
	if !present {
		return nil
	}
	if value, present := properties["message.timeout.ms"]; present && fmt.Sprint(value) != fmt.Sprint(alias) {
		left, leftErr := strconv.ParseInt(fmt.Sprint(value), 10, 64)
		right, rightErr := strconv.ParseInt(fmt.Sprint(alias), 10, 64)
		if leftErr != nil || rightErr != nil || left != right {
			return conflict("conflicting message.timeout.ms and delivery.timeout.ms")
		}
	}
	properties["message.timeout.ms"] = alias
	delete(properties, "delivery.timeout.ms")
	return nil
}

func producerLimit(properties k.ConfigMap, configuration map[string]string) (int64, error) {
	limit := int64(1000000)
	value, explicit := properties["message.max.bytes"]
	if explicit {
		parsed, err := strconv.ParseInt(fmt.Sprint(value), 10, 64)
		if err != nil || parsed < 1000 || parsed > 1000000000 {
			return 0, conflict("message.max.bytes must be between 1000 and 1000000000")
		}
		limit = parsed
	}
	if topic, err := strconv.ParseInt(configuration["max.message.bytes"], 10, 64); err == nil && topic >= 0 && (!explicit || topic < limit) {
		limit = topic
	}
	return max(1000, min(limit, 1000000000)), nil
}

func boundQueue(properties k.ConfigMap, key string, limit int64) error {
	if existing, ok := properties[key]; ok {
		value, err := strconv.ParseInt(fmt.Sprint(existing), 10, 64)
		if err != nil || value <= 0 || value > limit {
			return conflict("%s must be positive and no larger than %d", key, limit)
		}
		if strings.HasPrefix(key, "go.") {
			properties[key] = int(value)
		}
		return nil
	}
	properties[key] = strconv.FormatInt(limit, 10)
	if strings.HasPrefix(key, "go.") {
		properties[key] = int(limit)
	}
	return nil
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
