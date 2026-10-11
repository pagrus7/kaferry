package kafka

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pagrus7/kaferry/internal/config"
	"github.com/pagrus7/kaferry/internal/plan"
)

// consumerConfig protects manual commits and bounds the native fetch queue.
func consumerConfig(p plan.Plan, limits config.Worker) (k.ConfigMap, error) {
	cfg := native(p.Source.Properties)
	queueKB := (limits.ByteCap + 1023) / 1024
	if existing, ok := cfg["queued.max.messages.kbytes"]; !ok {
		cfg["queued.max.messages.kbytes"] = fmt.Sprint(queueKB)
	} else {
		var value int64
		if _, err := fmt.Sscan(fmt.Sprint(existing), &value); err != nil || value > queueKB || value < 1 {
			return nil, conflict("queued.max.messages.kbytes must be positive and no larger than the worker byte cap")
		}
	}
	if err := protectConfig(cfg, []protectedProperty{
		{"group.id", p.Group}, {"client.id", "kaferry-" + p.Group},
		{"enable.auto.commit", false}, {"enable.auto.offset.store", false},
		{"auto.offset.reset", "error"}, {"isolation.level", "read_committed"},
		{"enable.partition.eof", true}, {"partition.assignment.strategy", "cooperative-sticky"},
		{"group.protocol", "classic"}, {"allow.auto.create.topics", false},
		{"go.events.channel.enable", false},
	}); err != nil {
		return nil, err
	}
	return cfg, nil
}

// producerConfig rejects incompatible properties before any metadata RPC.
func producerConfig(endpoint plan.Endpoint, limits config.Worker) (k.ConfigMap, error) {
	properties := native(endpoint.Properties)
	if err := canonicalTimeout(properties); err != nil {
		return nil, err
	}
	if err := protectConfig(properties, []protectedProperty{
		{"go.delivery.reports", true}, {"go.batch.producer", false}, {"allow.auto.create.topics", false},
	}); err != nil {
		return nil, err
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
	return properties, nil
}

// sizeProducer applies optional topic limits after the static policy checks.
func sizeProducer(properties k.ConfigMap, topicConfig map[string]string, limits config.Worker) (int64, error) {
	messageLimit, err := producerLimit(properties, topicConfig)
	if err != nil {
		return 0, err
	}
	properties["message.max.bytes"] = strconv.FormatInt(messageLimit, 10)
	if err := boundQueue(properties, "queue.buffering.max.messages", int64(limits.RecordCap)); err != nil {
		return 0, err
	}
	queueBytes := limits.ByteCap
	if messageLimit > queueBytes {
		queueBytes = messageLimit
	}
	if err := boundQueue(properties, "queue.buffering.max.kbytes", (queueBytes+1023)/1024); err != nil {
		return 0, err
	}
	return messageLimit, nil
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
	var capacity any = strconv.FormatInt(limit, 10)
	if existing, ok := properties[key]; ok {
		capacity = existing
	}
	text := fmt.Sprint(capacity)
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil || value <= 0 || value > limit {
		return conflict("%s must be positive and no larger than %d", key, limit)
	}
	if strings.HasPrefix(key, "go.") {
		integer, err := strconv.Atoi(text)
		if err != nil {
			return conflict("%s exceeds Go integer capacity", key)
		}
		capacity = integer
	}
	properties[key] = capacity
	return nil
}

// The slice makes conflicting-property diagnostics deterministic.
type protectedProperty struct {
	key   string
	value any
}

func protectConfig(properties k.ConfigMap, protected []protectedProperty) error {
	for _, property := range protected {
		if old, ok := properties[property.key]; ok && fmt.Sprint(old) != fmt.Sprint(property.value) {
			return conflict("protected Kafka property %q cannot be overridden", property.key)
		}
		properties[property.key] = property.value
	}
	return nil
}
