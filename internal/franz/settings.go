// Package franz implements Kafka endpoints using franz-go.
package franz

import (
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/twmb/franz-go/pkg/kgo"
)

type settings struct {
	brokers, protocol, mechanism, username, password string
	ca, certificate, key, hostname, clientID         string
	compression                                      string
	linger, request, overhead, metadata              time.Duration
	session, heartbeat, rebalance, delivery          time.Duration
	messageBytes, queueRecords, queueBytes           int64
	fetchBytes, partitionBytes, fetchQueueBytes      int64
	explicitMessageBytes                             bool
}

func (settings) String() string     { return "[Kafka settings redacted]" }
func (s settings) GoString() string { return s.String() }

func invalidProperty(name, hint string) error {
	return &kafkaport.Conflict{Err: fmt.Errorf("kafka property %s: %s", name, hint)}
}

// parseSettings does no I/O and keeps raw plan properties unchanged for identity.
func parseSettings(properties plan.Properties) (settings, error) {
	s := settings{protocol: "PLAINTEXT", mechanism: "PLAIN", hostname: "https", compression: "none", linger: 5 * time.Millisecond, request: 30 * time.Second, overhead: 2 * time.Second, metadata: 5 * time.Minute, session: 45 * time.Second, heartbeat: 3 * time.Second, rebalance: 5 * time.Minute, messageBytes: 1000000, fetchBytes: 50 << 20, partitionBytes: 1 << 20}
	rules := s.rules()
	values := properties.Values()
	_, s.explicitMessageBytes = values["message.max.bytes"]
	keys := slices.Sorted(maps.Keys(values))
	for _, key := range keys {
		apply, ok := rules[key]
		if !ok {
			return settings{}, unsupportedProperty(key, rules)
		}
		if err := apply(values[key]); err != nil {
			return settings{}, invalidProperty(key, err.Error())
		}
	}
	if err := s.validate(values, keys); err != nil {
		return settings{}, err
	}
	return s, nil
}

type propertyRule func(string) error

func (s *settings) rules() map[string]propertyRule {
	return map[string]propertyRule{
		"bootstrap.servers": textProperty(&s.brokers), "security.protocol": choiceProperty(&s.protocol, "PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"),
		"sasl.mechanism": choiceProperty(&s.mechanism, "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"), "sasl.username": textProperty(&s.username), "sasl.password": textProperty(&s.password),
		"ssl.ca.location": textProperty(&s.ca), "ssl.certificate.location": textProperty(&s.certificate), "ssl.key.location": textProperty(&s.key),
		"ssl.endpoint.identification.algorithm": choiceProperty(&s.hostname, "https", "none"), "client.id": textProperty(&s.clientID),
		"message.max.bytes":  intProperty(&s.messageBytes, 1000, 1000000000),
		"message.timeout.ms": durationProperty(&s.delivery, 1), "delivery.timeout.ms": durationProperty(&s.delivery, 1),
		"queue.buffering.max.messages": intProperty(&s.queueRecords, 1, 2147483647), "queue.buffering.max.kbytes": kibProperty(&s.queueBytes),
		"queued.max.messages.kbytes": kibProperty(&s.fetchQueueBytes), "fetch.max.bytes": intProperty(&s.fetchBytes, 1, 1000000000), "max.partition.fetch.bytes": intProperty(&s.partitionBytes, 1, 1000000000),
		"session.timeout.ms": durationProperty(&s.session, 100), "heartbeat.interval.ms": durationProperty(&s.heartbeat, 1), "max.poll.interval.ms": durationProperty(&s.rebalance, 100),
		"request.timeout.ms": durationProperty(&s.request, 100), "socket.timeout.ms": durationBetween(&s.overhead, 100, 900000), "linger.ms": durationBetween(&s.linger, 0, 60000),
		"metadata.max.age.ms": durationBetween(&s.metadata, 10, 3600000), "topic.metadata.refresh.interval.ms": durationBetween(&s.metadata, 10, 3600000),
		"compression.type": choiceProperty(&s.compression, "none", "gzip", "snappy", "lz4", "zstd"),
	}
}

func textProperty(target *string) propertyRule {
	return func(value string) error { *target = value; return nil }
}
func choiceProperty(target *string, allowed ...string) propertyRule {
	return func(value string) error {
		if !slices.Contains(allowed, value) {
			return fmt.Errorf("must be one of %s", strings.Join(allowed, ", "))
		}
		*target = value
		return nil
	}
}
func intProperty(target *int64, low, high int64) propertyRule {
	return func(value string) error {
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil || n < low || n > high {
			return fmt.Errorf("must be an integer between %d and %d", low, high)
		}
		*target = n
		return nil
	}
}
func durationProperty(target *time.Duration, low int64) propertyRule {
	return durationBetween(target, low, 2147483647)
}
func durationBetween(target *time.Duration, low, high int64) propertyRule {
	return func(value string) error {
		var n int64
		if err := intProperty(&n, low, high)(value); err != nil {
			return err
		}
		*target = time.Duration(n) * time.Millisecond
		return nil
	}
}
func kibProperty(target *int64) propertyRule {
	return func(value string) error {
		var n int64
		if err := intProperty(&n, 1, 2147483647)(value); err != nil {
			return err
		}
		*target = n * 1024
		return nil
	}
}

func (s settings) validate(values map[string]string, keys []string) error {
	if strings.TrimSpace(s.brokers) == "" {
		return invalidProperty("bootstrap.servers", "must not be empty")
	}
	for _, broker := range strings.Split(s.brokers, ",") {
		if strings.TrimSpace(broker) == "" || strings.Contains(broker, "://") {
			return invalidProperty("bootstrap.servers", "use comma-separated host[:port] entries without a protocol prefix")
		}
	}
	if s.heartbeat >= s.session {
		return invalidProperty("heartbeat.interval.ms", "must be smaller than session.timeout.ms")
	}
	if s.rebalance < s.session || s.rebalance < 30*time.Second {
		return invalidProperty("max.poll.interval.ms", "must be at least session.timeout.ms and 30000 ms to allow draining and a final commit")
	}
	for _, key := range keys {
		if strings.HasPrefix(key, "ssl.") && !strings.HasSuffix(s.protocol, "SSL") {
			return invalidProperty(key, "requires SSL or SASL_SSL")
		}
		if strings.HasPrefix(key, "sasl.") && !strings.HasPrefix(s.protocol, "SASL_") {
			return invalidProperty(key, "requires SASL_PLAINTEXT or SASL_SSL")
		}
	}
	for _, pair := range [][2]string{{"message.timeout.ms", "delivery.timeout.ms"}, {"metadata.max.age.ms", "topic.metadata.refresh.interval.ms"}} {
		a, hasA := values[pair[0]]
		b, hasB := values[pair[1]]
		if hasA && hasB {
			left, _ := strconv.ParseInt(a, 10, 64)
			right, _ := strconv.ParseInt(b, 10, 64)
			if left != right {
				return invalidProperty(pair[0], "conflicts with "+pair[1])
			}
		}
	}
	if strings.HasPrefix(s.protocol, "SASL_") && (s.username == "" || s.password == "") {
		return invalidProperty("sasl.username/sasl.password", "both are required for SASL")
	}
	if (s.certificate == "") != (s.key == "") {
		return invalidProperty("ssl.certificate.location/ssl.key.location", "both are required for a client certificate")
	}
	return nil
}

func unsupportedProperty(key string, rules map[string]propertyRule) error {
	if key == "partitioner" {
		return invalidProperty(key, "unsupported; use WITH (PARTITIONING = PRESERVE) or WITH (PARTITIONING = KEY) after the INTO endpoint")
	}
	keys := slices.Sorted(maps.Keys(rules))
	// Property names are input too; do not print arbitrary bytes in diagnostics.
	if strings.IndexFunc(key, func(c rune) bool { return !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '.' || c == '_') }) >= 0 {
		key = "(invalid property name)"
	}
	return invalidProperty(key, "unsupported; supported properties: "+strings.Join(keys, ", "))
}

func (s settings) bounded(limits config.Worker) (settings, error) {
	if limits.RecordCap > math.MaxInt32-1 {
		return s, fmt.Errorf("worker.record_cap exceeds the Kafka buffer limit")
	}
	if limits.ByteCap > math.MaxInt64-1000001024 {
		return s, fmt.Errorf("worker.byte_cap leaves no room for Kafka buffer accounting")
	}

	if s.queueRecords > int64(limits.RecordCap) {
		return s, invalidProperty("queue.buffering.max.messages", "exceeds worker.record_cap")
	}
	queueBound := ((max(limits.ByteCap, s.messageBytes) + 1023) / 1024) * 1024
	if s.queueBytes > queueBound {
		return s, invalidProperty("queue.buffering.max.kbytes", "exceeds the worker/record byte bound")
	}
	if s.fetchQueueBytes > ((limits.ByteCap+1023)/1024)*1024 {
		return s, invalidProperty("queued.max.messages.kbytes", "exceeds worker.byte_cap")
	}
	if s.queueRecords == 0 {
		s.queueRecords = int64(limits.RecordCap)
	}
	if s.queueBytes == 0 {
		s.queueBytes = queueBound
	}
	if s.fetchQueueBytes == 0 {
		s.fetchQueueBytes = limits.ByteCap
	}
	if s.delivery == 0 {
		s.delivery, _ = time.ParseDuration(limits.DeliveryTimeout)
	}
	return s, nil
}

func (s settings) commonOptions() ([]kgo.Opt, error) {
	options := []kgo.Opt{kgo.SeedBrokers(strings.Split(s.brokers, ",")...), kgo.ClientID("kaferry-" + s.clientID), kgo.RequestTimeoutOverhead(s.overhead), kgo.MetadataMaxAge(s.metadata), kgo.MetadataMinAge(min(s.metadata, 5*time.Second)), kgo.BrokerMaxReadBytes(int32(max(100<<20, s.fetchBytes+1024, s.partitionBytes+1024))), kgo.BrokerMaxWriteBytes(int32(max(100<<20, s.messageBytes+1024))), kgo.DisableClientMetrics()}
	auth, err := s.securityOptions()
	return append(options, auth...), err
}

func (s settings) consumerOptions() []kgo.Opt {
	return []kgo.Opt{kgo.FetchMaxBytes(int32(min(s.fetchBytes, s.fetchQueueBytes))), kgo.FetchMaxPartitionBytes(int32(min(s.partitionBytes, s.fetchQueueBytes))), kgo.MaxConcurrentFetches(1), kgo.SessionTimeout(s.session), kgo.HeartbeatInterval(s.heartbeat), kgo.RebalanceTimeout(s.rebalance)}
}

func (s settings) producerOptions() []kgo.Opt {
	// Keep the narrowing conversion within the client's supported batch range.
	batchBytes := s.messageBytes
	if batchBytes < 1000 {
		batchBytes = 1000
	}
	if batchBytes > 1000000000 {
		batchBytes = 1000000000
	}
	codecs := map[string]kgo.CompressionCodec{"none": kgo.NoCompression(), "gzip": kgo.GzipCompression(), "snappy": kgo.SnappyCompression(), "lz4": kgo.Lz4Compression(), "zstd": kgo.ZstdCompression()}
	return []kgo.Opt{kgo.RequiredAcks(kgo.AllISRAcks()), kgo.AllowIdempotentProduceCancellation(), kgo.StopProducerOnDataLossDetected(), kgo.ProducerLinger(s.linger), kgo.ProduceRequestTimeout(s.request), kgo.ProducerBatchCompression(codecs[s.compression]), kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)), kgo.ProducerBatchMaxBytes(int32(batchBytes)), kgo.MaxBufferedRecords(int(s.queueRecords) + 1), kgo.MaxBufferedBytes(int(s.queueBytes + s.messageBytes))}
}
