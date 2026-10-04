package kafka

import (
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"kafka-fwd2/internal/config"
	core "kafka-fwd2/internal/copy"
	"kafka-fwd2/internal/plan"
	"kafka-fwd2/internal/sql"
)

func TestProducerSizeLimitUsesDestinationAndExplicitCeiling(t *testing.T) {
	for _, scenario := range []struct {
		name, topic, explicit, want string
		invalid                     bool
	}{
		{"Kafka default", "1048588", "", "1048588", false},
		{"large topic", "8388608", "", "8388608", false},
		{"explicit smaller", "8388608", "2000000", "2000000", false},
		{"explicit larger", "1048588", "2000000", "1048588", false},
		{"unavailable config", "", "", "1000000", false},
		{"native minimum", "500", "", "1000", false},
		{"native maximum", "2147483647", "", "1000000000", false},
		{"explicit below minimum", "8388608", "999", "", true},
		{"explicit above maximum", "8388608", "1000000001", "", true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			// Given a destination limit and an optional explicit producer ceiling.
			properties := make(map[string]string)
			if scenario.explicit != "" {
				properties["message.max.bytes"] = scenario.explicit
			}
			producer := &producerMock{events: make(chan k.Event)}
			clients := &mockClients{producer: producer}
			metadata := destinationMetadataMock{cfg: map[string]string{"max.message.bytes": scenario.topic}}

			// When constructing the independently mocked Kafka sink.
			sink, err := NewSink(clients, metadata, sinkEndpoint(t, properties), config.Defaults("home").Worker, core.RealClock{}, quietLogger())

			// Then valid limits match the destination and invalid user settings fail.
			if scenario.invalid {
				if err == nil || sink != nil {
					t.Fatal("invalid producer ceiling accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer sink.Close()
			if clients.cfg["message.max.bytes"] != scenario.want {
				t.Fatal("wrong producer ceiling", clients.cfg)
			}
		})
	}
}

func TestProducerTimeoutAliasesHaveDeterministicPrecedence(t *testing.T) {
	for _, scenario := range []struct {
		name       string
		properties map[string]string
		want       string
		invalid    bool
	}{
		{"default", nil, "60000", false},
		{"canonical", map[string]string{"message.timeout.ms": "1000"}, "1000", false},
		{"alias", map[string]string{"delivery.timeout.ms": "1000"}, "1000", false},
		{"equal aliases", map[string]string{"delivery.timeout.ms": "1000", "message.timeout.ms": "1000"}, "1000", false},
		{"equivalent integers", map[string]string{"delivery.timeout.ms": "01000", "message.timeout.ms": "1000"}, "01000", false},
		{"explicit infinite", map[string]string{"delivery.timeout.ms": "0"}, "0", false},
		{"conflict", map[string]string{"delivery.timeout.ms": "1000", "message.timeout.ms": "2000"}, "", true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			// Given explicit timeout properties or the worker default.
			clients := &mockClients{producer: &producerMock{events: make(chan k.Event)}}

			// When constructing the sink through its client factory port.
			sink, err := NewSink(clients, destinationMetadataMock{}, sinkEndpoint(t, scenario.properties), config.Defaults("home").Worker, core.RealClock{}, quietLogger())

			// Then exactly one effective timeout remains, or a conflict fails.
			if scenario.invalid {
				if err == nil || sink != nil {
					t.Fatal("conflicting timeouts accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer sink.Close()
			if clients.cfg["message.timeout.ms"] != scenario.want {
				t.Fatal("timeout override lost", clients.cfg)
			}
			if _, exists := clients.cfg["delivery.timeout.ms"]; exists {
				t.Fatal("native alias collision retained")
			}
		})
	}
}

func TestTimestampLimitsResolveDirectionsIndependently(t *testing.T) {
	for _, scenario := range []struct {
		name, before, after, legacy string
		delta                       int64
		rejected                    bool
	}{
		{"legacy past", "9223372036854775807", "9223372036854775807", "1000", -1001, true},
		{"legacy future", "9223372036854775807", "9223372036854775807", "1000", 1001, true},
		{"inclusive legacy edge", "9223372036854775807", "9223372036854775807", "1000", -1000, false},
		{"direction overrides legacy", "5000", "", "1000", -2000, false},
		{"other direction unavailable", "1000", "", "", -1001, true},
		{"past unavailable future known", "", "1000", "", 1001, true},
		{"unavailable direction skipped", "", "1000", "", -2000, false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			// Given independently readable directional and legacy broker limits.
			clock := &fixedClock{now: time.UnixMilli(100000)}
			sink := &Sink{clock: clock, configuration: map[string]string{"message.timestamp.type": "CreateTime", "message.timestamp.before.max.ms": scenario.before, "message.timestamp.after.max.ms": scenario.after, "message.timestamp.difference.max.ms": scenario.legacy}}
			stamp := clock.Now().UnixMilli() + scenario.delta

			// When preflighting a record timestamp before native submission.
			err := sink.validate(core.Record{TimestampMS: &stamp})

			// Then only a known violated bound rejects the identified record.
			var recordError *core.RecordError
			if (err != nil) != scenario.rejected || err != nil && (!errors.As(err, &recordError) || recordError.Class != "rejected_timestamp") {
				t.Fatal("incorrect timestamp decision", err)
			}
		})
	}
}

func TestSavedMixedWindowSurvivesClockAdvanceAndAdoption(t *testing.T) {
	for _, origin := range []string{"saved", "adopted", "fresh"} {
		t.Run(origin, func(t *testing.T) {
			// Given a mixed NOW/literal window whose fresh resolution is now empty.
			hi, lo := int64(7200000), int64(3600000)
			p := &positioner{plan: plan.Plan{Window: plan.WindowSpec{Lower: &plan.Expression{Now: true, AdjustmentMS: -3600000}, Upper: &plan.Expression{LiteralMS: hi}}}, log: quietLogger()}
			saved, metadata := map[int32]k.TopicPartition{}, map[int32]Progress{}
			var adopted *plan.Window
			window := plan.Window{Lower: &lo, Upper: &hi}
			if origin == "saved" {
				saved[0] = k.TopicPartition{Offset: 42}
				metadata[0] = Progress{Window: &window}
			}
			if origin == "adopted" {
				adopted = &window
			}

			// When positioning after both the saved upper bound and its cutoff.
			got, _, position, err := p.window(0, saved, metadata, adopted, time.UnixMilli(14400000))

			// Then persisted windows remain authoritative; only fresh resolution fails.
			if origin == "fresh" {
				if err == nil {
					t.Fatal("empty fresh window accepted")
				}
				return
			}
			if err != nil || !reflect.DeepEqual(got, window) || origin == "saved" && position != 42 {
				t.Fatal("saved window was re-resolved", got, position, err)
			}
		})
	}
}

func TestWatermarkErrorsAndDNSRespectRuntimeRetryClassification(t *testing.T) {
	for _, scenario := range []struct {
		name                    string
		code                    k.ErrorCode
		fatal, retry, pollRetry bool
	}{
		{"timeout", k.ErrTimedOut, false, true, true}, {"DNS", k.ErrResolve, false, true, true},
		{"fatal DNS", k.ErrResolve, true, false, false}, {"authorization", k.ErrTopicAuthorizationFailed, false, false, false},
		{"not leader", k.ErrNotLeaderForPartition, false, true, false},
		{"leader unavailable", k.ErrLeaderNotAvailable, false, true, false},
		{"replica unavailable", k.ErrReplicaNotAvailable, false, true, false},
		{"storage", k.ErrKafkaStorageError, false, true, false},
		{"offset unavailable", k.ErrOffsetNotAvailable, false, true, false},
		{"fenced epoch", k.ErrFencedLeaderEpoch, false, true, false},
		{"unknown epoch", k.ErrUnknownLeaderEpoch, false, true, false},
		{"unknown topic", k.ErrUnknownTopicOrPart, false, false, false},
		{"fatal leader", k.ErrNotLeaderForPartition, true, false, false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			// Given an independently mocked watermark query failure.
			native := k.NewError(scenario.code, "query failure", scenario.fatal)
			source := &Source{consumer: &consumerMock{watermarkErr: native}, plan: plan.Plan{Source: plan.Endpoint{Name: "orders"}}}

			// When sampling a partition's high watermark.
			_, err := source.HighWatermark(0)

			// Then only nonfatal transient failures are marked recoverable.
			var recoverable *core.Recoverable
			if errors.As(err, &recoverable) != scenario.retry || transient(native) != scenario.pollRetry {
				t.Fatal("wrong classification", err)
			}
			if !errors.Is(err, native) {
				t.Fatal("native cause lost", err)
			}
		})
	}
	// Given a successful native query.
	source := &Source{consumer: &consumerMock{high: math.MaxInt32}}

	// When reading the high watermark.
	high, err := source.HighWatermark(0)

	// Then its exact offset is returned without a retry marker.
	if err != nil || high != math.MaxInt32 {
		t.Fatal(high, err)
	}
}

func sinkEndpoint(t *testing.T, properties map[string]string) plan.Endpoint {
	t.Helper()
	profile := map[string]string{"bootstrap.servers": "fixture.invalid:9092"}
	for key, value := range properties {
		profile[key] = value
	}
	statement, err := sql.NewParser().Parse("INSERT INTO TOPIC 'copy' SELECT * FROM TOPIC 'orders' OPTIONS(GROUP='g',START=EARLIEST)")
	if err != nil {
		t.Fatal(err)
	}
	p, err := plan.NewPlanner(nil).Build(statement, plan.Profiles{"default": profile})
	if err != nil {
		t.Fatal(err)
	}
	return p.Destination
}

func TestAssignmentReadsCommittedMetadataThroughConsumer(t *testing.T) {
	for _, scenario := range []string{"valid", "query failure", "partition failure", "missing result", "foreign metadata"} {
		t.Run(scenario, func(t *testing.T) {
			// Given discovered source partitions and independently mocked consumer progress.
			name := "orders"
			metadata := (Progress{Fingerprint: "fp", TopicID: "topic-id"}).Encode()
			request := []k.TopicPartition{{Topic: &name, Partition: 0}, {Topic: &name, Partition: 1}}
			consumer := &consumerMock{committed: []k.TopicPartition{{Topic: &name, Partition: 0, Offset: 42, Metadata: &metadata}, {Topic: &name, Partition: 1, Offset: k.OffsetInvalid}}}
			cause := errors.New("injected fetch failure")
			switch scenario {
			case "query failure":
				consumer.committedErr = cause
			case "partition failure":
				consumer.committed[0].Error = cause
			case "missing result":
				consumer.committed = consumer.committed[:1]
			case "foreign metadata":
				bad := "foreign"
				consumer.committed[0].Metadata = &bad
			}
			source := &Source{consumer: consumer, inspector: &sourceMetadataMock{parts: request}, fingerprint: "fp", plan: plan.Plan{Source: plan.Endpoint{Name: name}}}

			// When an assignment reads group progress, including an unowned partition.
			saved, progress, err := source.saved()

			// Then the consumer returns metadata without a coordinator-targeted admin call.
			if !reflect.DeepEqual(consumer.committedRequests, request) || consumer.committedTimeout != 10000 {
				t.Fatal("consumer did not query every source partition", consumer.committedRequests, consumer.committedTimeout)
			}
			if scenario == "valid" {
				if err != nil || len(saved) != 1 || saved[0].Offset != 42 || progress[0].Fingerprint != "fp" {
					t.Fatal("committed metadata lost", saved, progress, err)
				}
			} else if err == nil || (scenario == "query failure" || scenario == "partition failure") && !errors.Is(err, cause) {
				t.Fatal("invalid progress accepted or cause lost", err)
			}
		})
	}
}
