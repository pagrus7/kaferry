package kafka

import (
	"errors"
	"fmt"
	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/pagrus7/kaferry/internal/sql"
	"testing"
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

func TestNativeQueueBoundsAndGoPropertyTypes(t *testing.T) {
	// Given
	for _, key := range []string{"queue.buffering.max.messages", "queue.buffering.max.kbytes", "go.events.channel.size"} {
		for _, value := range []string{"0", "-1", "101", "oops"} {
			p := k.ConfigMap{key: value}

			// When
			boundQueueErr := boundQueue(p, key, 100)

			// Then
			if boundQueueErr == nil {
				t.Fatal("accepted invalid queue bound", key, value)
			}
		}
		p := k.ConfigMap{key: "30"}
		if err := boundQueue(p, key, 100); err != nil {
			t.Fatal(err)
		}
		if key == "go.events.channel.size" {
			if _, ok := p[key].(int); !ok {
				t.Fatal("Go client requires an integer event capacity")
			}
		}
	}
}

type countedDestination struct{ calls int }

func (m *countedDestination) Config() map[string]string { m.calls++; return nil }
func (*countedDestination) PartitionCount() int         { return 1 }

func TestProducerPolicyFailsBeforeDestinationLookup(t *testing.T) {
	for _, properties := range []map[string]string{
		{"go.delivery.reports": "false"}, {"go.batch.producer": "true"}, {"allow.auto.create.topics": "true"},
		{"delivery.report.only.error": "true"},
		{"transactional.id": "private-transaction"}, {"enable.idempotence": "false"}, {"go.events.channel.size": "0"},
		{"message.timeout.ms": "1000", "delivery.timeout.ms": "2000"},
	} {
		t.Run(fmt.Sprint(properties), func(t *testing.T) {
			// Given incompatible producer policy and an independently observed destination port.
			destination := &countedDestination{}
			clients := &mockClients{}

			// When constructing the sink.
			sink, err := NewSink(clients, destination, sinkEndpoint(t, properties), config.Defaults("home").Worker, core.RealClock{}, quietLogger())

			// Then construction fails before inspecting the destination or creating a producer.
			if sink != nil || err == nil || destination.calls != 0 || clients.cfg != nil {
				t.Fatal("invalid producer policy performed startup work", sink, err, destination.calls)
			}
		})
	}
}

func TestConsumerPolicyReportsConflictsWithoutLeakingValues(t *testing.T) {
	// Given multiple unsafe overrides including a private group value.
	endpoint := sinkEndpoint(t, map[string]string{"group.id": "private-group-value", "enable.auto.commit": "true"})
	p := plan.Plan{Source: endpoint, Group: "copy-group"}

	// When deriving a bounded manual-commit consumer configuration.
	_, err := consumerConfig(p, config.Defaults("home").Worker)

	// Then the first protected property is deterministic and its value stays private.
	if err == nil || err.Error() != `protected Kafka property "group.id" cannot be overridden` {
		t.Fatal("wrong conflict diagnostic", err)
	}
}

func TestConsumerKeepsDefaultRebalanceAcknowledgement(t *testing.T) {
	// Given an override that disables default acknowledgements after callback errors.
	p := plan.Plan{Source: sinkEndpoint(t, map[string]string{"go.application.rebalance.enable": "true"}), Group: "copy-group"}
	// When preparing the consumer configuration.
	_, err := consumerConfig(p, config.Defaults("home").Worker)
	// Then it fails before construction instead of allowing shutdown to hang.
	if err == nil || err.Error() != `protected Kafka property "go.application.rebalance.enable" cannot be overridden` {
		t.Fatal("unsafe rebalance mode accepted", err)
	}
}

func TestUnusedProduceChannelRespectsWorkerRecordLimit(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  int
	}{{"", 8}, {"4", 4}, {"9", 0}} {
		t.Run("capacity-"+tc.value, func(t *testing.T) {
			// Given a binding-allocated channel, even though direct Produce does not use it.
			properties := map[string]string{}
			if tc.value != "" {
				properties["go.produce.channel.size"] = tc.value
			}
			limits := config.Defaults("home").Worker
			limits.RecordCap = 8
			// When preparing producer configuration.
			cfg, err := producerConfig(sinkEndpoint(t, properties), limits)
			// Then default and explicit capacities stay bounded before any allocation.
			if tc.want == 0 {
				if err == nil || cfg != nil {
					t.Fatal("oversized produce channel accepted", cfg, err)
				}
			} else if err != nil || cfg["go.produce.channel.size"] != tc.want {
				t.Fatal("wrong produce channel bound", cfg, err)
			}
		})
	}
}

func TestConsumerFetchQueueOverrideRespectsByteBudget(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  string
	}{{"", "2"}, {"1", "1"}, {"2", "2"}, {"3", ""}, {"0", ""}, {"-1", ""}, {"abc", ""}} {
		t.Run(tc.value, func(t *testing.T) {
			// Given a 1,500-byte worker budget, rounded up to a 2 KiB fetch queue ceiling.
			overrides := map[string]string{}
			if tc.value != "" {
				overrides["queued.max.messages.kbytes"] = tc.value
			}
			p := plan.Plan{Source: sinkEndpoint(t, overrides), Group: "copy-group"}
			limits := config.Defaults("home").Worker
			limits.ByteCap = 1500

			// When deriving the native consumer queue size.
			properties, err := consumerConfig(p, limits)

			// Then defaults and valid overrides respect the bound; unsafe capacity is rejected.
			if tc.want != "" {
				if err != nil || properties["queued.max.messages.kbytes"] != tc.want {
					t.Fatal("wrong fetch queue bound", properties, err)
				}
			} else {
				var conflict *Conflict
				if properties != nil || !errors.As(err, &conflict) {
					t.Fatal("unsafe fetch queue accepted", properties, err)
				}
			}
		})
	}
}
