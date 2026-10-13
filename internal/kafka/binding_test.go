package kafka

import (
	"strings"
	"testing"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pagrus7/kaferry/internal/config"
	"github.com/pagrus7/kaferry/internal/plan"
)

func TestSharedBindingOptionsConstructRealClients(t *testing.T) {
	for _, client := range []clientKind{adminClient, consumerClient, producerClient} {
		t.Run(string(client), func(t *testing.T) {
			// Given one shared profile containing producer and consumer Go options.
			endpoint := sinkEndpoint(t, map[string]string{
				"bootstrap.servers": "127.0.0.1:1", "log_level": "0",
				"go.events.channel.size": "32", "go.events.channel.enable": "false",
				"go.application.rebalance.enable": "false", "go.delivery.reports": "true",
				"go.batch.producer": "false", "go.delivery.report.fields": "none",
				"go.produce.channel.size": "16", "go.logs.channel.enable": "false",
				"delivery.report.only.error": "false",
			})
			limits := config.Defaults("home").Worker
			limits.RecordCap = 32
			var cfg k.ConfigMap
			var err error
			switch client {
			case adminClient:
				cfg, err = clientConfig(endpoint.Properties, client)
			case consumerClient:
				cfg, err = consumerConfig(plan.Plan{Source: endpoint, Group: "binding-test"}, limits)
			case producerClient:
				cfg, err = producerConfig(endpoint, limits)
			}
			if err != nil {
				t.Fatal(err)
			}

			// When constructing the actual pinned Go binding without a broker.
			native := NewClients()
			switch client {
			case adminClient:
				c, createErr := native.Admin(cfg)
				err = createErr
				if createErr == nil {
					c.Close()
				}
			case consumerClient:
				c, createErr := native.Consumer(cfg)
				err = createErr
				if createErr == nil {
					_ = c.Close()
				}
			case producerClient:
				c, createErr := native.Producer(cfg)
				err = createErr
				if createErr == nil {
					c.Close()
				}
			}

			// Then role filtering and Go value types satisfy the real constructors.
			if err != nil {
				t.Fatal("shared profile rejected by native binding", err)
			}
		})
	}
}

func TestBindingDiagnosticsRejectTyposAndNeverEchoValues(t *testing.T) {
	for _, tc := range []struct{ key, value, want string }{
		{"go.events.channel.szie", "private-option-secret", "unsupported Go client property"},
		{"go.events.channel.size", "private-option-secret", "positive integer"},
		{"go.events.channel.size", "0", "positive integer"},
		{"go.delivery.reports", "private-option-secret", "boolean"},
		{"go.logs.channel.enable", "true", "protected Kafka property"},
		{"go.logs.channel", "private-option-secret", "unsupported Go client property"},
	} {
		t.Run(tc.key+"-"+tc.want, func(t *testing.T) {
			// Given a typo, invalid Go value or unsupported logging redirection.
			endpoint := sinkEndpoint(t, map[string]string{tc.key: tc.value})
			// When preparing producer configuration.
			_, err := clientConfig(endpoint.Properties, producerClient)
			// Then diagnostics name the key and requirement, without secret values.
			if err == nil || !strings.Contains(err.Error(), tc.key) || !strings.Contains(err.Error(), tc.want) || strings.Contains(err.Error(), "private-option-secret") {
				t.Fatal("unsafe or unhelpful configuration error", err)
			}
		})
	}
	// Given an unknown native property rather than a recognized Go option.
	endpoint := sinkEndpoint(t, map[string]string{"misspelled.native.property": "private-option-secret", "log_level": "0"})
	cfg, err := clientConfig(endpoint.Properties, adminClient)
	if err != nil {
		t.Fatal(err)
	}
	// When the real admin validates it, then it still fails instead of being dropped.
	c, err := NewClients().Admin(cfg)
	if err == nil {
		c.Close()
	}
	if err == nil {
		t.Fatal("unknown native property silently discarded")
	}
}
