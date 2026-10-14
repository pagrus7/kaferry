package kafka

import (
	"maps"
	"slices"
	"strconv"
	"strings"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pagrus7/kaferry/internal/plan"
)

type clientKind string

const (
	adminClient    clientKind = "admin"
	consumerClient clientKind = "consumer"
	producerClient clientKind = "producer"
)

// The Go binding extracts these before passing native properties to librdkafka.
// Profiles can be shared by clients; each receives only its binding options.
var bindingProperties = map[string]struct {
	kind    string
	clients []clientKind
}{
	"go.events.channel.size":          {"int", []clientKind{consumerClient, producerClient}},
	"go.events.channel.enable":        {"bool", []clientKind{consumerClient}},
	"go.application.rebalance.enable": {"bool", []clientKind{consumerClient}},
	"go.delivery.reports":             {"bool", []clientKind{producerClient}},
	"go.batch.producer":               {"bool", []clientKind{producerClient}},
	"go.delivery.report.fields":       {"string", []clientKind{producerClient}},
	"go.produce.channel.size":         {"int", []clientKind{producerClient}},
	"delivery.report.only.error":      {"bool", []clientKind{producerClient}},
	"go.logs.channel.enable":          {"bool", []clientKind{adminClient, consumerClient, producerClient}},
}

func clientConfig(properties plan.Properties, client clientKind) (k.ConfigMap, error) {
	cfg := native(properties)
	for _, key := range slices.Sorted(maps.Keys(cfg)) {
		option, known := bindingProperties[key]
		if !known {
			if strings.HasPrefix(key, "go.") {
				return nil, conflict("unsupported Go client property %q", key)
			}
			continue
		}
		if !slices.Contains(option.clients, client) {
			delete(cfg, key)
			continue
		}
		value, err := bindingValue(properties.Get(key), option.kind)
		if err != nil {
			hint := "boolean"
			if option.kind == "int" {
				hint = "positive integer"
			}
			return nil, conflict("Go client property %q requires a %s", key, hint)
		}
		cfg[key] = value
	}
	// The CLI consumes its JSON logger and native stderr, not a Go logs channel.
	if err := protectConfig(cfg, []protectedProperty{{"go.logs.channel.enable", false}}); err != nil {
		return nil, err
	}
	return cfg, nil
}

func bindingValue(value, kind string) (any, error) {
	switch kind {
	case "bool":
		return strconv.ParseBool(value)
	case "int":
		n, err := strconv.Atoi(value)
		if err == nil && n <= 0 {
			return nil, strconv.ErrRange
		}
		return n, err
	default:
		return value, nil
	}
}
