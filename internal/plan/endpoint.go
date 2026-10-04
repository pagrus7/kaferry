package plan

import (
	"fmt"
	"strings"

	"kafka-fwd2/internal/sql"
)

func endpoint(source sql.Endpoint, profiles Profiles) (Endpoint, error) {
	e := Endpoint{Kind: source.Kind, Name: source.Name, Format: source.Format}
	if e.Name == "" {
		return Endpoint{}, fmt.Errorf("endpoint name is empty")
	}
	switch e.Kind {
	case "FILE":
		if e.Format != "cbor" {
			return Endpoint{}, fmt.Errorf("unsupported file format")
		}
		return e, nil
	case "TOPIC":
		name, alias, err := topicEndpoint(e.Name)
		if err != nil {
			return Endpoint{}, err
		}
		e.Name, e.Alias = name, alias
	default:
		return Endpoint{}, fmt.Errorf("unsupported endpoint kind")
	}
	values, exists := profiles[e.Alias]
	if !exists || values["bootstrap.servers"] == "" {
		return Endpoint{}, fmt.Errorf("connection %q requires bootstrap.servers", e.Alias)
	}
	e.Properties = Properties{values: values}
	return e, nil
}

func topicEndpoint(input string) (string, string, error) {
	name, alias := input, "default"
	if strings.Contains(input, "@") {
		if strings.Count(input, "@") != 1 {
			return "", "", fmt.Errorf("topic endpoint must be topic@alias")
		}
		name, alias, _ = strings.Cut(input, "@")
	}
	if name == "" || !validAlias(alias) {
		return "", "", fmt.Errorf("invalid topic endpoint")
	}
	if !validTopicName(name) {
		return "", "", fmt.Errorf("invalid Kafka topic name")
	}
	return name, alias, nil
}

func validTopicName(name string) bool {
	if len(name) > 249 || name == "." || name == ".." {
		return false
	}
	for _, c := range name {
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' || c == '.' {
			continue
		}
		return false
	}
	return true
}
