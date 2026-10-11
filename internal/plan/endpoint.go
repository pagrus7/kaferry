package plan

import (
	"fmt"
	"strings"

	"github.com/pagrus7/kaferry/internal/sql"
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
	if !exists {
		if e.Alias == "default" {
			return Endpoint{}, fmt.Errorf("default connection is not defined; add [connections.default] or specify topic@alias")
		}
		return Endpoint{}, fmt.Errorf("connection alias is not defined; add it with WITH CONNECTION or TOML [connections.alias]")
	}
	if values["bootstrap.servers"] == "" {
		return Endpoint{}, fmt.Errorf("connection requires bootstrap.servers; set it in WITH CONNECTION or TOML")
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
		return "", "", fmt.Errorf("invalid topic endpoint; use a nonempty topic and an alias starting with a letter or underscore")
	}
	if !validTopicName(name) {
		return "", "", fmt.Errorf("invalid Kafka topic name; use 1–249 ASCII letters, digits, dots, underscores or hyphens, excluding names made of one or two dots")
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
