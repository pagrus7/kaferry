package plan

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/pagrus7/kaferry/internal/sql"
)

func endpoint(source sql.Endpoint, profiles Profiles, inline []sql.Connection) (Endpoint, error) {
	e := Endpoint{Kind: source.Kind, Name: source.Name, Format: source.Format}
	if e.Name == "" {
		return Endpoint{}, fmt.Errorf("endpoint name is empty")
	}
	switch e.Kind {
	case "FILE":
		if e.Format != "avro" {
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
			return Endpoint{}, fmt.Errorf("default connection is not defined; add WITH CONNECTION default or TOML [connections.default]")
		}
		return Endpoint{}, fmt.Errorf("connection alias is not defined; add it with WITH CONNECTION or TOML [connections.alias]")
	}
	bootstrap, present := values["bootstrap.servers"]
	if !present {
		return Endpoint{}, fmt.Errorf("connection requires bootstrap.servers; add it to %s", connectionOrigin(e.Alias, inline))
	}
	if bootstrap == "" {
		return Endpoint{}, emptyBootstrapError(e.Alias, inline)
	}
	e.Properties = Properties{values: values}
	return e, nil
}

func connectionOrigin(alias string, inline []sql.Connection) string {
	for _, connection := range inline {
		if connection.Alias == alias {
			return "the inline connection" + sql.Reference(connection.Position)
		}
	}
	return "the configured connection profile"
}

func emptyBootstrapError(alias string, inline []sql.Connection) error {
	for _, connection := range inline {
		if connection.Alias != alias {
			continue
		}
		for _, property := range connection.Properties {
			if property.Name != "bootstrap.servers" {
				continue
			}
			if property.Environment {
				return fmt.Errorf("ENV(...) value for bootstrap.servers is empty; set the variable referenced%s to a nonempty value", sql.Reference(property.ValuePosition))
			}
			return fmt.Errorf("bootstrap.servers is empty; set a nonempty literal value%s", sql.Reference(property.ValuePosition))
		}
	}
	return fmt.Errorf("bootstrap.servers is empty in the configured connection profile; set a nonempty value there")
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
		return "", "", fmt.Errorf("invalid topic endpoint; use a nonempty topic and an alias starting with an ASCII letter or underscore, containing only ASCII letters, digits and underscores")
	}
	if !validTopicName(name) {
		return "", "", fmt.Errorf("invalid Kafka topic name; use 1-249 ASCII letters, digits, dots, underscores or hyphens, excluding names made of one or two dots")
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

func (e *Endpoint) destinationOptions(options []sql.Option) error {
	return applyOptions(options, sql.DestinationSettings, func(o sql.Option) error {
		if o.Name == "PARTITIONING" {
			if e.Kind != "TOPIC" {
				return fmt.Errorf("PARTITIONING requires a topic destination")
			}
			if o.Value != Preserve && o.Value != Key {
				return fmt.Errorf("PARTITIONING must be PRESERVE or KEY")
			}
			e.Partitioning = o.Value
			return nil
		}
		if e.Kind != "FILE" {
			return fmt.Errorf("%s requires a FILE destination", o.Name)
		}
		return e.rotationOption(o)
	})
}

func (e *Endpoint) rotationOption(o sql.Option) error {
	if o.Name == "ROTATE_AFTER_SIZE" {
		text := o.Value
		multiplier := int64(1)
		if len(text) > 0 {
			switch text[len(text)-1] {
			case 'k':
				multiplier, text = 1<<10, text[:len(text)-1]
			case 'm':
				multiplier, text = 1<<20, text[:len(text)-1]
			case 'g':
				multiplier, text = 1<<30, text[:len(text)-1]
			}
		}
		n, err := strconv.ParseInt(text, 10, 64)
		if err != nil || n <= 0 || n > math.MaxInt64/multiplier {
			return fmt.Errorf("ROTATE_AFTER_SIZE requires positive bytes, optionally suffixed k, m or g")
		}
		e.RotateAfterSize = n * multiplier
		return nil
	}
	if o.Interval == nil {
		return fmt.Errorf("ROTATE_AFTER_AGE requires a positive interval")
	}
	n, err := intervalMS(*o.Interval)
	if err != nil || n <= 0 || n > math.MaxInt64/1000000 {
		return fmt.Errorf("ROTATE_AFTER_AGE requires a positive interval within the duration limit")
	}
	e.RotateAfterAgeMS = n
	return nil
}
