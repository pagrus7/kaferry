// Package plan validates statements without accessing Kafka or source files.
package plan

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"kafka-fwd2/internal/sql"
)

type Environment interface{ LookupEnv(string) (string, bool) }

type Planner struct{ environment Environment }

func NewPlanner(environment Environment) *Planner { return &Planner{environment: environment} }

type Profiles map[string]map[string]string

type Properties struct{ values map[string]string }

func (p Properties) Get(name string) string { return p.values[name] }
func (p Properties) Values() map[string]string {
	values := make(map[string]string, len(p.values))
	for key, value := range p.values {
		values[key] = value
	}
	return values
}

// Do not expose credentials through fmt, JSON, or error reporting.
func (p Properties) String() string   { return "[connection properties redacted]" }
func (p Properties) GoString() string { return p.String() }

type Endpoint struct {
	Kind       string
	Name       string
	Format     string
	Alias      string
	Properties Properties
}

type Policies struct {
	MissingTimestamp  string
	RejectedTimestamp string
	OtherRecord       string
}

type Plan struct {
	Source      Endpoint
	Destination Endpoint
	Group       string
	OneOff      bool
	Start       string
	Window      WindowSpec
	CutoffMS    int64
	Policies    Policies
}

func (p *Planner) Build(statement sql.Statement, profiles Profiles) (Plan, error) {
	connections, err := p.connections(statement.Connections, profiles)
	if err != nil {
		return Plan{}, err
	}
	var plan Plan
	plan.Source, err = endpoint(statement.Source, connections)
	if err != nil {
		return Plan{}, err
	}
	plan.Destination, err = endpoint(statement.Destination, connections)
	if err != nil {
		return Plan{}, err
	}
	plan.Window, err = normalize(statement.Bounds)
	if err != nil {
		return Plan{}, err
	}
	if err = plan.options(statement.Options); err != nil {
		return Plan{}, err
	}
	if hi := plan.Window.Upper; hi != nil && !hi.Now {
		if _, err := checkedAdd(hi.LiteralMS, plan.CutoffMS); err != nil {
			return Plan{}, err
		}
	}
	if plan.Source.Kind == "TOPIC" {
		if (len(statement.Bounds) > 0) == (plan.Start != "") {
			return Plan{}, fmt.Errorf("topic sources require exactly one of WHERE and START")
		}
	} else if plan.Start != "" {
		return Plan{}, fmt.Errorf("START requires a topic source")
	}
	return plan, nil
}

func (p *Planner) connections(inline []sql.Connection, profiles Profiles) (Profiles, error) {
	connections := make(Profiles, len(profiles)+len(inline))
	for alias, properties := range profiles {
		if !validAlias(alias) {
			return nil, fmt.Errorf("invalid connection alias")
		}
		copy := make(map[string]string, len(properties))
		for key, value := range properties {
			copy[key] = value
		}
		connections[alias] = copy
	}
	seen := make(map[string]bool)
	for _, connection := range inline {
		if !validAlias(connection.Alias) || seen[connection.Alias] {
			return nil, fmt.Errorf("invalid or repeated connection alias")
		}
		seen[connection.Alias] = true
		values := connections[connection.Alias]
		if values == nil {
			values = make(map[string]string)
			connections[connection.Alias] = values
		}
		keys := make(map[string]bool)
		for _, property := range connection.Properties {
			if property.Name == "" || keys[property.Name] {
				return nil, fmt.Errorf("empty or repeated inline connection property")
			}
			keys[property.Name] = true
			value := property.Value
			if property.Environment {
				if p.environment == nil {
					return nil, fmt.Errorf("environment lookup is unavailable")
				}
				var ok bool
				value, ok = p.environment.LookupEnv(property.Value)
				if !ok {
					return nil, fmt.Errorf("required environment variable is unset")
				}
			}
			values[property.Name] = value
		}
	}
	return connections, nil
}

func validAlias(alias string) bool {
	if alias == "" {
		return false
	}
	for i, c := range alias {
		if c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || i > 0 && c >= '0' && c <= '9' {
			continue
		}
		return false
	}
	return true
}

func endpoint(source sql.Endpoint, profiles Profiles) (Endpoint, error) {
	e := Endpoint{Kind: source.Kind, Name: source.Name, Format: source.Format}
	if e.Name == "" {
		return Endpoint{}, fmt.Errorf("endpoint name is empty")
	}
	if e.Kind == "FILE" {
		if e.Format != "cbor" {
			return Endpoint{}, fmt.Errorf("unsupported file format")
		}
		return e, nil
	}
	if e.Kind != "TOPIC" {
		return Endpoint{}, fmt.Errorf("unsupported endpoint kind")
	}
	e.Alias = "default"
	if strings.Contains(e.Name, "@") {
		if strings.Count(e.Name, "@") != 1 {
			return Endpoint{}, fmt.Errorf("topic endpoint must be topic@alias")
		}
		e.Name, e.Alias, _ = strings.Cut(e.Name, "@")
	}
	if e.Name == "" || !validAlias(e.Alias) {
		return Endpoint{}, fmt.Errorf("invalid topic endpoint")
	}
	for _, c := range e.Name {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' || c == '.') {
			return Endpoint{}, fmt.Errorf("invalid Kafka topic name")
		}
	}
	if len(e.Name) > 249 || e.Name == "." || e.Name == ".." {
		return Endpoint{}, fmt.Errorf("invalid Kafka topic name")
	}
	values, exists := profiles[e.Alias]
	if !exists || values["bootstrap.servers"] == "" {
		return Endpoint{}, fmt.Errorf("connection %q requires bootstrap.servers", e.Alias)
	}
	e.Properties = Properties{values: values}
	return e, nil
}

func (p *Plan) options(options []sql.Option) error {
	p.Policies = Policies{MissingTimestamp: "FAIL", RejectedTimestamp: "FAIL", OtherRecord: "FAIL"}
	p.CutoffMS = 60000
	seen := make(map[string]bool)
	for _, option := range options {
		if seen[option.Name] {
			return fmt.Errorf("option %s repeats", option.Name)
		}
		seen[option.Name] = true
		switch option.Name {
		case "GROUP":
			p.Group = option.Value
			p.OneOff = !option.Quoted && option.Value == "ONE_OFF"
			if p.Group == "" || strings.TrimSpace(p.Group) == "" {
				return fmt.Errorf("GROUP must not be empty")
			}
		case "START":
			if option.Value != "EARLIEST" && option.Value != "LATEST" {
				return fmt.Errorf("START must be EARLIEST or LATEST")
			}
			p.Start = option.Value
		case "CUTOFF":
			if p.Source.Kind != "TOPIC" || p.Window.Upper == nil {
				return fmt.Errorf("CUTOFF requires a topic source with an upper bound")
			}
			if option.Interval == nil {
				return fmt.Errorf("CUTOFF requires an interval")
			}
			var err error
			p.CutoffMS, err = intervalMS(*option.Interval)
			if err != nil {
				return err
			}
		case "ON_ERROR", "ON_MISSING_TIMESTAMP", "ON_REJECTED_TIMESTAMP":
			if option.Value != "FAIL" && option.Value != "SKIP" {
				return fmt.Errorf("error policy must be FAIL or SKIP")
			}
			if option.Name == "ON_MISSING_TIMESTAMP" && p.Window.Lower == nil && p.Window.Upper == nil {
				return fmt.Errorf("ON_MISSING_TIMESTAMP requires WHERE")
			}
			if option.Name == "ON_REJECTED_TIMESTAMP" && p.Destination.Kind != "TOPIC" {
				return fmt.Errorf("ON_REJECTED_TIMESTAMP requires a topic destination")
			}
			switch option.Name {
			case "ON_ERROR":
				p.Policies = Policies{option.Value, option.Value, option.Value}
			case "ON_MISSING_TIMESTAMP":
				p.Policies.MissingTimestamp = option.Value
			case "ON_REJECTED_TIMESTAMP":
				p.Policies.RejectedTimestamp = option.Value
			}
		default:
			return fmt.Errorf("unsupported option")
		}
	}
	if !seen["GROUP"] {
		return fmt.Errorf("GROUP is required")
	}
	if seen["ON_ERROR"] && (seen["ON_MISSING_TIMESTAMP"] || seen["ON_REJECTED_TIMESTAMP"]) {
		return fmt.Errorf("ON_ERROR cannot be combined with specific error policies")
	}
	if p.Source.Kind != "TOPIC" || p.Window.Upper == nil {
		p.CutoffMS = 0
	}
	return nil
}

// Fingerprint receives verified cluster IDs at startup. File identities stay in
// progress metadata; moving a file must not change the query identity.
func (p Plan) Fingerprint(sourceClusterID, destinationClusterID string) (string, error) {
	type identity struct {
		Kind      string
		ClusterID string
		Topic     string
	}
	endpointIdentity := func(endpoint Endpoint, clusterID string) (identity, error) {
		if endpoint.Kind == "FILE" {
			return identity{Kind: "FILE"}, nil
		}
		if clusterID == "" {
			return identity{}, fmt.Errorf("Kafka fingerprint requires verified cluster IDs")
		}
		return identity{Kind: "TOPIC", ClusterID: clusterID, Topic: endpoint.Name}, nil
	}
	source, err := endpointIdentity(p.Source, sourceClusterID)
	if err != nil {
		return "", err
	}
	destination, err := endpointIdentity(p.Destination, destinationClusterID)
	if err != nil {
		return "", err
	}
	if source.Kind == "TOPIC" && source == destination {
		return "", fmt.Errorf("source and destination are the same Kafka topic")
	}
	canonical := struct {
		Version     int
		Source      identity
		Destination identity
		Window      WindowSpec
		CutoffMS    int64
	}{1, source, destination, p.Window, p.CutoffMS}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}
