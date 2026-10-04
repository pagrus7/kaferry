// Package plan validates statements without accessing Kafka or source files.
package plan

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"kafka-fwd2/internal/sql"
)

type Environment interface{ LookupEnv(string) (string, bool) }
type Builder interface {
	Build(sql.Statement, Profiles) (Plan, error)
}

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
