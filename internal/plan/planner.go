// Package plan validates statements without accessing Kafka or source files.
package plan

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/pagrus7/kaferry/internal/sql"
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
	plan.Destination, err = endpoint(statement.Destination, connections, statement.Connections)
	if err != nil {
		return Plan{}, sql.At(statement.Destination.Position, fmt.Errorf("INTO endpoint: %w", err))
	}
	plan.Source, err = endpoint(statement.Source, connections, statement.Connections)
	if err != nil {
		return Plan{}, sql.At(statement.Source.Position, fmt.Errorf("FROM endpoint: %w", err))
	}
	plan.Window, err = normalize(statement.Bounds)
	if err != nil {
		return Plan{}, sql.At(statement.WherePosition, err)
	}
	if err = plan.options(statement.Options, statement.OptionsPosition); err != nil {
		return Plan{}, err
	}
	if hi := plan.Window.Upper; hi != nil && !hi.Now {
		if _, err := checkedAdd(hi.LiteralMS, plan.CutoffMS); err != nil {
			return Plan{}, sql.At(optionPosition(statement.Options, "CUTOFF", statement.WherePosition), fmt.Errorf("upper bound plus CUTOFF: %w", err))
		}
	}
	if plan.Source.Kind == "TOPIC" {
		if len(statement.Bounds) == 0 && plan.Start == "" {
			return Plan{}, sql.At(statement.OptionsPosition, fmt.Errorf("topic sources require WHERE or START; add START = EARLIEST or START = LATEST to OPTIONS"))
		}
		if len(statement.Bounds) > 0 && plan.Start != "" {
			return Plan{}, sql.At(optionPosition(statement.Options, "START", statement.OptionsPosition), fmt.Errorf("START cannot be combined with WHERE; remove START"))
		}
	} else if plan.Start != "" {
		return Plan{}, sql.At(optionPosition(statement.Options, "START", statement.OptionsPosition), fmt.Errorf("START requires a topic source; remove START for FILE sources"))
	}
	return plan, nil
}

func (p *Planner) connections(inline []sql.Connection, profiles Profiles) (Profiles, error) {
	connections := make(Profiles, len(profiles)+len(inline))
	for alias, properties := range profiles {
		if !validAlias(alias) {
			return nil, fmt.Errorf("configuration: connection aliases must start with an ASCII letter or underscore and contain only ASCII letters, digits and underscores")
		}
		copy := make(map[string]string, len(properties))
		for key, value := range properties {
			copy[key] = value
		}
		connections[alias] = copy
	}
	seen := make(map[string]sql.Position)
	for _, connection := range inline {
		if !validAlias(connection.Alias) {
			return nil, sql.At(connection.Position, fmt.Errorf("invalid connection alias; use ASCII letters, digits and underscores, starting with a letter or underscore"))
		}
		if first, exists := seen[connection.Alias]; exists {
			return nil, sql.At(connection.Position, fmt.Errorf("connection alias repeats; first declaration%s", sql.Reference(first)))
		}
		seen[connection.Alias] = connection.Position
		values := connections[connection.Alias]
		if values == nil {
			values = make(map[string]string)
			connections[connection.Alias] = values
		}
		keys := make(map[string]sql.Position)
		for _, property := range connection.Properties {
			if property.Name == "" {
				return nil, sql.At(property.Position, fmt.Errorf("inline connection property name must not be empty"))
			}
			if first, exists := keys[property.Name]; exists {
				return nil, sql.At(property.Position, fmt.Errorf("inline connection property repeats; first declaration%s", sql.Reference(first)))
			}
			keys[property.Name] = property.Position
			value := property.Value
			if property.Environment {
				if p.environment == nil {
					return nil, fmt.Errorf("environment lookup is unavailable")
				}
				var ok bool
				value, ok = p.environment.LookupEnv(property.Value)
				if !ok {
					return nil, sql.At(property.ValuePosition, fmt.Errorf("ENV(...) variable is unset; set it before running"))
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
			return identity{}, fmt.Errorf("kafka fingerprint requires verified cluster IDs")
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
