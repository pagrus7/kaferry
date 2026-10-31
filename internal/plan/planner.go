// Package plan validates statements without accessing Kafka or source files.
package plan

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/pagrus7/kaferry/internal/sql"
)

type Planner struct{ environment Environment }

func NewPlanner(environment Environment) *Planner { return &Planner{environment: environment} }

func (p *Planner) Build(statement sql.Statement, profiles Profiles) (Plan, error) {
	connections, err := p.connections(statement.Connections, profiles)
	if err != nil {
		return Plan{}, err
	}
	plan := Plan{Group: "ONE_OFF", OneOff: true, Policies: Policies{"FAIL", "FAIL", "FAIL"}}
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
	plan.Partitions, err = partitionFilter(statement.Partitions)
	if err != nil {
		return Plan{}, err
	}
	if err = plan.offsetCriteria(statement); err != nil {
		return Plan{}, err
	}
	if plan.Destination.Kind == "TOPIC" {
		plan.Destination.Partitioning = Preserve
	}
	if plan.Source.Kind == "TOPIC" && plan.Window.Upper != nil && !plan.HasOffsetEnd() {
		plan.MaxStalenessMS = 300000
	}
	if err = plan.Destination.destinationOptions(statement.Destination.Options); err != nil {
		return Plan{}, err
	}
	if err = applyOptions(statement.Source.Options, sql.SourceSettings, plan.applyOption); err != nil {
		return Plan{}, err
	}
	if err = applyOptions(statement.Options, sql.CopySettings, plan.applyOption); err != nil {
		return Plan{}, err
	}
	if hi := plan.Window.Upper; hi != nil && !hi.Now {
		if plan.MaxStalenessMS >= 0 {
			if _, err := checkedAdd(hi.LiteralMS, plan.MaxStalenessMS); err != nil {
				return Plan{}, sql.At(optionPosition(statement.Source.Options, "MAX_STALENESS", statement.WherePosition), fmt.Errorf("upper bound plus MAX_STALENESS: %w", err))
			}
		}
		if plan.MaxOutOfOrderMS != nil && *plan.MaxOutOfOrderMS >= 0 {
			if _, err := checkedAdd(hi.LiteralMS, *plan.MaxOutOfOrderMS); err != nil {
				return Plan{}, sql.At(optionPosition(statement.Source.Options, "MAX_OUT_OF_ORDER", statement.WherePosition), fmt.Errorf("upper bound plus MAX_OUT_OF_ORDER: %w", err))
			}
		}
	}

	if plan.Source.Kind == "TOPIC" {
		if len(statement.Bounds) == 0 && plan.Start == "" {
			plan.Start = "EARLIEST"
		}
		if len(statement.Bounds) > 0 && plan.Start != "" {
			return Plan{}, sql.At(optionPosition(statement.Source.Options, "START", statement.Source.OptionsPosition), fmt.Errorf("START cannot be combined with timestamp bounds in WHERE; remove START"))
		}
	} else if plan.Start != "" {
		return Plan{}, sql.At(optionPosition(statement.Source.Options, "START", statement.Source.OptionsPosition), fmt.Errorf("START requires a topic source; remove START for ARCHIVE sources"))
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
		maps.Copy(copy, properties)
		connections[alias] = copy
	}
	seen := make(map[string]sql.Position)
	for _, connection := range inline {
		if !validAlias(connection.Alias) {
			return nil, sql.At(connection.Position, fmt.Errorf("invalid connection alias; use ASCII letters, digits and underscores, starting with a letter or underscore"))
		}
		if first, exists := seen[connection.Alias]; exists {
			return nil, sql.At(connection.Position, fmt.Errorf("connection alias repeats; declare each alias once%s", sql.Related(first, "first declaration")))
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
				return nil, sql.At(property.Position, fmt.Errorf("inline connection property repeats; specify it once per CONNECTION%s", sql.Related(first, "first declaration")))
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
	if p.Destination.Partitioning != "" && p.Destination.Partitioning != Preserve && p.Destination.Partitioning != Key {
		return "", fmt.Errorf("PARTITIONING must be PRESERVE or KEY")
	}
	type identity struct {
		Kind      string
		Format    string `json:",omitempty"`
		ClusterID string
		Topic     string
	}
	endpointIdentity := func(endpoint Endpoint, clusterID string) (identity, error) {
		if endpoint.Kind == "ARCHIVE" {
			// FILE is the fixed wire identifier in saved query fingerprints.
			return identity{Kind: "FILE", Format: endpoint.Format}, nil
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
		// CutoffMS is the fixed identity field name for unbounded Kafka and archive queries.
		Disorder         *int64                 `json:",omitempty"`
		Offsets          *OffsetRange           `json:",omitempty"`
		PartitionOffsets []PartitionOffsetRange `json:",omitempty"`
		MaxStalenessMS   int64                  `json:"CutoffMS"`
		Partitioning     string                 `json:",omitempty"`
		Partitions       *PartitionFilter       `json:",omitempty"`
	}{Version: 1, Source: source, Destination: destination, Window: p.Window, MaxStalenessMS: p.MaxStalenessMS}
	if p.Source.Kind == "TOPIC" && p.Window.Upper != nil {
		canonical.Version = 2
	}
	canonical.Offsets, canonical.PartitionOffsets = p.Offsets, p.PartitionOffsets
	// The wire encoding omits disorder when it equals the topic staleness
	// allowance (or OFF for archives and offset ends). Keep that encoding for
	// explicit settings so existing progress retains its identity.
	wireDisorder := int64(-1)
	if p.Source.Kind == "TOPIC" && !p.HasOffsetEnd() {
		wireDisorder = p.MaxStalenessMS
	}
	if disorder := p.DisorderMS(); p.Window.Upper != nil && disorder != wireDisorder {
		canonical.Disorder = &disorder
	}
	if canonical.Disorder != nil || p.MaxStalenessMS < 0 || p.Offsets != nil || len(p.PartitionOffsets) > 0 {
		canonical.Version = 3
	}
	if p.Destination.PreservesPartitions() {
		canonical.Partitioning = Preserve
	}
	canonical.Partitions = p.Partitions
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}
