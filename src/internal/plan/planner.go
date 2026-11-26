// Package plan validates statements without accessing Kafka or source files.
package plan

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"

	"github.com/pagrus7/kaferry/src/internal/sql"
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
	if err = plan.Destination.destinationOptions(statement.Destination.Options); err != nil {
		return Plan{}, err
	}
	if err = applyOptions(statement.Source.Options, sql.SourceSettings, plan.applyOption); err != nil {
		return Plan{}, err
	}
	if err = applyOptions(statement.Options, sql.CopySettings, plan.applyOption); err != nil {
		return Plan{}, err
	}
	if err = plan.resolveEnd(statement); err != nil {
		return Plan{}, err
	}
	if err = plan.resolveStartRule(statement); err != nil {
		return Plan{}, err
	}

	return plan, nil
}

func (p *Planner) connections(inline []sql.Connection, profiles Profiles) (Profiles, error) {
	connections := make(Profiles, len(profiles)+len(inline))
	for alias, properties := range profiles {
		if !validAlias(alias) {
			return nil, fmt.Errorf("configuration: connection aliases must start with an ASCII letter or underscore and contain only ASCII letters, digits and underscores")
		}
		connections[alias] = maps.Clone(properties)
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
		if err := p.inlineProperties(values, connection.Properties); err != nil {
			return nil, err
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

var ErrFingerprintMismatch = errors.New("query fingerprint mismatch")

// Fingerprint receives verified Kafka identities at startup. File identities stay in
// progress metadata; moving a file must not change the query identity.
func (p Plan) Fingerprint(sourceClusterID, destinationClusterID, destinationTopicID string) (string, error) {
	if err := p.validateFingerprintDestination(destinationTopicID); err != nil {
		return "", err
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
	canonical := fingerprintQuery{
		Version: 4, Source: source, Destination: destination, DestinationTopicID: destinationTopicID,
		Window: p.Window, Offsets: p.Offsets, PartitionOffsets: p.PartitionOffsets, Disorder: p.MaxOutOfOrderMS,
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

type fingerprintEndpoint struct {
	Kind      string
	Format    string `json:",omitempty"`
	ClusterID string
	Topic     string
}

func endpointIdentity(endpoint Endpoint, clusterID string) (fingerprintEndpoint, error) {
	if endpoint.Kind == "ARCHIVE" {
		// FILE is the fixed wire identifier in saved query fingerprints.
		return fingerprintEndpoint{Kind: "FILE", Format: endpoint.Format}, nil
	}
	if clusterID == "" {
		return fingerprintEndpoint{}, fmt.Errorf("kafka fingerprint requires verified cluster IDs")
	}
	return fingerprintEndpoint{Kind: "TOPIC", ClusterID: clusterID, Topic: endpoint.Name}, nil
}

type fingerprintQuery struct {
	Version            int
	Source             fingerprintEndpoint
	Destination        fingerprintEndpoint
	Window             WindowSpec
	Disorder           *int64                 `json:",omitempty"`
	Offsets            *OffsetRange           `json:",omitempty"`
	PartitionOffsets   []PartitionOffsetRange `json:",omitempty"`
	Partitioning       string                 `json:",omitempty"`
	Partitions         *PartitionFilter       `json:",omitempty"`
	DestinationTopicID string                 `json:",omitempty"`
}

func (p Plan) validateFingerprintDestination(destinationTopicID string) error {
	if p.Destination.Kind == "TOPIC" && destinationTopicID == "" {
		return fmt.Errorf("kafka fingerprint requires a verified destination topic ID")
	}
	if p.Destination.Kind == "ARCHIVE" && destinationTopicID != "" {
		return fmt.Errorf("archive destination cannot have a Kafka topic ID")
	}
	if p.Destination.Partitioning != "" && p.Destination.Partitioning != Preserve && p.Destination.Partitioning != Key {
		return fmt.Errorf("PARTITIONING must be PRESERVE or KEY")
	}

	return nil
}

func (p *Planner) inlineProperties(values map[string]string, properties []sql.Property) error {
	keys := make(map[string]sql.Position)
	for _, property := range properties {
		if property.Name == "" {
			return sql.At(property.Position, fmt.Errorf("inline connection property name must not be empty"))
		}
		if first, exists := keys[property.Name]; exists {
			return sql.At(property.Position, fmt.Errorf("inline connection property repeats; specify it once per CONNECTION%s", sql.Related(first, "first declaration")))
		}
		keys[property.Name] = property.Position
		value := property.Value
		if property.Environment {
			if p.environment == nil {
				return fmt.Errorf("environment lookup is unavailable")
			}
			var ok bool
			value, ok = p.environment.LookupEnv(property.Value)
			if !ok {
				return sql.At(property.ValuePosition, fmt.Errorf("ENV(...) variable is unset; set it before running"))
			}
		}
		values[property.Name] = value
	}
	return nil
}

func (p *Plan) resolveEnd(s sql.Statement) error {
	if p.Source.Kind != "TOPIC" {
		return nil
	}
	if p.Follow {
		if p.Window.Upper != nil || p.HasOffsetEnd() {
			return sql.At(optionPosition(s.Source.Options, "FOLLOW", s.WherePosition), fmt.Errorf("FOLLOW = ON cannot have an upper bound; remove FOLLOW or the upper bound"))
		}
		return nil
	}
	if p.Start != "" {
		return sql.At(optionPosition(s.Source.Options, "START", s.Source.OptionsPosition), fmt.Errorf("START requires FOLLOW = ON; remove START or add FOLLOW = ON"))
	}
	if p.TimestampStopEnabled() && p.Window.Upper != nil && !p.Window.Upper.Now {
		if _, err := checkedAdd(p.Window.Upper.LiteralMS, p.DisorderMS()); err != nil {
			return sql.At(optionPosition(s.Source.Options, "MAX_OUT_OF_ORDER", s.WherePosition), fmt.Errorf("upper bound plus MAX_OUT_OF_ORDER: %w", err))
		}
	}
	if !p.HasOffsetEnd() {
		if p.Offsets == nil {
			p.Offsets = &OffsetRange{}
		}
		p.Offsets.Latest = true
	}
	return nil
}

func (p *Plan) resolveStartRule(statement sql.Statement) error {
	if p.Source.Kind == "TOPIC" {
		if len(statement.Bounds) == 0 && p.Start == "" {
			p.Start = "EARLIEST"
		}
		if len(statement.Bounds) > 0 && p.Start != "" {
			return sql.At(optionPosition(statement.Source.Options, "START", statement.Source.OptionsPosition), fmt.Errorf("START cannot be combined with timestamp bounds in WHERE; remove START"))
		}
	} else if p.Start != "" {
		return sql.At(optionPosition(statement.Source.Options, "START", statement.Source.OptionsPosition), fmt.Errorf("START requires a topic source; remove START for ARCHIVE sources"))
	}
	return nil
}
