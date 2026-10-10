package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pagrus7/kaferry/internal/plan"
)

type Conflict struct{ Err error }

func (e *Conflict) Error() string               { return e.Err.Error() }
func (e *Conflict) Unwrap() error               { return e.Err }
func (*Conflict) ExitCode() int                 { return 2 }
func conflict(format string, args ...any) error { return &Conflict{Err: fmt.Errorf(format, args...)} }

type Progress struct {
	Fingerprint string       `json:"fp"`
	TopicID     string       `json:"tid"`
	Window      *plan.Window `json:"win,omitempty"`
	FileID      string       `json:"fid,omitempty"`
	Extent      int64        `json:"extent,omitempty"`
}

// The v1 wire prefix stays stable across the Kaferry rename.
func (p Progress) Encode() string { data, _ := json.Marshal(p); return "kfwd2:1;" + string(data) }
func decodeProgress(metadata *string, fp, topicID string, relative bool) (Progress, error) {
	var progress Progress
	if metadata == nil || !strings.HasPrefix(*metadata, "kfwd2:1;") {
		return progress, conflict("foreign or unsupported commit metadata")
	}
	decoder := json.NewDecoder(strings.NewReader(strings.TrimPrefix(*metadata, "kfwd2:1;")))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&progress); err != nil || decoder.Decode(new(any)) != io.EOF {
		return progress, conflict("invalid commit metadata")
	}
	if progress.Fingerprint != fp {
		return progress, conflict("query fingerprint mismatch")
	}
	if progress.TopicID == "" || progress.TopicID != topicID {
		return progress, conflict("source topic identity mismatch: saved=%s current=%s", progress.TopicID, topicID)
	}
	if relative && progress.Window == nil {
		return progress, conflict("NOW() commit is missing its window")
	}
	if progress.Extent < 0 || (progress.FileID == "") != (progress.Extent == 0) {
		return progress, conflict("invalid destination file progress")
	}
	if progress.Window != nil && progress.Window.Lower != nil && progress.Window.Upper != nil && *progress.Window.Lower >= *progress.Window.Upper {
		return progress, conflict("invalid stored time window")
	}
	return progress, nil
}

type Inspector struct {
	admin       AdminClient
	topic       string
	description k.TopicDescription
	log         *slog.Logger
}

func NewInspector(clients Clients, properties plan.Properties, topic string, logger *slog.Logger) (Metadata, error) {
	cfg := native(properties)
	a, err := clients.Admin(cfg)
	if err != nil {
		return nil, conflict("invalid Kafka connection configuration")
	}
	i := &Inspector{admin: a, topic: topic, log: logger}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result, err := a.DescribeTopics(ctx, k.NewTopicCollectionOfTopicNames([]string{topic}))
	if err != nil {
		a.Close()
		return nil, err
	}
	if len(result.TopicDescriptions) != 1 {
		a.Close()
		return nil, conflict("topic %q does not exist or cannot be described", topic)
	}
	if result.TopicDescriptions[0].Error.Code() != k.ErrNoError {
		a.Close()
		return nil, conflict("topic %q does not exist or cannot be described: %v", topic, result.TopicDescriptions[0].Error)
	}
	i.description = result.TopicDescriptions[0]
	if i.description.TopicID.GetMostSignificantBits() == 0 && i.description.TopicID.GetLeastSignificantBits() == 0 {
		a.Close()
		return nil, conflict("broker did not provide a source topic identity")
	}
	return i, nil
}
func (i *Inspector) Close() { i.admin.Close() }
func (i *Inspector) ClusterID() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return i.admin.ClusterID(ctx)
}
func (i *Inspector) TopicID() string     { return i.description.TopicID.String() }
func (i *Inspector) PartitionCount() int { return len(i.description.Partitions) }
func (i *Inspector) Parts() []k.TopicPartition {
	parts := make([]k.TopicPartition, len(i.description.Partitions))
	for n, p := range i.description.Partitions {
		parts[n] = k.TopicPartition{Topic: &i.topic, Partition: int32(p.Partition)}
	}
	return parts
}
func (i *Inspector) CurrentPartitions() ([]k.TopicPartition, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return i.currentPartitions(ctx)
}
func (i *Inspector) currentPartitions(ctx context.Context) ([]k.TopicPartition, error) {
	described, err := i.admin.DescribeTopics(ctx, k.NewTopicCollectionOfTopicNames([]string{i.topic}))
	if err != nil {
		return nil, err
	}
	if len(described.TopicDescriptions) != 1 || described.TopicDescriptions[0].Error.Code() != k.ErrNoError {
		return nil, conflict("source topic cannot be described")
	}
	current := described.TopicDescriptions[0]
	if current.TopicID.String() != i.TopicID() {
		return nil, conflict("source topic identity changed")
	}
	if len(current.Partitions) != len(i.description.Partitions) {
		i.log.Info("source partition count changed", "topic", i.topic, "previous", len(i.description.Partitions), "current", len(current.Partitions))
	}
	i.description = current
	return i.Parts(), nil
}
func (i *Inspector) Progress(group string) ([]k.TopicPartition, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	parts, err := i.currentPartitions(ctx)
	if err != nil {
		return nil, err
	}
	result, err := i.admin.ListConsumerGroupOffsets(ctx, []k.ConsumerGroupTopicPartitions{{Group: group, Partitions: parts}})
	if err != nil {
		return nil, err
	}
	if len(result.ConsumerGroupsTopicPartitions) != 1 {
		return nil, fmt.Errorf("missing group offset result")
	}
	parts = result.ConsumerGroupsTopicPartitions[0].Partitions
	for _, part := range parts {
		if part.Error != nil {
			return nil, part.Error
		}
	}
	return parts, nil
}
func (i *Inspector) ReadProgress(group, fp string, window plan.WindowSpec, cutoff int64) (map[int32]Progress, error) {
	parts, err := i.Progress(group)
	if err != nil {
		return nil, err
	}
	_, result, err := decodeCommitted(parts, fp, i.TopicID(), window, cutoff)
	return result, err
}

// decodeCommitted validates the same persisted contract at startup and assignment.
func decodeCommitted(parts []k.TopicPartition, fp, topicID string, window plan.WindowSpec, cutoff int64) (map[int32]k.TopicPartition, map[int32]Progress, error) {
	positions := make(map[int32]k.TopicPartition)
	progress := make(map[int32]Progress)
	for _, part := range parts {
		if part.Error != nil {
			return nil, nil, part.Error
		}
		if part.Offset < 0 {
			continue
		}
		saved, err := decodeProgress(part.Metadata, fp, topicID, window.Relative())
		if err != nil {
			return nil, nil, err
		}
		if err := validateWindow(saved, window, cutoff); err != nil {
			return nil, nil, err
		}
		positions[part.Partition] = part
		progress[part.Partition] = saved
	}
	return positions, progress, nil
}

func validateWindow(progress Progress, spec plan.WindowSpec, cutoff int64) error {
	if progress.Window != nil {
		if err := spec.ValidateStored(*progress.Window, cutoff); err != nil {
			return conflict("invalid stored time window: %v", err)
		}
	}
	return nil
}
func (i *Inspector) CheckMembers(group string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result, err := i.admin.DescribeConsumerGroups(ctx, []string{group})
	if err != nil {
		return err
	}
	for _, description := range result.ConsumerGroupDescriptions {
		if description.Error.Code() == k.ErrGroupIDNotFound {
			continue
		}
		if description.Error.Code() != k.ErrNoError {
			return description.Error
		}
		for _, member := range description.Members {
			if !strings.HasPrefix(member.ClientID, "kaferry-") && !strings.HasPrefix(member.ClientID, "kafka-fwd2-") {
				return conflict("GROUP has a foreign active member")
			}
			for _, part := range member.Assignment.TopicPartitions {
				if part.Topic == nil || *part.Topic != i.topic {
					return conflict("GROUP has a member assigned to another topic")
				}
			}
		}
	}
	return nil
}

// Offsets lists one offset kind for many partitions in a single request.
func (i *Inspector) Offsets(partitions []int32, spec k.OffsetSpec, readCommitted bool) (map[int32]int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	request := make(map[k.TopicPartition]k.OffsetSpec, len(partitions))
	for _, partition := range partitions {
		request[k.TopicPartition{Topic: &i.topic, Partition: partition}] = spec
	}
	var options []k.ListOffsetsAdminOption
	if readCommitted {
		options = append(options, k.SetAdminIsolationLevel(k.IsolationLevelReadCommitted))
	}
	result, err := i.admin.ListOffsets(ctx, request, options...)
	if err != nil {
		return nil, err
	}
	infos := partitionInfos(result, i.topic)
	offsets := make(map[int32]int64, len(partitions))
	for _, partition := range partitions {
		info, ok := infos[partition]
		if !ok {
			return nil, fmt.Errorf("missing offset result for partition %d", partition)
		}
		if info.Error.Code() != k.ErrNoError {
			return nil, info.Error
		}
		offsets[partition] = int64(info.Offset)
	}
	return offsets, nil
}

// Returned Topic pointers differ from request pointers; compare Kafka identity.
func partitionInfos(result k.ListOffsetsResult, topic string) map[int32]k.ListOffsetsResultInfo {
	infos := make(map[int32]k.ListOffsetsResultInfo, len(result.ResultInfos))
	for key, info := range result.ResultInfos {
		if key.Topic != nil && *key.Topic == topic {
			infos[key.Partition] = info
		}
	}
	return infos
}
func (i *Inspector) Config() map[string]string {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result, err := i.admin.DescribeConfigs(ctx, []k.ConfigResource{{Type: k.ResourceTopic, Name: i.topic}})
	if err != nil || len(result) != 1 || result[0].Error.Code() != k.ErrNoError {
		i.log.Warn("topic configuration unreadable; dependent preflight checks disabled", "topic", i.topic)
		return nil
	}
	values := make(map[string]string)
	for key, entry := range result[0].Config {
		values[key] = entry.Value
	}
	return values
}
func native(properties plan.Properties) k.ConfigMap {
	cfg := make(k.ConfigMap)
	for key, value := range properties.Values() {
		cfg[key] = value
	}
	return cfg
}
