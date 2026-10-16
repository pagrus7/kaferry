package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/plan"
)

type Conflict = kafkaport.Conflict
type Progress = kafkaport.Progress

func conflict(format string, args ...any) error { return &Conflict{Err: fmt.Errorf(format, args...)} }
func decodeProgress(metadata *string, fp, topicID string, relative bool) (Progress, error) {
	return kafkaport.DecodeProgress(metadata, fp, topicID, relative)
}

type Inspector struct {
	admin       AdminClient
	topic       string
	description k.TopicDescription
	topicID     string
	log         *slog.Logger
	timeout     time.Duration // Go deadline per admin call; zero selects inspectTimeout
}

const inspectTimeout = 10 * time.Second

func (i *Inspector) deadline() time.Duration {
	if i.timeout <= 0 {
		return inspectTimeout
	}
	return i.timeout
}
func (i *Inspector) context(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, i.deadline())
}

// Keep coordinator requests alive past the Go deadline until the engine closes the failed handle.
// This mitigates native timeout crashes; see design/client-compatibility.md and native_test.go.
func (i *Inspector) coordinatorTimeout() time.Duration { return 2 * i.deadline() }

func NewInspector(ctx context.Context, clients Clients, properties plan.Properties, topic string, logger *slog.Logger) (*Inspector, error) {
	cfg, err := clientConfig(properties, adminClient)
	if err != nil {
		return nil, err
	}
	a, err := clients.Admin(cfg)
	if err != nil {
		return nil, conflict("invalid Kafka connection configuration")
	}
	i := &Inspector{admin: a, topic: topic, log: logger}
	ctx, cancel := i.context(ctx)
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
	i.topicID = i.description.TopicID.String()
	return i, nil
}
func (i *Inspector) Close() { i.admin.Close() }
func (i *Inspector) clusterID(parent context.Context) (string, error) {
	ctx, cancel := i.context(parent)
	defer cancel()
	return i.admin.ClusterID(ctx)
}
func (i *Inspector) TopicID() string     { return i.topicID }
func (i *Inspector) PartitionCount() int { return len(i.description.Partitions) }
func (i *Inspector) Parts() []k.TopicPartition {
	parts := make([]k.TopicPartition, len(i.description.Partitions))
	for n, p := range i.description.Partitions {
		parts[n] = k.TopicPartition{Topic: &i.topic, Partition: int32(p.Partition)}
	}
	return parts
}
func (i *Inspector) partitions(parent context.Context) ([]k.TopicPartition, error) {
	ctx, cancel := i.context(parent)
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
func (i *Inspector) progress(parent context.Context, group string) ([]k.TopicPartition, error) {
	ctx, cancel := i.context(parent)
	defer cancel()
	parts, err := i.currentPartitions(ctx)
	if err != nil {
		return nil, err
	}
	return i.groupOffsets(ctx, group, parts)
}

// groupOffsets reads the group's commits for parts; after CheckMembers it is the only coordinator request.
func (i *Inspector) groupOffsets(ctx context.Context, group string, parts []k.TopicPartition) ([]k.TopicPartition, error) {
	request := []k.ConsumerGroupTopicPartitions{{Group: group, Partitions: parts}}
	result, err := i.admin.ListConsumerGroupOffsets(ctx, request, k.SetAdminRequestTimeout(i.coordinatorTimeout()))
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
func (i *Inspector) readProgress(parent context.Context, group, fp string, window plan.WindowSpec, cutoff int64) (map[int32]Progress, error) {
	parts, err := i.progress(parent, group)
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
	return kafkaport.ValidateWindow(progress, spec, cutoff)
}
func (i *Inspector) checkMembers(parent context.Context, group string) error {
	ctx, cancel := i.context(parent)
	defer cancel()
	result, err := i.admin.DescribeConsumerGroups(ctx, []string{group}, k.SetAdminRequestTimeout(i.coordinatorTimeout()))
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
			if !strings.HasPrefix(member.ClientID, "kaferry-") {
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
func (i *Inspector) offsets(parent context.Context, partitions []int32, spec k.OffsetSpec, readCommitted bool) (map[int32]int64, error) {
	ctx, cancel := i.context(parent)
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
func (i *Inspector) config(parent context.Context) map[string]string {
	ctx, cancel := i.context(parent)
	defer cancel()
	result, err := i.admin.DescribeConfigs(ctx, []k.ConfigResource{{Type: k.ResourceTopic, Name: i.topic}})
	if err != nil || len(result) != 1 || result[0].Error.Code() != k.ErrNoError || result[0].Type != k.ResourceTopic || result[0].Name != i.topic {
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
