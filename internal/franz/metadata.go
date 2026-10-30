package franz

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"time"

	"github.com/pagrus7/kaferry/internal/commitmeta"
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type Inspector struct {
	codec     commitmeta.Codec
	client    MetadataClient
	topic     string
	topicID   [16]byte
	clusterID string
	parts     []int32
	log       *slog.Logger
}

func requestContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, 10*time.Second)
}
func conflict(format string, args ...any) error {
	return &kafkaport.Conflict{Err: fmt.Errorf(format, args...)}
}
func topicID(id [16]byte) string { return base64.RawStdEncoding.EncodeToString(id[:]) }
func sameTopic(name string, id, want [16]byte, topic string) bool {
	if id != ([16]byte{}) {
		return id == want && (name == "" || name == topic)
	}
	return name == topic
}

// Refresh requests metadata directly; cached identities cannot detect recreation.
func (i *Inspector) Refresh(parent context.Context) error {
	return i.refresh(parent, true)
}
func (i *Inspector) DestinationPartitions(ctx context.Context) (int, error) {
	if err := i.refresh(ctx, false); err != nil {
		return 0, err
	}
	return len(i.parts), nil
}
func (i *Inspector) refresh(parent context.Context, preserveIdentity bool) error {
	ctx, cancel := requestContext(parent)
	defer cancel()
	req := kmsg.NewPtrMetadataRequest()
	req.AllowAutoTopicCreation = false
	topic := kmsg.NewMetadataRequestTopic()
	topic.Topic = &i.topic
	req.Topics = []kmsg.MetadataRequestTopic{topic}
	response, err := req.RequestWith(ctx, i.client)
	if err != nil {
		return err
	}
	if len(response.Topics) != 1 || response.Topics[0].Topic == nil || *response.Topics[0].Topic != i.topic {
		return conflict("topic does not exist or cannot be described")
	}
	t := response.Topics[0]
	if err := kerr.ErrorForCode(t.ErrorCode); err != nil {
		return conflict("topic cannot be described: %v", err)
	}
	if t.TopicID == ([16]byte{}) {
		return conflict("broker did not provide a topic identity")
	}
	if preserveIdentity && i.topicID != ([16]byte{}) && i.topicID != t.TopicID {
		return conflict("topic identity changed")
	}
	if response.ClusterID == nil || *response.ClusterID == "" {
		return conflict("broker did not provide a cluster identity")
	}
	parts := make([]int32, len(t.Partitions))
	for n, p := range t.Partitions {
		if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
			return err
		}
		parts[n] = p.Partition
	}
	if preserveIdentity && i.parts != nil && len(i.parts) != len(parts) {
		i.log.Info("topic partition count changed", "topic", i.topic, "previous", len(i.parts), "current", len(parts))
	}
	i.parts, i.topicID, i.clusterID = parts, t.TopicID, *response.ClusterID
	return nil
}

func (i *Inspector) Offsets(parent context.Context, ids []int32, kind kafkaport.OffsetKind, committed bool) (map[int32]int64, error) {
	stamp := int64(-2)
	if kind == kafkaport.LatestOffset {
		stamp = -1
	}
	queries := make(map[int32]int64, len(ids))
	for _, id := range ids {
		queries[id] = stamp
	}
	return i.lookupOffsets(parent, queries, committed)
}
func (i *Inspector) OffsetsForTimes(ctx context.Context, queries map[int32]int64) (map[int32]int64, error) {
	return i.lookupOffsets(ctx, queries, true)
}
func (i *Inspector) lookupOffsets(parent context.Context, queries map[int32]int64, committed bool) (map[int32]int64, error) {
	if len(queries) == 0 {
		return map[int32]int64{}, nil
	}
	ctx, cancel := requestContext(parent)
	defer cancel()
	req := kmsg.NewPtrListOffsetsRequest()
	if committed {
		req.IsolationLevel = 1
	}
	t := kmsg.NewListOffsetsRequestTopic()
	t.Topic = i.topic
	for id, stamp := range queries {
		p := kmsg.NewListOffsetsRequestTopicPartition()
		p.Partition, p.Timestamp = id, stamp
		t.Partitions = append(t.Partitions, p)
	}
	req.Topics = []kmsg.ListOffsetsRequestTopic{t}
	response, err := req.RequestWith(ctx, i.client)
	if err != nil {
		return nil, err
	}
	result := make(map[int32]int64, len(queries))
	for _, t := range response.Topics {
		if t.Topic != i.topic {
			continue
		}
		for _, p := range t.Partitions {
			if _, wanted := queries[p.Partition]; !wanted {
				continue
			}
			if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
				return nil, fmt.Errorf("offset lookup partition %d: %w", p.Partition, err)
			}
			result[p.Partition] = p.Offset
		}
	}
	for id := range queries {
		if _, ok := result[id]; !ok {
			return nil, fmt.Errorf("missing offset result for partition %d", id)
		}
	}
	return result, nil
}

func (i *Inspector) Config(parent context.Context) map[string]string {
	ctx, cancel := requestContext(parent)
	defer cancel()
	req := kmsg.NewPtrDescribeConfigsRequest()
	req.Resources = []kmsg.DescribeConfigsRequestResource{{ResourceType: kmsg.ConfigResourceTypeTopic, ResourceName: i.topic}}
	response, err := req.RequestWith(ctx, i.client)
	if err == nil && len(response.Resources) == 1 {
		r := response.Resources[0]
		if r.ErrorCode == 0 && r.ResourceName == i.topic && r.ResourceType == kmsg.ConfigResourceTypeTopic {
			result := make(map[string]string, len(r.Configs))
			for _, c := range r.Configs {
				if c.Value != nil {
					result[c.Name] = *c.Value
				}
			}
			return result
		}
	}
	if parent.Err() == nil {
		i.log.Warn("topic configuration unreadable; dependent preflight checks disabled", "topic", i.topic)
	}
	return nil
}
