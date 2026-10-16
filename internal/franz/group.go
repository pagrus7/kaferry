package franz

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func (i *Inspector) CheckMembers(parent context.Context, group string) error {
	ctx, cancel := requestContext(parent)
	defer cancel()
	req := kmsg.NewPtrDescribeGroupsRequest()
	req.Groups = []string{group}
	response, err := req.RequestWith(ctx, i.client)
	if err != nil {
		return err
	}
	if len(response.Groups) != 1 || response.Groups[0].Group != group {
		return fmt.Errorf("missing group description")
	}
	g := response.Groups[0]
	if errors.Is(kerr.ErrorForCode(g.ErrorCode), kerr.GroupIDNotFound) {
		return nil
	}
	if err := kerr.ErrorForCode(g.ErrorCode); err != nil {
		return err
	}
	for _, m := range g.Members {
		if !strings.HasPrefix(m.ClientID, "kaferry-") {
			return conflict("GROUP has a foreign active member")
		}
		if len(m.MemberAssignment) == 0 {
			continue
		}
		var assignment kmsg.ConsumerMemberAssignment
		if err := assignment.ReadFrom(m.MemberAssignment); err != nil {
			return conflict("GROUP has an unreadable member assignment")
		}
		for _, t := range assignment.Topics {
			if t.Topic != i.topic {
				return conflict("GROUP has a member assigned to another topic")
			}
		}
	}
	return nil
}

func (i *Inspector) Saved(parent context.Context, group, fp string, window plan.WindowSpec, cutoff int64) (map[int32]int64, map[int32]kafkaport.Progress, error) {
	if err := i.Refresh(parent); err != nil {
		return nil, nil, err
	}
	ctx, cancel := requestContext(parent)
	defer cancel()
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Group, req.RequireStable = group, true
	req.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: i.topic, Partitions: i.parts}}
	req.Groups = []kmsg.OffsetFetchRequestGroup{{Group: group, MemberEpoch: -1, Topics: []kmsg.OffsetFetchRequestGroupTopic{{Topic: i.topic, TopicID: i.topicID, Partitions: i.parts}}}}
	response, err := req.RequestWith(ctx, i.client)
	if err != nil {
		return nil, nil, err
	}
	parts, err := i.offsetParts(response, group)
	if errors.Is(err, kerr.GroupIDNotFound) {
		return map[int32]int64{}, map[int32]kafkaport.Progress{}, nil
	}
	if err != nil {
		return nil, nil, err
	}
	return i.decodeSaved(parts, fp, window, cutoff)
}

func (i *Inspector) offsetParts(r *kmsg.OffsetFetchResponse, group string) (map[int32]kmsg.OffsetFetchResponseTopicPartition, error) {
	if err := kerr.ErrorForCode(r.ErrorCode); err != nil {
		return nil, err
	}
	result := make(map[int32]kmsg.OffsetFetchResponseTopicPartition)
	if len(r.Groups) > 0 {
		if len(r.Groups) != 1 || r.Groups[0].Group != group {
			return nil, fmt.Errorf("missing group offset result")
		}
		g := r.Groups[0]
		if err := kerr.ErrorForCode(g.ErrorCode); err != nil {
			return nil, err
		}
		for _, t := range g.Topics {
			if !sameTopic(t.Topic, t.TopicID, i.topicID, i.topic) {
				continue
			}
			for _, p := range t.Partitions {
				result[p.Partition] = kmsg.OffsetFetchResponseTopicPartition{Partition: p.Partition, Offset: p.Offset, Metadata: p.Metadata, ErrorCode: p.ErrorCode}
			}
		}
	} else {
		for _, t := range r.Topics {
			if t.Topic == i.topic {
				for _, p := range t.Partitions {
					result[p.Partition] = p
				}
			}
		}
	}
	return result, nil
}

func (i *Inspector) decodeSaved(parts map[int32]kmsg.OffsetFetchResponseTopicPartition, fp string, window plan.WindowSpec, cutoff int64) (map[int32]int64, map[int32]kafkaport.Progress, error) {
	positions := make(map[int32]int64)
	metadata := make(map[int32]kafkaport.Progress)
	for _, id := range i.parts {
		p, ok := parts[id]
		if !ok {
			return nil, nil, fmt.Errorf("missing committed result for partition %d", id)
		}
		if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
			return nil, nil, err
		}
		if p.Offset < 0 {
			continue
		}
		progress, err := kafkaport.DecodeProgress(p.Metadata, fp, topicID(i.topicID), window.Relative())
		if err != nil {
			return nil, nil, err
		}
		if err = kafkaport.ValidateWindow(progress, window, cutoff); err != nil {
			return nil, nil, err
		}
		positions[id], metadata[id] = p.Offset, progress
	}
	return positions, metadata, nil
}
