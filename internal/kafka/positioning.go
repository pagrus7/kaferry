package kafka

import (
	"context"
	"sort"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/plan"
)

type Positioning = kafkaport.Positioning
type AssignmentPositioner interface {
	Locate([]k.TopicPartition, map[int32]k.TopicPartition, map[int32]Progress) (Positioning, error)
}
type PositionerFactory interface {
	New(context.Context, OffsetReader, TimeReader, plan.Plan, string, string) AssignmentPositioner
}
type positionerFactory struct{ shared kafkaport.PositionerFactory }

func NewPositionerFactory(shared kafkaport.PositionerFactory) PositionerFactory {
	return &positionerFactory{shared: shared}
}
func (f *positionerFactory) New(ctx context.Context, offsets OffsetReader, times TimeReader, p plan.Plan, fp, topicID string) AssignmentPositioner {
	return &nativePositioner{ctx: ctx, shared: f.shared.New(nativeOffsets{offsets}, nativeTimes{times, p.Source.Name}, p, fp, topicID)}
}

type nativePositioner struct {
	ctx    context.Context
	shared kafkaport.AssignmentPositioner
}

func (p *nativePositioner) Locate(parts []k.TopicPartition, saved map[int32]k.TopicPartition, metadata map[int32]Progress) (Positioning, error) {
	ids := make([]int32, len(parts))
	for i, part := range parts {
		ids[i] = part.Partition
	}
	positions := make(map[int32]int64, len(saved))
	for id, part := range saved {
		positions[id] = int64(part.Offset)
	}
	return p.shared.Locate(p.ctx, ids, positions, metadata)
}

type nativeOffsets struct{ reader OffsetReader }

func (n nativeOffsets) Offsets(ctx context.Context, ids []int32, kind kafkaport.OffsetKind, committed bool) (map[int32]int64, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	spec := k.EarliestOffsetSpec
	if kind == kafkaport.LatestOffset {
		spec = k.LatestOffsetSpec
	}
	return n.reader.Offsets(ids, spec, committed)
}

type nativeTimes struct {
	reader TimeReader
	topic  string
}

func (n nativeTimes) OffsetsForTimes(ctx context.Context, times map[int32]int64) (map[int32]int64, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ids := make([]int32, 0, len(times))
	for id := range times {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	parts := make([]k.TopicPartition, len(ids))
	for i, id := range ids {
		parts[i] = k.TopicPartition{Topic: &n.topic, Partition: id, Offset: k.Offset(times[id])}
	}
	timeout := 5000
	if deadline, ok := ctx.Deadline(); ok {
		timeout = min(timeout, max(1, int((time.Until(deadline)+time.Millisecond-1)/time.Millisecond)))
	}
	result, err := n.reader.OffsetsForTimes(parts, timeout)
	if err != nil {
		return nil, err
	}
	return timestampOffsets(result, n.topic)
}

func timestampOffsets(parts []k.TopicPartition, topic string) (map[int32]int64, error) {
	found := make(map[int32]int64, len(parts))
	for _, part := range parts {
		if part.Topic == nil || *part.Topic != topic {
			continue
		}
		if part.Error != nil {
			return nil, part.Error
		}
		found[part.Partition] = int64(part.Offset)
	}
	return found, nil
}
