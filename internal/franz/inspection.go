package franz

import (
	"context"
	"slices"

	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/plan"
)

func (c *connection) Describe(ctx context.Context, configuration bool) (kafkaport.TopicDescription, error) {
	if err := c.inspector.Refresh(ctx); err != nil {
		return kafkaport.TopicDescription{}, err
	}
	info := kafkaport.TopicDescription{ClusterID: c.inspector.clusterID, TopicID: topicID(c.inspector.topicID), Partitions: slices.Clone(c.inspector.parts)}
	if configuration {
		info.TimestampType = c.inspector.Config(ctx)["message.timestamp.type"]
	}
	return info, ctx.Err()
}

func (c *connection) Membership(ctx context.Context, group string) (kafkaport.Membership, error) {
	return c.inspector.Membership(ctx, group)
}

func (c *connection) Saved(ctx context.Context, group, fingerprint string, window plan.WindowSpec, cutoff int64) (map[int32]int64, map[int32]kafkaport.Progress, error) {
	return c.inspector.Saved(ctx, group, fingerprint, window, cutoff)
}
func (c *connection) Offsets(ctx context.Context, parts []int32, kind kafkaport.OffsetKind, committed bool) (map[int32]int64, error) {
	return c.inspector.Offsets(ctx, parts, kind, committed)
}
func (c *connection) OffsetsForTimes(ctx context.Context, queries map[int32]int64) (map[int32]int64, error) {
	return c.inspector.OffsetsForTimes(ctx, queries)
}
