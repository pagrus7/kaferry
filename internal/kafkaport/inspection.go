package kafkaport

import (
	"context"

	"github.com/pagrus7/kaferry/internal/plan"
)

type TopicDescription struct {
	ClusterID     string
	TopicID       string
	Partitions    []int32
	TimestampType string
}

// InspectionConnection cannot create a consumer, producer or progress store.
type InspectionConnection interface {
	OffsetReader
	TimeReader
	Describe(context.Context, bool) (TopicDescription, error)
	CheckMembers(context.Context, string) error
	Saved(context.Context, string, string, plan.WindowSpec, int64) (map[int32]int64, map[int32]Progress, error)
	Close(context.Context) error
}
