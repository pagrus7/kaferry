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

// Membership is one observation, not a lease or a completion barrier.
type Membership struct {
	State   string
	Members int
	Foreign bool
	Idle    bool
}

// InspectionConnection cannot create a consumer, producer or progress store.
type InspectionConnection interface {
	OffsetReader
	TimeReader
	Describe(context.Context, bool) (TopicDescription, error)
	CheckMembers(context.Context, string) error
	Membership(context.Context, string) (Membership, error)
	Saved(context.Context, string, string, plan.WindowSpec, int64) (map[int32]int64, map[int32]Progress, error)
	Close(context.Context) error
}
