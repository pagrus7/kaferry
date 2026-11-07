package kafkaport

import (
	"context"

	"github.com/pagrus7/kaferry/src/internal/plan"
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
	Foreign bool
	Idle    bool
}

// InspectionConnection cannot create a consumer, producer or progress store.
type InspectionConnection interface {
	OffsetReader
	TimeReader
	Describe(context.Context, bool) (TopicDescription, error)
	CheckMembers(context.Context, string, string) error
	Membership(context.Context, string, string) (Membership, error)
	// Saved returns source-validated rows with a FingerprintMismatch error for
	// diagnostic inspection. Every execution caller must stop on any error.
	Saved(context.Context, string, string, plan.WindowSpec, int64) (map[int32]SavedProgress, error)
	Close(context.Context) error
}
