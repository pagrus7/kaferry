// Package kafka defines endpoint contracts, positioning and saved-progress validation.
package kafka

import (
	"context"

	"github.com/pagrus7/kaferry/src/internal/config"
	core "github.com/pagrus7/kaferry/src/internal/copy"
	"github.com/pagrus7/kaferry/src/internal/plan"
)

type OffsetKind uint8

const (
	EarliestOffset OffsetKind = iota
	LatestOffset
)

type OffsetReader interface {
	Offsets(context.Context, []int32, OffsetKind, bool) (map[int32]int64, error)
}

type TimeReader interface {
	OffsetsForTimes(context.Context, map[int32]int64) (map[int32]int64, error)
}

type RetentionCheck struct {
	Low   int64
	Lower int64
}

// PartitionValidator checks new source assignments against the destination layout.
type PartitionValidator interface {
	ValidatePartitions(context.Context, []int32) error
}

// Connection owns endpoint metadata and creates one query's source or sink.
type Connection interface {
	Describe(context.Context, bool) (TopicDescription, error)
	Partitions(context.Context) ([]int32, error)
	DestinationPartitions(context.Context) (int, error)
	PartitionValidator
	CheckMembers(context.Context, string, string) error
	ReadProgress(context.Context, string, string, plan.WindowSpec, int64) (map[int32]SavedProgress, error)
	Source(context.Context, plan.Plan, string, config.Worker, PartitionValidator) (core.Source, core.ProgressStore, error)
	Sink(context.Context, config.Worker) (core.Sink, error)
	Close(context.Context) error
}
