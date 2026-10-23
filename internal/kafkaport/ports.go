// Package kafkaport defines the contracts shared by Kafka client adapters.
package kafkaport

import (
	"context"

	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
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
	ClusterID(context.Context) (string, error)
	Partitions(context.Context) ([]int32, error)
	PartitionValidator
	CheckMembers(context.Context, string) error
	ReadProgress(context.Context, string, string, plan.WindowSpec, int64) (map[int32]Progress, error)
	Source(context.Context, plan.Plan, string, config.Worker, PartitionValidator) (core.Source, core.ProgressStore, error)
	Sink(context.Context, config.Worker) (core.Sink, error)
	Close(context.Context) error
}
