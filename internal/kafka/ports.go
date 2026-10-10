package kafka

import (
	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	copy "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
)

type AssignmentStore interface {
	copy.ProgressStore
	SetMetadata(int32, Progress)
}

type OffsetReader interface {
	Offsets([]int32, k.OffsetSpec, bool) (map[int32]int64, error)
}
type TimeReader interface {
	OffsetsForTimes([]k.TopicPartition, int) ([]k.TopicPartition, error)
}
type SourceMetadata interface {
	OffsetReader
	TopicID() string
	CurrentPartitions() ([]k.TopicPartition, error)
}
type DestinationMetadata interface {
	Config() map[string]string
	PartitionCount() int
}
type Metadata interface {
	SourceMetadata
	Progress(string) ([]k.TopicPartition, error)
	DestinationMetadata
	ClusterID() (string, error)
	CheckMembers(string) error
	ReadProgress(string, string, plan.WindowSpec, int64) (map[int32]Progress, error)
	Close()
}
