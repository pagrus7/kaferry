package kafka

import (
	"context"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Clients creates the native handles used by the Kafka components.
type Clients interface {
	Admin(k.ConfigMap) (AdminClient, error)
	Consumer(k.ConfigMap) (ConsumerClient, error)
	Producer(k.ConfigMap) (ProducerClient, error)
}

type AdminClient interface {
	ClusterID(context.Context) (string, error)
	DescribeTopics(context.Context, k.TopicCollection, ...k.DescribeTopicsAdminOption) (k.DescribeTopicsResult, error)
	DescribeConsumerGroups(context.Context, []string, ...k.DescribeConsumerGroupsAdminOption) (k.DescribeConsumerGroupsResult, error)
	ListConsumerGroupOffsets(context.Context, []k.ConsumerGroupTopicPartitions, ...k.ListConsumerGroupOffsetsAdminOption) (k.ListConsumerGroupOffsetsResult, error)
	ListOffsets(context.Context, map[k.TopicPartition]k.OffsetSpec, ...k.ListOffsetsAdminOption) (k.ListOffsetsResult, error)
	DescribeConfigs(context.Context, []k.ConfigResource, ...k.DescribeConfigsAdminOption) ([]k.ConfigResourceResult, error)
	Close()
}

type ConsumerClient interface {
	poller
	offsetCommitter
	TimeReader
	Committed([]k.TopicPartition, int) ([]k.TopicPartition, error)
	Subscribe(string, k.RebalanceCb) error
	IncrementalAssign([]k.TopicPartition) error
	IncrementalUnassign([]k.TopicPartition) error
	AssignmentLost() bool
	Pause([]k.TopicPartition) error
	Resume([]k.TopicPartition) error
	Close() error
}

type ProducerClient interface {
	Produce(*k.Message, chan k.Event) error
	Events() chan k.Event
	GetMetadata(*string, bool, int) (*k.Metadata, error)
	Close()
}

type nativeClients struct{}

func NewClients() Clients                                              { return nativeClients{} }
func (nativeClients) Admin(cfg k.ConfigMap) (AdminClient, error)       { return k.NewAdminClient(&cfg) }
func (nativeClients) Consumer(cfg k.ConfigMap) (ConsumerClient, error) { return k.NewConsumer(&cfg) }
func (nativeClients) Producer(cfg k.ConfigMap) (ProducerClient, error) { return k.NewProducer(&cfg) }
