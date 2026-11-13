package faults

import (
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func proxyProducer(seeds ...string) (*kgo.Client, error) {
	return proxyProducerWithHooks(seeds)
}

func proxyProducerWithHooks(seeds []string, hooks ...kgo.Hook) (*kgo.Client, error) {
	return newProxyProducer(seeds, nil, hooks...)
}

func newProxyProducer(seeds []string, diagnostic kgo.Logger, hooks ...kgo.Hook) (*kgo.Client, error) {
	// A diagnostic write must be cancellable even after an ambiguous request.
	// Close this producer before healing; output reconciliation still rejects
	// any cancelled record that actually reached Kafka.
	// The SDK's 5s metadata minimum age exceeds this probe's 2s delivery bound.
	// Its valid 10ms minimum permits native retries inside the existing bound.
	options := []kgo.Opt{kgo.SeedBrokers(seeds...), kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.AllowIdempotentProduceCancellation(), kgo.RecordDeliveryTimeout(2 * time.Second),
		kgo.MetadataMinAge(10 * time.Millisecond), kgo.ProduceRequestTimeout(time.Second), kgo.RequestTimeoutOverhead(time.Second), kgo.WithHooks(hooks...)}
	if diagnostic != nil {
		options = append(options, kgo.WithLogger(diagnostic))
	}
	return kgo.NewClient(options...)
}

func rejectFirstProxyProduce(cluster *kfake.Cluster, code int16) *atomic.Int32 {
	requests := &atomic.Int32{}
	cluster.ControlKey(int16(kmsg.Produce), func(request kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		if requests.Add(1) != 1 {
			return nil, nil, false
		}
		produce := request.(*kmsg.ProduceRequest)
		response := kmsg.NewPtrProduceResponse()
		response.Version = produce.Version
		for _, topic := range produce.Topics {
			result := kmsg.ProduceResponseTopic{Topic: topic.Topic, TopicID: topic.TopicID}
			for _, partition := range topic.Partitions {
				failed := kmsg.NewProduceResponseTopicPartition()
				failed.Partition, failed.ErrorCode = partition.Partition, code
				result.Partitions = append(result.Partitions, failed)
			}
			response.Topics = append(response.Topics, result)
		}
		return response, nil, true
	})
	return requests
}
