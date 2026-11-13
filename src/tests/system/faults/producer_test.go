package faults

import (
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
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
