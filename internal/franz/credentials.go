package franz

import (
	"context"
	"crypto/tls"
	"errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"time"
)

func (c *connection) borrowedOptions() ([]kgo.Opt, error) {
	view, ok := c.inspector.client.(optionView)
	if !ok {
		return nil, errors.New("Kafka metadata client cannot share credentials")
	}
	return commonSnapshot(view), nil
}

type borrowedMechanism struct{ sasl.Mechanism }

func borrowCredentials(mechanisms []sasl.Mechanism) []kgo.Opt {
	borrowed := make([]sasl.Mechanism, len(mechanisms))
	for i, m := range mechanisms {
		borrowed[i] = borrowedMechanism{m}
	}
	return []kgo.Opt{kgo.SASL(borrowed...)}
}

type optionView interface{ OptValue(any) any }

func commonSnapshot(view optionView) []kgo.Opt {
	// SASL options append rather than replace; construct a fresh option list so
	// no derived client retains the owning provider's Close method.
	ctx, _ := view.OptValue(kgo.WithContext).(context.Context)
	if ctx == nil {
		ctx = context.Background()
	}
	options := []kgo.Opt{
		kgo.SeedBrokers(view.OptValue(kgo.SeedBrokers).([]string)...), kgo.ClientID(view.OptValue(kgo.ClientID).(string)),
		kgo.WithContext(ctx),
		kgo.RequestTimeoutOverhead(view.OptValue(kgo.RequestTimeoutOverhead).(time.Duration)),
		kgo.MetadataMaxAge(view.OptValue(kgo.MetadataMaxAge).(time.Duration)), kgo.MetadataMinAge(view.OptValue(kgo.MetadataMinAge).(time.Duration)),
		kgo.BrokerMaxReadBytes(view.OptValue(kgo.BrokerMaxReadBytes).(int32)), kgo.BrokerMaxWriteBytes(view.OptValue(kgo.BrokerMaxWriteBytes).(int32)), kgo.DisableClientMetrics(),
	}
	if config := view.OptValue(kgo.DialTLSConfig).(*tls.Config); config != nil {
		options = append(options, kgo.DialTLSConfig(config.Clone()))
	}
	return append(options, borrowCredentials(view.OptValue(kgo.SASL).([]sasl.Mechanism))...)
}
