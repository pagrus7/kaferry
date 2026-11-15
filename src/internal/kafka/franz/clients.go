package franz

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl"
)

type requester interface {
	Request(context.Context, kmsg.Request) (kmsg.Response, error)
}
type MetadataClient interface {
	requester
	RequestTo(context.Context, int32, kmsg.Request) (kmsg.Response, error)
	Close()
}
type ConsumerClient interface {
	AddConsumeTopics(...string)
	PollRecords(context.Context, int) kgo.Fetches
	AllowRebalance()
	PauseFetchPartitions(map[string][]int32) map[string][]int32
	ResumeFetchPartitions(map[string][]int32)
	CommitOffsetsSync(context.Context, map[string]map[int32]kgo.EpochOffset, func(*kgo.Client, *kmsg.OffsetCommitRequest, *kmsg.OffsetCommitResponse, error))
	LeaveGroupContext(context.Context) error
	CloseAllowingRebalance()
}
type ProducerClient interface {
	TryProduce(context.Context, *kgo.Record, func(*kgo.Record, error))
	Flush(context.Context) error
	Close()
}
type Clients interface {
	Metadata(...kgo.Opt) (MetadataClient, error)
	Consumer(...kgo.Opt) (ConsumerClient, error)
	Producer(...kgo.Opt) (ProducerClient, error)
}
type clients struct{}

func NewClients() Clients { return clients{} }
func (clients) Metadata(opts ...kgo.Opt) (MetadataClient, error) {
	return newProtocolClient(opts...)
}
func (clients) Consumer(opts ...kgo.Opt) (ConsumerClient, error) {
	return newProtocolClient(opts...)
}
func (clients) Producer(opts ...kgo.Opt) (ProducerClient, error) {
	return newProtocolClient(opts...)
}

func newProtocolClient(opts ...kgo.Opt) (protocolClient, error) {
	c, err := kgo.NewClient(opts...)
	if err != nil {
		return protocolClient{}, err
	}
	return protocolClient{c}, nil
}

type protocolClient struct{ *kgo.Client }

func (c protocolClient) redact(err error) error {
	if brokerAuthenticationFailed(err) {
		mechanisms := c.OptValue(kgo.SASL).([]sasl.Mechanism)
		if len(mechanisms) != 0 {
			return authenticationError(mechanisms[0].Name(), err)
		}
	}
	return err
}

func (c protocolClient) Request(ctx context.Context, req kmsg.Request) (kmsg.Response, error) {
	response, err := c.Client.Request(ctx, req)
	return response, c.redact(err)
}

func (c protocolClient) RequestTo(ctx context.Context, broker int32, req kmsg.Request) (kmsg.Response, error) {
	response, err := c.Broker(int(broker)).Request(ctx, req)
	return response, c.redact(err)
}

func (c protocolClient) PollRecords(ctx context.Context, maxRecords int) kgo.Fetches {
	fetches := c.Client.PollRecords(ctx, maxRecords)
	for _, fetch := range fetches {
		for _, topic := range fetch.Topics {
			for i := range topic.Partitions {
				topic.Partitions[i].Err = c.redact(topic.Partitions[i].Err)
			}
		}
	}
	return fetches
}

func (c protocolClient) CommitOffsetsSync(ctx context.Context, offsets map[string]map[int32]kgo.EpochOffset, callback func(*kgo.Client, *kmsg.OffsetCommitRequest, *kmsg.OffsetCommitResponse, error)) {
	c.Client.CommitOffsetsSync(ctx, offsets, func(client *kgo.Client, request *kmsg.OffsetCommitRequest, response *kmsg.OffsetCommitResponse, err error) {
		callback(client, request, response, c.redact(err))
	})
}

func (c protocolClient) LeaveGroupContext(ctx context.Context) error {
	return c.redact(c.Client.LeaveGroupContext(ctx))
}

func (c protocolClient) TryProduce(ctx context.Context, record *kgo.Record, callback func(*kgo.Record, error)) {
	c.Client.TryProduce(ctx, record, func(record *kgo.Record, err error) {
		callback(record, c.redact(err))
	})
}
