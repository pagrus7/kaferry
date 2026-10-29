package franz

import (
	"context"

	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
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
	TryProduce(context.Context, *kgo.Record, core.Cursor, func(*kgo.Record, error))
	Flush(context.Context) error
	Close()
}
type TransactionClient interface {
	Produce(context.Context, *kgo.Record, func(*kgo.Record, error))
	Flush(context.Context) error
	ProducerID(context.Context) (int64, int16, error)
	BeginTransaction() error
	EndTransaction(context.Context, kgo.TransactionEndTry) error
	OptValue(any) any
	Close()
}
type Clients interface {
	Transaction(...kgo.Opt) (TransactionClient, error)
	Metadata(...kgo.Opt) (MetadataClient, error)
	Consumer(...kgo.Opt) (ConsumerClient, error)
}
type clients struct{}

func NewClients() Clients { return clients{} }
func (clients) Metadata(opts ...kgo.Opt) (MetadataClient, error) {
	c, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return protocolClient{c}, nil
}
func (clients) Consumer(opts ...kgo.Opt) (ConsumerClient, error) { return kgo.NewClient(opts...) }

type protocolClient struct{ *kgo.Client }

func (c protocolClient) RequestTo(ctx context.Context, broker int32, req kmsg.Request) (kmsg.Response, error) {
	return c.Broker(int(broker)).Request(ctx, req)
}

func (clients) Transaction(opts ...kgo.Opt) (TransactionClient, error) { return kgo.NewClient(opts...) }
