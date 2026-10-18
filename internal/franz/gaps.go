package franz

import (
	"context"
	"errors"
	"fmt"
	"syscall"

	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type gapReader interface {
	Next(context.Context, int32, int64) (int64, error)
}

// fetchGap proves a consumed gap from record batches, including compacted tails.
// A broker's end offset alone is never evidence that records were consumed.
type fetchGap struct {
	client  MetadataClient
	topic   string
	topicID string
}

func (g *fetchGap) Next(ctx context.Context, id int32, position int64) (int64, error) {
	lookup := kmsg.NewPtrMetadataRequest()
	wanted := kmsg.NewMetadataRequestTopic()
	wanted.Topic = &g.topic
	lookup.Topics = []kmsg.MetadataRequestTopic{wanted}
	metadata, err := lookup.RequestWith(ctx, g.client)
	if err != nil {
		return position, transientFailure(err)
	}
	if len(metadata.Topics) != 1 || metadata.Topics[0].Topic == nil || *metadata.Topics[0].Topic != g.topic {
		return position, fmt.Errorf("source topic unavailable")
	}
	topic := metadata.Topics[0]
	if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
		return position, transientFailure(err)
	}
	if topicID(topic.TopicID) != g.topicID {
		return position, fmt.Errorf("source topic identity changed")
	}
	var partition *kmsg.MetadataResponseTopicPartition
	for i := range topic.Partitions {
		if topic.Partitions[i].Partition == id {
			partition = &topic.Partitions[i]
			break
		}
	}
	if partition == nil {
		return position, fmt.Errorf("source partition %d is missing from metadata", id)
	}
	if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
		return position, transientFailure(err)
	}
	if partition.Leader < 0 {
		return position, &core.Recoverable{Err: fmt.Errorf("source partition %d leader is being elected", id)}
	}
	req := kmsg.NewPtrFetchRequest()
	req.MaxWaitMillis = 0
	req.ReplicaID, req.IsolationLevel, req.SessionEpoch = -1, 1, -1
	req.MinBytes, req.MaxBytes = 1, 1<<20
	part := kmsg.NewFetchRequestTopicPartition()
	part.Partition, part.FetchOffset, part.CurrentLeaderEpoch, part.PartitionMaxBytes = id, position, partition.LeaderEpoch, 1<<20
	req.Topics = []kmsg.FetchRequestTopic{{Topic: g.topic, TopicID: topic.TopicID, Partitions: []kmsg.FetchRequestTopicPartition{part}}}
	response, err := req.RequestWith(ctx, brokerRequest{g.client, partition.Leader})
	if err != nil {
		return position, transientFailure(err)
	}
	if err := kerr.ErrorForCode(response.ErrorCode); err != nil {
		return position, transientFailure(err)
	}
	for _, result := range response.Topics {
		if (response.Version >= 13 && result.TopicID != topic.TopicID) || (response.Version < 13 && result.Topic != g.topic) {
			continue
		}
		for _, p := range result.Partitions {
			if p.Partition == id {
				next, err := emptyBatchEnd(position, g.topic, &p)
				return next, transientFailure(err)
			}
		}
	}
	failure := fmt.Errorf("gap fetch omitted partition %d", id)
	if response.ThrottleMillis > 0 {
		return position, &core.Recoverable{Err: failure}
	}
	return position, failure
}

func transientFailure(err error) error {
	if errors.Is(err, kerr.UnknownTopicOrPartition) || errors.Is(err, kerr.UnknownTopicID) {
		return err // The source identity was already established before consumption.
	}
	if brokerUnreachable(err) || errors.Is(err, context.DeadlineExceeded) || kerr.IsRetriable(err) || kgo.IsRetryableBrokerErr(err) {
		return &core.Recoverable{Err: err}
	}
	return err
}

// An established source may still have the old leader or coordinator address.
func brokerUnreachable(err error) bool {
	return errors.Is(err, syscall.ECONNREFUSED)
}

func emptyBatchEnd(position int64, topic string, p *kmsg.FetchResponseTopicPartition) (int64, error) {
	parsed, next := kgo.ProcessFetchPartition(kgo.ProcessFetchPartitionOpts{Offset: position, IsolationLevel: kgo.ReadCommitted(), Topic: topic, Partition: p.Partition}, p, kgo.DefaultDecompressor(), nil)
	if parsed.Err != nil {
		return position, parsed.Err
	}
	if next < position || next > p.LastStableOffset {
		return position, fmt.Errorf("gap cursor outside the fetched stable range")
	}
	if len(parsed.Records) > 0 {
		return position, nil // The ordinary consumer must deliver retained records.
	}
	return next, nil
}

// brokerRequest keeps leader routing mockable without exposing a concrete broker.
type brokerRequest struct {
	client MetadataClient
	id     int32
}

func (b brokerRequest) Request(ctx context.Context, req kmsg.Request) (kmsg.Response, error) {
	return b.client.RequestTo(ctx, b.id, req)
}
