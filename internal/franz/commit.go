package franz

import (
	"context"
	"errors"
	"fmt"

	"github.com/pagrus7/kaferry/internal/commitmeta"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/delivery"
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type offsetCommitter interface {
	CommitOffsetsSync(context.Context, map[string]map[int32]kgo.EpochOffset, func(*kgo.Client, *kmsg.OffsetCommitRequest, *kmsg.OffsetCommitResponse, error))
}
type Committer struct {
	delivery     *delivery.Session
	codec        commitmeta.Codec
	client       offsetCommitter
	topic, group string
	topicID      [16]byte
	metadata     map[int32]kafkaport.Progress
}

func (c *Committer) Save(ctx context.Context, cursors []core.Cursor) (map[int32]error, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(cursors) == 0 {
		return map[int32]error{}, nil
	}
	offsets, encoded, err := c.snapshot(cursors)
	if err != nil {
		return nil, err
	}
	commitCtx := kgo.PreCommitFnContext(ctx, c.metadataHook(encoded))
	results := make(map[int32]error, len(cursors))
	var failure error
	c.client.CommitOffsetsSync(commitCtx, offsets, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, response *kmsg.OffsetCommitResponse, err error) {
		failure = classifyCommit(err)
		if response == nil {
			return
		}
		for _, t := range response.Topics {
			if !sameTopic(t.Topic, t.TopicID, c.topicID, c.topic) {
				continue
			}
			for _, p := range t.Partitions {
				results[p.Partition] = classifyCommit(kerr.ErrorForCode(p.ErrorCode))
			}
		}
	})
	if failure == nil {
		updates := make([]delivery.Frame, 0, len(cursors))
		for _, cursor := range cursors {
			if err, present := results[cursor.Partition]; present && err == nil {
				updates = append(updates, delivery.Frame{Cursor: cursor, Metadata: encoded[cursor.Partition]})
			}
		}
		if len(updates) > 0 {
			if c.delivery == nil {
				failure = errors.New("output authority required for progress commit")
			} else {
				failure = c.delivery.CommitBatch(ctx, updates)
			}
		}
	}
	return results, failure
}

func classifyCommit(err error) error {
	for _, recoverable := range []error{kerr.RebalanceInProgress, kerr.IllegalGeneration, kerr.UnknownMemberID, kerr.CoordinatorNotAvailable, kerr.NotCoordinator, kerr.CoordinatorLoadInProgress, kerr.RequestTimedOut, context.DeadlineExceeded} {
		if errors.Is(err, recoverable) {
			return &core.Recoverable{Err: err}
		}
	}
	if brokerUnreachable(err) || kgo.IsRetryableBrokerErr(err) {
		return &core.Recoverable{Err: err}
	}
	return err
}

func (c *Committer) snapshot(cursors []core.Cursor) (map[string]map[int32]kgo.EpochOffset, map[int32]string, error) {
	offsets := map[string]map[int32]kgo.EpochOffset{c.topic: {}}
	encoded := make(map[int32]string, len(cursors))
	for _, cursor := range cursors {
		if cursor.Position < 0 {
			return nil, nil, fmt.Errorf("cannot commit a negative source position")
		}
		progress, owned := c.metadata[cursor.Partition]
		if !owned {
			return nil, nil, fmt.Errorf("cannot commit unowned partition %d", cursor.Partition)
		}
		progress.Stop = cursor.Stop
		progress.End = cursor.End
		if cursor.End != nil && cursor.Position > *cursor.End {
			return nil, nil, fmt.Errorf("committed position exceeds captured end")
		}
		if cursor.Stop != nil && cursor.Stop.At > cursor.Position {
			return nil, nil, fmt.Errorf("stop exceeds committed position")
		}
		text, err := c.codec.Encode(progress)
		if err != nil {
			return nil, nil, err
		}
		encoded[cursor.Partition] = text
		offsets[c.topic][cursor.Partition] = kgo.EpochOffset{Epoch: -1, Offset: cursor.Position}
	}

	return offsets, encoded, nil
}
func (c *Committer) metadataHook(encoded map[int32]string) func(*kmsg.OffsetCommitRequest) error {
	return func(req *kmsg.OffsetCommitRequest) error {
		if req.Group != c.group || req.MemberID == "" || req.Generation < 0 {
			return &core.Recoverable{Err: fmt.Errorf("consumer group ownership unavailable for commit")}
		}
		for t := range req.Topics {
			if !sameTopic(req.Topics[t].Topic, req.Topics[t].TopicID, c.topicID, c.topic) {
				return fmt.Errorf("commit names an unexpected topic")
			}
			for n := range req.Topics[t].Partitions {
				part := &req.Topics[t].Partitions[n]
				value, ok := encoded[part.Partition]
				if !ok {
					return fmt.Errorf("commit names an unexpected partition")
				}
				part.Metadata = &value
			}
		}
		return nil
	}

}
