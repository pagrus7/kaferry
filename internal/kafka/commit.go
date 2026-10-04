package kafka

import (
	"context"
	"errors"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	copy "kafka-fwd2/internal/copy"
)

type offsetCommitter interface {
	CommitOffsets([]k.TopicPartition) ([]k.TopicPartition, error)
}

type Committer struct {
	consumer offsetCommitter
	topic    string
	metadata map[int32]Progress
	output   copy.OutputProgress
}

func (c *Committer) SetMetadata(partition int32, progress Progress) {
	c.metadata[partition] = progress
}

func (c *Committer) Save(ctx context.Context, cursors []copy.Cursor) (map[int32]error, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(cursors) == 0 {
		return map[int32]error{}, nil
	}
	parts := make([]k.TopicPartition, len(cursors))
	for n, cursor := range cursors {
		progress := c.metadata[cursor.Partition]
		if c.output != nil {
			id, extent := c.output.DurableOutput()
			if progress.FileID != "" && progress.FileID != id {
				return nil, conflict("destination file identity mismatch")
			}
			progress.FileID = id
			progress.Extent = extent
			c.metadata[cursor.Partition] = progress
		}
		metadata := progress.Encode()
		parts[n] = k.TopicPartition{Topic: &c.topic, Partition: cursor.Partition, Offset: k.Offset(cursor.Position), Metadata: &metadata}
	}
	result, err := c.consumer.CommitOffsets(parts)
	if err != nil {
		return nil, classifyCommit(err)
	}
	failures := make(map[int32]error, len(result))
	for _, part := range result {
		failures[part.Partition] = classifyCommit(part.Error)
	}
	return failures, nil
}

// Commit failures that only mean the group moved on or the coordinator is
// unreachable; releasing a revoked partition after one of them replays work.
// Authorization, metadata size, fencing, fatal and unknown errors stay terminal.
var recoverableCommit = map[k.ErrorCode]bool{
	k.ErrRebalanceInProgress: true, k.ErrIllegalGeneration: true, k.ErrUnknownMemberID: true,
	k.ErrCoordinatorNotAvailable: true, k.ErrNotCoordinator: true, k.ErrCoordinatorLoadInProgress: true,
	k.ErrRequestTimedOut: true, k.ErrTransport: true, k.ErrTimedOut: true, k.ErrWaitCoord: true, k.ErrAllBrokersDown: true,
}

func classifyCommit(err error) error {
	var native k.Error
	if errors.As(err, &native) && !native.IsFatal() && recoverableCommit[native.Code()] {
		return &copy.Recoverable{Err: err}
	}
	return err
}
