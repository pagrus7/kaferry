package franz

import (
	"context"
	"errors"
	"github.com/pagrus7/kaferry/internal/delivery"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"time"
)

const abortBudget = 3 * time.Second

type ProgressLog struct {
	options []kgo.Opt
	topic   string
	session *delivery.Session
	clients Clients
}

func (c *connection) ProgressLog(ctx context.Context, s *delivery.Session, topic string) (delivery.Log, error) {
	options, err := c.borrowedOptions()
	if err != nil {
		return nil, err
	}
	id, err := topicIdentity(ctx, options, topic)
	if err != nil {
		return nil, err
	}
	s.Identity.ProgressTopic = id
	log := &ProgressLog{options: options, topic: topic, session: s, clients: c.factory.clients}
	c.factory.progress = log
	return log, nil
}
func topicIdentity(ctx context.Context, options []kgo.Opt, topic string) ([16]byte, error) {
	client, err := kgo.NewClient(options...)
	if err != nil {
		return [16]byte{}, err
	}
	defer client.Close()
	metadata, err := kadm.NewClient(client).Metadata(ctx, topic)
	if err != nil {
		return [16]byte{}, err
	}
	if err = metadata.Topics.Error(); err != nil {
		return [16]byte{}, err
	}
	item, ok := metadata.Topics[topic]
	if !ok || item.ID == [16]byte{} {
		return [16]byte{}, errors.New("topic identity unavailable")
	}
	return item.ID, nil
}
func (j *ProgressLog) Open(ctx context.Context, id int32) (delivery.Transaction, error) {
	options := append([]kgo.Opt{}, j.options...)
	options = append(options, kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.TransactionalID(j.session.TransactionID(id, j.topic)), kgo.TransactionTimeout(10*time.Second))
	client, err := j.clients.Transaction(options...)
	if err != nil {
		return nil, err
	}
	if _, _, err = client.ProducerID(ctx); err != nil {
		client.Close()
		return nil, err
	}
	return &progressTransaction{client: client, log: j, id: id}, nil
}
func (j *ProgressLog) Publish(ctx context.Context, id int32, writer, key string, value []byte) error {
	options := append(append([]kgo.Opt{}, j.options...), kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.TransactionalID("kaferry-publication-v1-"+writer))
	client, err := j.clients.Transaction(options...)
	if err != nil {
		return err
	}
	defer client.Close()
	tx := &progressTransaction{client: client, log: j, id: id}
	if err = tx.Write(ctx, key, value); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type progressTransaction struct {
	client TransactionClient
	log    *ProgressLog
	id     int32
	open   bool
	failed error
}

func (t *progressTransaction) begin() error {
	if t.failed != nil {
		return t.failed
	}
	if t.open {
		return nil
	}
	if err := t.client.BeginTransaction(); err != nil {
		t.failed = err
		return err
	}
	t.open = true
	return nil
}
func (t *progressTransaction) Write(ctx context.Context, key string, value []byte) error {
	if err := t.begin(); err != nil {
		return err
	}
	err := writeValue(ctx, t.client, t.log.topic, t.id, key, value, func() {
		if key == t.log.session.Key("state") {
			t.log.session.Hook("checkpoint-chunks", t.id)
		}
	})
	if err != nil {
		t.failed = err
	}
	return err
}
func (t *progressTransaction) Commit(ctx context.Context) error {
	if t.failed != nil {
		return t.failed
	}
	if !t.open {
		return errors.New("no prepared transaction")
	}
	if err := t.client.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.failed = err
		return err
	}
	t.open = false
	return nil
}
func (t *progressTransaction) Close() error {
	var err error
	if t.open {
		ctx, cancel := context.WithTimeout(context.Background(), abortBudget)
		err = t.client.EndTransaction(ctx, kgo.TryAbort)
		cancel()
	}
	t.client.Close()
	return err
}
func (j *ProgressLog) Ensure(ctx context.Context, ids []int32) error {
	client, err := kgo.NewClient(j.options...)
	if err != nil {
		return err
	}
	defer client.Close()
	admin := kadm.NewClient(client)
	metadata, err := admin.Metadata(ctx, j.topic)
	if err != nil {
		return err
	}
	if err = metadata.Topics.Error(); err != nil {
		return err
	}
	configs, err := admin.DescribeTopicConfigs(ctx, j.topic)
	if err != nil {
		return err
	}
	for _, config := range configs {
		if config.Err != nil {
			return config.Err
		}
		found := false
		for _, entry := range config.Configs {
			if entry.Key == "cleanup.policy" && entry.Value != nil {
				found = *entry.Value == "compact"
			}
		}
		if !found {
			return errors.New("progress topic requires cleanup.policy=compact without delete")
		}
	}
	count := len(metadata.Topics[j.topic].Partitions)
	needed := count
	for _, id := range ids {
		needed = max(needed, int(id)+1)
	}
	if needed > count {
		result, err := admin.UpdatePartitions(ctx, needed, j.topic)
		if err != nil {
			return err
		}
		if err = result.Error(); err != nil {
			current, readErr := admin.Metadata(ctx, j.topic)
			if readErr != nil {
				return errors.Join(err, readErr)
			}
			if topic, ok := current.Topics[j.topic]; !ok || topic.Err != nil || topic.ID != j.session.Identity.ProgressTopic || len(topic.Partitions) < needed {
				return err
			}
		}
		return nil
	}
	return nil
}
func (j *ProgressLog) ReadValue(ctx context.Context, id int32, key string, barrier bool) ([]byte, error) {
	options := append(append([]kgo.Opt{}, j.options...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	plain, err := kgo.NewClient(options...)
	if err != nil {
		return nil, err
	}
	defer plain.Close()
	var end int64
	if barrier {
		record := &kgo.Record{Topic: j.topic, Partition: id, Key: []byte(j.session.Key("barrier")), Value: []byte("owner scan")}
		if err = produceCheckpoint(ctx, plain, record); err != nil {
			return nil, err
		}
		end = record.Offset + 1
	} else {
		offsets, err := kadm.NewClient(plain).ListCommittedOffsets(ctx, j.topic)
		if err != nil {
			return nil, err
		}
		if err = offsets.Error(); err != nil {
			return nil, err
		}
		if offset, ok := offsets[j.topic][id]; ok {
			end = offset.Offset
		} else {
			return nil, nil
		}
	}
	if end == 0 {
		return nil, nil
	}
	var value valueReader
	position := int64(0)
	for position < end {
		metadata, err := kadm.NewClient(plain).Metadata(ctx, j.topic)
		if err != nil {
			return nil, err
		}
		if err := metadata.Topics.Error(); err != nil {
			return nil, err
		}
		topic := metadata.Topics[j.topic]
		if topic.ID != j.session.Identity.ProgressTopic {
			return nil, errors.New("progress topic identity changed")
		}
		part, ok := topic.Partitions[id]
		if !ok {
			return nil, errors.New("progress partition disappeared")
		}
		request := kmsg.NewPtrFetchRequest()
		request.ReplicaID, request.IsolationLevel, request.SessionEpoch = -1, 1, -1
		request.MaxWaitMillis, request.MinBytes, request.MaxBytes = 50, 1, 2<<20
		partition := kmsg.NewFetchRequestTopicPartition()
		partition.Partition, partition.FetchOffset, partition.CurrentLeaderEpoch, partition.PartitionMaxBytes = id, position, part.LeaderEpoch, 2<<20
		request.Topics = []kmsg.FetchRequestTopic{{Topic: j.topic, TopicID: topic.ID, Partitions: []kmsg.FetchRequestTopicPartition{partition}}}
		raw, err := plain.Broker(int(part.Leader)).Request(ctx, request)
		if err != nil {
			return nil, err
		}
		response := raw.(*kmsg.FetchResponse)
		if err := kerr.ErrorForCode(response.ErrorCode); err != nil {
			return nil, err
		}
		found := false
		previous := position
		for _, topic := range response.Topics {
			for _, partition := range topic.Partitions {
				if partition.Partition != id {
					continue
				}
				found = true
				parsed, next := kgo.ProcessFetchPartition(kgo.ProcessFetchPartitionOpts{Offset: position, IsolationLevel: kgo.ReadCommitted(), KeepControlRecords: true, Topic: j.topic, Partition: id}, &partition, kgo.DefaultDecompressor(), nil)
				if parsed.Err != nil {
					return nil, parsed.Err
				}
				if next < position || next > partition.LastStableOffset {
					return nil, errors.New("checkpoint scan exceeded stable range")
				}
				for _, record := range parsed.Records {
					if record.Offset < end {
						if err := value.accept(key, record); err != nil {
							return nil, err
						}
					}
				}
				position = next
			}
		}
		if !found {
			return nil, errors.New("checkpoint fetch omitted partition")
		}
		if position == previous {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	return value.value()
}
