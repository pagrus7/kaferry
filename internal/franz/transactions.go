package franz

import (
	"context"
	"errors"
	"fmt"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/delivery"
	"github.com/twmb/franz-go/pkg/kgo"
	"reflect"
	"sync"
	"time"
)

type kafkaWrite struct {
	ctx      context.Context
	record   *kgo.Record
	callback func(*kgo.Record, error)
	next     int64
}
type kafkaPartition struct {
	tx     *progressTransaction
	output *kafkaOutput
	frame  *delivery.Frame
	queue  []kafkaWrite
	ready  int64
	failed error
}
type kafkaOutput struct {
	session    *delivery.Session
	log        *ProgressLog
	options    []kgo.Opt
	settings   settings
	mu         sync.Mutex
	partitions map[int32]*kafkaPartition
}

func (o *kafkaOutput) Acquire(ctx context.Context, id int32) (delivery.Partition, error) {
	p, err := o.openPartition(ctx, id)
	if err != nil {
		return nil, err
	}
	o.mu.Lock()
	o.partitions[id] = p
	o.mu.Unlock()
	return p, nil
}
func (o *kafkaOutput) openPartition(ctx context.Context, id int32) (*kafkaPartition, error) {
	options := append([]kgo.Opt{}, o.options...)
	options = append(options, o.settings.producerOptions()...)
	partitioner := kgo.StickyKeyPartitioner(nil)
	if o.session.Plan.Destination.PreservesPartitions() {
		partitioner = kgo.ManualPartitioner()
	}
	options = append(options, kgo.RecordPartitioner(progressPartitioner{topic: o.log.topic, output: partitioner}))
	options = append(options, kgo.TransactionalID(o.session.TransactionID(id, o.log.topic)), kgo.TransactionTimeout(10*time.Second))
	client, err := o.log.clients.Transaction(options...)
	if err != nil {
		return nil, err
	}
	if _, _, err = client.ProducerID(ctx); err != nil {
		client.Close()
		return nil, err
	}
	frame, err := o.log.readFrame(ctx, id, true)
	if err != nil {
		client.Close()
		return nil, err
	}
	p := &kafkaPartition{tx: &progressTransaction{client: client, log: o.log, id: id}, output: o, frame: frame}
	return p, nil
}
func (o *kafkaOutput) Read(ctx context.Context, id int32) (*delivery.Frame, error) {
	return o.log.readFrame(ctx, id, false)
}
func (j *ProgressLog) readFrame(ctx context.Context, id int32, barrier bool) (*delivery.Frame, error) {
	return (delivery.Reader{Session: j.session, Log: j}).ReadBarrier(ctx, id, barrier)
}
func (p *kafkaPartition) Saved() *delivery.Frame { return p.frame }
func (p *kafkaPartition) Commit(ctx context.Context, f delivery.Frame) error {
	if p.failed != nil {
		return p.failed
	}
	if p.ready > f.Cursor.Position {
		return fmt.Errorf("transaction contains output past confirmed cursor: prepared=%d confirmed=%d", p.ready, f.Cursor.Position)
	}
	if p.frame != nil && !p.tx.open && p.ready == 0 && reflect.DeepEqual(*p.frame, f) {
		return nil
	}
	data, err := delivery.Encode(f)
	if err != nil {
		return err
	}
	if err = p.tx.Write(ctx, p.output.session.Key("state"), data); err != nil {
		p.failed = err
		return err
	}
	p.output.session.Hook("prepared", f.Cursor.Partition)
	if err = p.tx.Commit(ctx); err != nil {
		p.failed = err
		return err
	}
	p.ready = 0
	p.frame = &f
	p.output.session.Hook("committed", f.Cursor.Partition)
	return nil
}
func (p *kafkaPartition) Close() error {
	o := p.output
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, w := range p.queue {
		w.callback(w.record, errors.New("output partition released"))
	}
	p.queue = nil
	delete(o.partitions, p.tx.id)
	return p.tx.Close()
}
func (o *kafkaOutput) TryProduce(ctx context.Context, r *kgo.Record, cursor core.Cursor, callback func(*kgo.Record, error)) {
	o.mu.Lock()
	defer o.mu.Unlock()
	p := o.partitions[cursor.Partition]
	if p == nil {
		callback(r, errors.New("submission has no output lease"))
		return
	}
	p.queue = append(p.queue, kafkaWrite{ctx: ctx, record: r, callback: callback, next: cursor.Position})
	o.session.Hook("queued", cursor.Partition)
}
func (o *kafkaOutput) Flush(ctx context.Context) error { return o.flush(ctx) }
func (o *kafkaOutput) Close()                          {}

type progressPartitioner struct {
	topic  string
	output kgo.Partitioner
}

func (p progressPartitioner) ForTopic(topic string) kgo.TopicPartitioner {
	if topic == p.topic {
		return kgo.ManualPartitioner().ForTopic(topic)
	}
	return p.output.ForTopic(topic)
}
func (j *kafkaOutput) flush(ctx context.Context) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	for id, l := range j.partitions {
		if l.failed != nil {
			return l.failed
		}
		if len(l.queue) == 0 {
			continue
		}
		batch := l.queue
		l.queue = nil
		waitContext, cancel := context.WithCancel(ctx)
		for _, entry := range batch {
			if deadline, ok := entry.ctx.Deadline(); ok {
				if current, present := waitContext.Deadline(); !present || deadline.Before(current) {
					cancel()
					waitContext, cancel = context.WithDeadline(ctx, deadline)
				}
			}
		}
		for {
			if !l.tx.open {
				if err := l.tx.client.BeginTransaction(); err != nil {
					cancel()
					l.failed = err
					for _, entry := range batch {
						entry.callback(entry.record, err)
					}
					return err
				}
				l.tx.open = true
			}
			completed := make(chan struct{}, len(batch))
			failures := make([]error, len(batch))
			for i, entry := range batch {
				l.tx.client.Produce(entry.ctx, entry.record, func(_ *kgo.Record, err error) { failures[i] = err; completed <- struct{}{} })
			}
			received := 0
			for received < len(batch) {
				select {
				case <-completed:
					received++
				case <-waitContext.Done():
					l.failed = waitContext.Err()
					l.tx.client.Close()
					l.tx.open = false
					for received < len(batch) {
						<-completed
						received++
					}
				}
			}
			first := l.failed
			retryMissingBroker := l.failed == nil && l.ready == 0 && waitContext.Err() == nil
			for _, err := range failures {
				first = errors.Join(first, err)
				// franz-go's missing-broker error has no exported sentinel. It
				// occurs when coordinator metadata loses a stopped broker.
				if err != nil && err.Error() != "unknown broker" {
					retryMissingBroker = false
				}
			}
			if first != nil && retryMissingBroker {
				// This batch has not committed and no earlier batch awaits commit.
				// A new producer fences this one and aborts its pending transaction.
				// Retry after reloading the same cursor, with the original deadlines.
				l.tx.client.Close()
				l.tx.open = false
				replacement, err := j.openPartition(waitContext, id)
				if err == nil && !reflect.DeepEqual(l.frame, replacement.frame) {
					err = errors.New("output authority changed during producer recovery")
					_ = replacement.tx.Close()
				}
				if err == nil {
					l.tx, l.frame = replacement.tx, replacement.frame
					continue
				}
				first = errors.Join(first, err)
			}
			cancel()
			if first == nil {
				for _, entry := range batch {
					l.ready = max(l.ready, entry.next)
				}
			}
			for _, entry := range batch {
				entry.callback(entry.record, first)
			}
			if first != nil {
				l.failed = first
				return first
			}
			break
		}
		j.session.Hook("flushed", id)
	}
	return ctx.Err()
}

func (o *kafkaOutput) Prepare(ctx context.Context, ids []int32) error { return o.log.Ensure(ctx, ids) }
