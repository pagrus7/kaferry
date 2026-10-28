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
	o.mu.Lock()
	o.partitions[id] = p
	o.mu.Unlock()
	return p, nil
}
func (o *kafkaOutput) Read(ctx context.Context, id int32) (*delivery.Frame, error) {
	return o.log.readFrame(ctx, id, false)
}
func (j *ProgressLog) readFrame(ctx context.Context, id int32, barrier bool) (*delivery.Frame, error) {
	data, err := j.ReadValue(ctx, id, j.session.Key("state"), barrier)
	if err != nil || data == nil {
		return nil, err
	}
	f, err := j.session.Decode(data)
	if err != nil {
		return nil, err
	}
	if f.Cursor.Partition != id {
		return nil, errors.New("checkpoint partition disagrees with key")
	}
	return &f, nil
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
		if !l.tx.open {
			if err := l.tx.client.BeginTransaction(); err != nil {
				l.failed = err
				return err
			}
			l.tx.open = true
		}
		// Every delivery from this synchronous preparation is reported together.
		// Commit checks that its cursor covers every record in the transaction.
		batch := l.queue
		l.queue = nil
		completed := make(chan struct{}, len(batch))
		failures := make([]error, len(batch))
		waitContext, cancel := context.WithCancel(ctx)
		for _, entry := range batch {
			if deadline, ok := entry.ctx.Deadline(); ok {
				if current, present := waitContext.Deadline(); !present || deadline.Before(current) {
					cancel()
					waitContext, cancel = context.WithDeadline(ctx, deadline)
				}
			}
		}
		for i, entry := range batch {
			l.tx.client.Produce(entry.ctx, entry.record, func(_ *kgo.Record, err error) { failures[i] = err; completed <- struct{}{} })
		}
		received := 0
		for received < len(batch) {
			select {
			case <-completed:
				received++
			case <-waitContext.Done():
				// A transactional producer cannot cancel ambiguous in-flight records
				// individually. Closing leaves the transaction uncommitted; the next
				// owner fences it before deciding the authoritative resume position.
				l.failed = waitContext.Err()
				l.tx.client.Close()
				l.tx.open = false
				for received < len(batch) {
					<-completed
					received++
				}
			}
		}
		cancel()
		first := l.failed
		for i, entry := range batch {
			first = errors.Join(first, failures[i])
			if failures[i] == nil {
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
		j.session.Hook("flushed", id)
	}
	return ctx.Err()
}

func (o *kafkaOutput) Prepare(ctx context.Context, ids []int32) error { return o.log.Ensure(ctx, ids) }
