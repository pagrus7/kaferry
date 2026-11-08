//go:build integration && fault

package compat

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const replicatedPartitions = 24

// replicatedPublisher publishes one record per partition every 100ms. The
// acknowledgement counts, rather than the scheduler, gate scenario transitions.
// Only one bounded batch is in flight; errors terminate publishing immediately.
type replicatedPublisher struct {
	cancel context.CancelFunc
	done   chan struct{}
	mu     sync.Mutex
	state  replicatedPublishing
}

type replicatedPublishing struct {
	Observed           time.Time `json:"observed"`
	Started            time.Time `json:"started"`
	Stopped            time.Time `json:"stopped"`
	Acknowledged       []int64   `json:"acknowledged_by_partition"`
	Offered            []int64   `json:"offered_by_partition,omitempty"`
	Batches            int       `json:"completed_batches,omitempty"`
	MaxGapSeconds      float64   `json:"max_acknowledgement_gap_seconds,omitempty"`
	MaxOfferGapSeconds float64   `json:"max_offer_gap_seconds,omitempty"`
	err                error
}

// Broker faults can delay one source leader's acknowledgements. Keep one
// bounded offer per partition outstanding so healthy partitions still publish;
// stop joins every callback, including uncertain writes, before reconciliation.
func startReplicatedBrokerPublisher(ctx context.Context, client *kgo.Client, topic string, delivery time.Duration) *replicatedPublisher {
	ctx, cancel := context.WithCancel(ctx)
	p := &replicatedPublisher{cancel: cancel, done: make(chan struct{}), state: replicatedPublishing{
		Started: time.Now().UTC(), Acknowledged: make([]int64, replicatedPartitions), Offered: make([]int64, replicatedPartitions)}}
	go p.runOffers(ctx, client, topic, delivery)
	return p
}

func (p *replicatedPublisher) runOffers(ctx context.Context, client *kgo.Client, topic string, delivery time.Duration) {
	defer close(p.done)
	var pending sync.WaitGroup
	busy := make([]bool, replicatedPartitions)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	var sequence uint64
	last := p.state.Started
	for running := true; running; {
		select {
		case <-ctx.Done():
			running = false
		case <-tick.C:
			now := time.Now().UTC()
			p.mu.Lock()
			p.state.MaxOfferGapSeconds = max(p.state.MaxOfferGapSeconds, now.Sub(last).Seconds())
			p.mu.Unlock()
			last = now
			for partition, record := range replicatedBatch(topic, sequence) {
				p.offer(context.WithoutCancel(ctx), client, record, partition, busy, &pending)
			}
			p.mu.Lock()
			running = p.state.err == nil
			p.mu.Unlock()
			sequence++
		}
	}
	drain, done := context.WithTimeout(context.WithoutCancel(ctx), delivery+time.Second)
	err := client.Flush(drain)
	done()
	// A failed flush must resolve callbacks before the final source snapshot.
	if err != nil {
		client.AbortBufferedRecords(context.Background())
	}
	pending.Wait()
	p.mu.Lock()
	if p.state.err == nil {
		p.state.err = err
	}
	p.state.Stopped = time.Now().UTC()
	p.mu.Unlock()
}

func (p *replicatedPublisher) offer(ctx context.Context, client *kgo.Client, record *kgo.Record, partition int, busy []bool, pending *sync.WaitGroup) {
	p.mu.Lock()
	if busy[partition] || p.state.err != nil {
		p.mu.Unlock()
		return
	}
	busy[partition] = true
	p.state.Offered[partition]++
	p.mu.Unlock()
	pending.Add(1)
	client.TryProduce(ctx, record, func(_ *kgo.Record, err error) {
		defer pending.Done()
		p.mu.Lock()
		defer p.mu.Unlock()
		busy[partition] = false
		if err == nil {
			p.state.Acknowledged[partition]++
		} else if p.state.err == nil {
			p.state.err = err
		}
	})
}

func startReplicatedPublisher(ctx context.Context, client *kgo.Client, topic string) *replicatedPublisher {
	ctx, cancel := context.WithCancel(ctx)
	p := &replicatedPublisher{cancel: cancel, done: make(chan struct{}),
		state: replicatedPublishing{Started: time.Now().UTC(), Acknowledged: make([]int64, replicatedPartitions)}}
	go p.run(ctx, client, topic)
	return p
}

func (p *replicatedPublisher) run(ctx context.Context, client *kgo.Client, topic string) {
	defer close(p.done)
	defer func() { p.mu.Lock(); p.state.Stopped = time.Now().UTC(); p.mu.Unlock() }()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	last := p.state.Started
	var sequence uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		// Finish a submitted batch before honoring a stop request. The parent
		// context bounds failures; stop() never abandons uncertain publications.
		batch := replicatedBatch(topic, sequence)
		batchCtx, finish := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		result := client.ProduceSync(batchCtx, batch...)
		finish()
		now := time.Now().UTC()
		p.mu.Lock()
		for _, record := range result {
			if record.Err == nil {
				p.state.Acknowledged[record.Record.Partition]++
			}
		}
		p.state.err = result.FirstErr()
		p.state.MaxGapSeconds = max(p.state.MaxGapSeconds, now.Sub(last).Seconds())
		p.state.Batches++
		failed := p.state.err != nil
		p.mu.Unlock()
		if failed {
			return
		}
		last, sequence = now, sequence+1
	}
}

func replicatedBatch(topic string, sequence uint64) []*kgo.Record {
	batch := make([]*kgo.Record, replicatedPartitions)
	stamp := time.Now().Truncate(time.Millisecond)
	for partition := range replicatedPartitions {
		value := make([]byte, 2048)
		binary.BigEndian.PutUint64(value, sequence)
		binary.BigEndian.PutUint32(value[8:], uint32(partition))
		// Deterministic varied bytes make each full record unique and retain
		// enough payload to exercise worker byte accounting.
		for n := 12; n < len(value); n++ {
			value[n] = byte(uint64(n)*131 + sequence*17 + uint64(partition))
		}
		batch[partition] = &kgo.Record{Topic: topic, Partition: int32(partition),
			Key: []byte(fmt.Sprintf("%d/%d", partition, sequence)), Value: value, Timestamp: stamp,
			Headers: []kgo.RecordHeader{{Key: "duplicate", Value: nil}, {Key: "duplicate", Value: []byte{}}}}
	}
	return batch
}

func (p *replicatedPublisher) snapshot() replicatedPublishing {
	p.mu.Lock()
	defer p.mu.Unlock()
	state := p.state
	state.Observed = time.Now().UTC()
	state.Acknowledged = append([]int64(nil), state.Acknowledged...)
	state.Offered = append([]int64(nil), state.Offered...)
	return state
}

func (p *replicatedPublisher) stop() replicatedPublishing {
	p.cancel()
	<-p.done
	return p.snapshot()
}
