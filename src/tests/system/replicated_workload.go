//go:build integration && fault

package compat

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/src/tests/system/faults"
	"github.com/twmb/franz-go/pkg/kgo"
)

const replicatedPartitions = 24

func replicatedBrokerProducer(t *testing.T, brokers string, delivery time.Duration) *kgo.Client {
	t.Helper()
	// The dedicated fixture proves its plain connection settings before faults.
	// A severed first API request must retry within the existing delivery bound,
	// rather than treating the EOF as a guessed TLS/SASL configuration error.
	// Restore the SDK-equivalent unlimited safe retries instead of the shared
	// fixture's five-try cap; the existing delivery bound still limits each write.
	diagnostic := &faults.SDKDiagnostics{}
	client := fixtureClient(t, brokers, kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.MaxBufferedRecords(2*replicatedPartitions), kgo.RecordDeliveryTimeout(delivery),
		kgo.AllowIdempotentProduceCancellation(), kgo.AlwaysRetryEOF(), kgo.RecordRetries(math.MaxInt), kgo.WithLogger(diagnostic))
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("source publisher SDK record_retries=%v metadata_min_age=%v delivery_timeout=%v:\n%s", client.OptValue(kgo.RecordRetries), client.OptValue(kgo.MetadataMinAge), client.OptValue(kgo.RecordDeliveryTimeout), diagnostic.Snapshot())
		}
	})
	return client
}

// replicatedPublisher schedules one record per partition every 100ms. The
// acknowledgement counts, rather than the scheduler, gate scenario transitions.
// Broker publication retains due slots while keeping one write per partition
// outstanding; stopping discards only slots that have never been offered.
type replicatedPublisher struct {
	cancel           context.CancelFunc
	done             chan struct{}
	mu               sync.Mutex
	state            replicatedPublishing
	lastAcknowledged []time.Time
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
	p.lastAcknowledged = make([]time.Time, replicatedPartitions)
	for partition := range p.lastAcknowledged {
		p.lastAcknowledged[partition] = p.state.Started
	}
	go p.runOffers(ctx, client, topic, delivery)
	return p
}

func (p *replicatedPublisher) runOffers(ctx context.Context, client *kgo.Client, topic string, delivery time.Duration) {
	defer close(p.done)
	var pending sync.WaitGroup
	busy := make([]bool, replicatedPartitions)
	next := make([]uint64, replicatedPartitions)
	wake := make(chan struct{}, 1)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	last := p.state.Started
offers:
	for {
		select {
		case <-ctx.Done():
			break offers
		case <-tick.C:
			now := time.Now().UTC()
			p.mu.Lock()
			p.state.MaxOfferGapSeconds = max(p.state.MaxOfferGapSeconds, now.Sub(last).Seconds())
			p.mu.Unlock()
			last = now
		case <-wake:
		}
		if ctx.Err() != nil || p.snapshot().err != nil {
			break
		}
		now := time.Now().UTC()
		// Counters represent elapsed schedule slots without retaining payloads.
		// A completion wakes this loop immediately instead of losing another
		// 100ms tick; slow sustained acknowledgements still limit actual input.
		due := uint64(now.Sub(p.state.Started) / (100 * time.Millisecond))
		for partition := range replicatedPartitions {
			if next[partition] < due && p.offer(ctx, client, topic, next[partition], partition, now, busy, wake, &pending) {
				next[partition]++
			}
		}
	}

	// Cancellation or a permanent error ends schedule intake, including debt.
	// Already offered records keep their original delivery deadlines and must
	// all resolve within the existing drain bound before the final snapshot.
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

func (p *replicatedPublisher) offer(ctx context.Context, client *kgo.Client, topic string, sequence uint64, partition int, stamp time.Time, busy []bool, wake chan<- struct{}, pending *sync.WaitGroup) bool {
	p.mu.Lock()
	if busy[partition] || p.state.err != nil || ctx.Err() != nil {
		p.mu.Unlock()
		return false
	}
	busy[partition] = true
	p.state.Offered[partition]++
	p.mu.Unlock()
	pending.Add(1)
	client.TryProduce(context.WithoutCancel(ctx), replicatedRecord(topic, sequence, partition, stamp.Truncate(time.Millisecond)), func(_ *kgo.Record, err error) {
		defer pending.Done()
		p.mu.Lock()
		defer p.mu.Unlock()
		busy[partition] = false
		if err == nil {
			now := time.Now().UTC()
			p.state.MaxGapSeconds = max(p.state.MaxGapSeconds, now.Sub(p.lastAcknowledged[partition]).Seconds())
			p.lastAcknowledged[partition] = now
			p.state.Acknowledged[partition]++
		} else if p.state.err == nil {
			p.state.err = err
		}
		select {
		case wake <- struct{}{}:
		default:
		}
	})
	return true
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
		batch[partition] = replicatedRecord(topic, sequence, partition, stamp)
	}
	return batch
}

func replicatedRecord(topic string, sequence uint64, partition int, stamp time.Time) *kgo.Record {
	value := make([]byte, 2048)
	binary.BigEndian.PutUint64(value, sequence)
	binary.BigEndian.PutUint32(value[8:], uint32(partition))
	// Deterministic varied bytes make each full record unique and retain
	// enough payload to exercise worker byte accounting.
	for n := 12; n < len(value); n++ {
		value[n] = byte(uint64(n)*131 + sequence*17 + uint64(partition))
	}
	return &kgo.Record{Topic: topic, Partition: int32(partition),
		Key: []byte(fmt.Sprintf("%d/%d", partition, sequence)), Value: value, Timestamp: stamp,
		Headers: []kgo.RecordHeader{{Key: "duplicate", Value: nil}, {Key: "duplicate", Value: []byte{}}}}
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
