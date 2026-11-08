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
	Observed      time.Time `json:"observed"`
	Started       time.Time `json:"started"`
	Stopped       time.Time `json:"stopped"`
	Acknowledged  []int64   `json:"acknowledged_by_partition"`
	Batches       int       `json:"completed_batches"`
	MaxGapSeconds float64   `json:"max_acknowledgement_gap_seconds"`
	err           error
}

func startReplicatedPublisher(ctx context.Context, client *kgo.Client, topic string) *replicatedPublisher {
	ctx, cancel := context.WithCancel(ctx)
	p := &replicatedPublisher{cancel: cancel, done: make(chan struct{}),
		state: replicatedPublishing{Started: time.Now().UTC(), Acknowledged: make([]int64, replicatedPartitions)}}
	go func() {
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
			// Finish a submitted batch before honoring a stop request. The parent
			// context bounds failures; stop() never abandons uncertain publications.
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
	}()
	return p
}

func (p *replicatedPublisher) snapshot() replicatedPublishing {
	p.mu.Lock()
	defer p.mu.Unlock()
	state := p.state
	state.Observed = time.Now().UTC()
	state.Acknowledged = append([]int64(nil), state.Acknowledged...)
	return state
}

func (p *replicatedPublisher) stop() replicatedPublishing {
	p.cancel()
	<-p.done
	return p.snapshot()
}
