//go:build integration && load

package compat

import (
	"sync"
	"time"
)

type observedDelivery struct {
	Backlog            int      `json:"backlog"`
	PeakBacklog        int      `json:"peak_backlog"`
	TimedReceipts      int      `json:"timed_receipts"`
	MaxLatencySeconds  *float64 `json:"max_latency_seconds"`
	P95LatencySeconds  *float64 `json:"p95_latency_seconds"`
	P99LatencySeconds  *float64 `json:"p99_latency_seconds"`
	P999LatencySeconds *float64 `json:"p999_latency_seconds"`
	Unsettled          int      `json:"unsettled"`
}

type deliveryTiming struct {
	start                  time.Time
	acknowledged, received bool
}

// deliveryObserver linearizes source callbacks and verifier receipts at this host.
// It retains only IDs awaiting an acknowledgement or a first receipt.
type deliveryObserver struct {
	mu          sync.Mutex
	now         func() time.Time
	pending     map[int]deliveryTiming
	backlog     int
	peakBacklog int
	receipts    int
	maxLatency  time.Duration
	latencies   latencyHistogram
}

func newDeliveryObserver(now func() time.Time) *deliveryObserver {
	return &deliveryObserver{now: now, pending: make(map[int]deliveryTiming)}
}

func (o *deliveryObserver) submit(id int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.pending[id] = deliveryTiming{start: o.now()}
}

func (o *deliveryObserver) publication(id int, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	entry, exists := o.pending[id]
	if !exists {
		return
	}
	if err != nil {
		if entry.acknowledged && !entry.received {
			o.backlog--
		}
		delete(o.pending, id)
		return
	}
	if entry.acknowledged {
		return
	}
	if entry.received {
		delete(o.pending, id)
		return
	}
	entry.acknowledged = true
	o.pending[id] = entry
	o.backlog++
	o.peakBacklog = max(o.peakBacklog, o.backlog)
}

func (o *deliveryObserver) receive(id int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	entry, exists := o.pending[id]
	if !exists || entry.received {
		return
	}
	o.receipts++
	latency := max(0, o.now().Sub(entry.start))
	o.maxLatency = max(o.maxLatency, latency)
	o.latencies.record(latency)
	if entry.acknowledged {
		o.backlog--
		delete(o.pending, id)
		return
	}
	entry.received = true
	o.pending[id] = entry
}

func (o *deliveryObserver) snapshot() observedDelivery {
	o.mu.Lock()
	defer o.mu.Unlock()
	result := observedDelivery{Backlog: o.backlog, PeakBacklog: o.peakBacklog,
		TimedReceipts: o.receipts, Unsettled: len(o.pending)}
	if o.receipts != 0 {
		seconds := o.maxLatency.Seconds()
		result.MaxLatencySeconds = &seconds
		percentiles := o.latencies.percentiles(o.receipts, o.maxLatency)
		result.P95LatencySeconds, result.P99LatencySeconds, result.P999LatencySeconds = &percentiles[0], &percentiles[1], &percentiles[2]
	}
	return result
}
