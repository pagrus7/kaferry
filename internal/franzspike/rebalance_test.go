//go:build integration && franzspike

package franzspike

import (
	"fmt"
	"testing"
	"time"

	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/twmb/franz-go/pkg/kgo"
)

type lifeProbe struct {
	assignments int
	revoked     int
	src         *Source
	paused      bool
}

func (p *lifeProbe) Assign(as []core.Assignment) error {
	p.assignments += len(as)
	for _, a := range as {
		if p.paused {
			if err := p.src.Pause(a.Cursor.Partition, true); err != nil {
				return err
			}
		}
	}
	return nil
}
func (p *lifeProbe) Revoke(parts []int32, lost bool) error { p.revoked += len(parts); return nil }
func pollUntil(t *testing.T, s *Source, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !condition() {
		if _, err := s.Poll(20 * time.Millisecond); err != nil {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("lifecycle condition not reached")
		}
	}
}
func TestSpikePausedHeldRecordAllowsCooperativeRebalance(t *testing.T) {
	// Given two partitions owned by one source, with a fetched but unsubmitted record held behind a pause.
	src, dst := topic(t, 2), topic(t, 1)
	producer := client(t, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	for part := int32(0); part < 2; part++ {
		if err := producer.ProduceSync(ctx(t), &kgo.Record{Topic: src, Partition: part, Value: []byte("one")}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	p := spikePlan(t, fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' OPTIONS(GROUP='%s',START=EARLIEST)", dst, src, src+"-churn"))
	_, id, err := EndpointIdentity(ctx(t), p.Source)
	if err != nil {
		t.Fatal(err)
	}
	first, err := NewSource(p, "probe", id)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	life := &lifeProbe{src: first}
	if err := first.Start(life); err != nil {
		t.Fatal(err)
	}
	pollUntil(t, first, func() bool { return life.assignments == 2 })
	// Consume the fetch handoff directly without delivering it to the core.
	select {
	case result := <-first.fetched:
		if result.err != nil || len(result.records) == 0 {
			t.Fatal(result.err)
		}
		first.pending = &result
	case <-time.After(5 * time.Second):
		t.Fatal("no fetched record")
	}
	held := first.pending.records[0].Partition
	if err := first.Pause(held, true); err != nil {
		t.Fatal(err)
	}
	// When a second group member joins while the first keeps polling lifecycle events.
	second, err := NewSource(p, "probe", id)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	other := &lifeProbe{src: second, paused: true}
	if err := second.Start(other); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for life.revoked == 0 || other.assignments == 0 {
		if _, err := first.Poll(10 * time.Millisecond); err != nil {
			t.Fatal(err)
		}
		if _, err := second.Poll(10 * time.Millisecond); err != nil {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("pause blocked rebalance", life.revoked, other.assignments)
		}
	}
	// Then ownership changes without waiting for the held record to be published.
	if life.revoked != 1 || other.assignments != 1 {
		t.Fatal("unexpected cooperative assignment", life.revoked, other.assignments)
	}
	// If the held partition was retained, explicitly leave to exercise its revoke too.
	if _, owned := first.positions[held]; owned {
		_ = first.client.LeaveGroupContext(nil)
		pollUntil(t, first, func() bool { _, owned := first.positions[held]; return !owned })
	}
	if first.held[held] != nil || (first.pending != nil && len(first.pending.records) > 0 && first.pending.records[0].Partition == held) {
		t.Fatal("revoked held input survived")
	}
}
