// Package capture writes durable, independently recoverable Avro archive files.
package capture

import (
	"errors"
	"time"

	"github.com/pagrus7/kaferry/internal/archive/avro"
)

type Clock interface{ Now() time.Time }

type Policy struct {
	RotateAfterSize int64
	RotateAfterAge  time.Duration
	MaxOpenFiles    int
}

type partition struct {
	segment Segment
	last    int64
	opened  time.Time
	touched uint64
}

// Capture applies rotation policy; its caller acknowledges only successful Flush.
type Capture struct {
	store    Segments
	clock    Clock
	policy   Policy
	active   map[avro.Source]partition
	failed   error
	sequence uint64
}

func New(store Segments, clock Clock, policy Policy) (*Capture, error) {
	if policy.RotateAfterSize == 0 {
		policy.RotateAfterSize = 1 << 30
	}
	if policy.RotateAfterAge == 0 {
		policy.RotateAfterAge = time.Hour
	}
	if policy.MaxOpenFiles == 0 {
		policy.MaxOpenFiles = 1024
	}
	if policy.RotateAfterSize < 0 || policy.RotateAfterAge < 0 || policy.MaxOpenFiles < 0 {
		return nil, errors.New("archive rotation size, age and open-file cap must be positive")
	}
	return &Capture{store: store, clock: clock, policy: policy, active: make(map[avro.Source]partition)}, nil
}

func (c *Capture) Append(record avro.Record) error {
	if c.failed != nil {
		return c.failed
	}
	p, exists := c.active[record.Source]
	if exists && (record.Offset < p.last || c.clock.Now().Sub(p.opened) >= c.policy.RotateAfterAge) {
		if err := c.publish(record.Source); err != nil {
			return err
		}
		exists = false
	}
	if !exists {
		if len(c.active) >= c.policy.MaxOpenFiles {
			if err := c.publish(c.oldest()); err != nil {
				return err
			}
		}
		segment, err := c.store.Create(record.Source, record.Offset)
		if err != nil {
			return c.fail(err)
		}
		p = partition{segment: segment, opened: c.clock.Now()}
	}
	next, err := c.store.Append(p.segment.ID, record)
	if err != nil {
		var invalid *avro.InvalidRecord
		if !errors.As(err, &invalid) {
			return c.fail(err)
		}
		if p.segment.Records == 0 {
			if cleanup := c.store.Publish(p.segment.ID); cleanup != nil {
				return c.fail(cleanup)
			}
		}
		return err
	}
	p.segment, p.last = next, record.Offset
	c.sequence++
	p.touched = c.sequence
	c.active[record.Source] = p
	if next.Bytes >= c.policy.RotateAfterSize {
		return c.publish(record.Source)
	}
	return nil
}

// Flush must also be called while input is quiet so elapsed-age rotation runs.
func (c *Capture) Flush() error {
	if c.failed != nil {
		return c.failed
	}
	if err := c.store.Checkpoint(); err != nil {
		return c.fail(err)
	}
	now := c.clock.Now()
	for source, p := range c.active {
		if c.store.Size(p.segment.ID) >= c.policy.RotateAfterSize || now.Sub(p.opened) >= c.policy.RotateAfterAge {
			if err := c.publish(source); err != nil {
				return err
			}
		}
	}
	return nil
}

// Finish publishes every remaining nonempty file before the worker succeeds.
func (c *Capture) Finish() error {
	if err := c.Flush(); err != nil {
		return err
	}
	for source := range c.active {
		if err := c.publish(source); err != nil {
			return err
		}
	}
	return nil
}

func (c *Capture) Close() error {
	if c.failed == nil {
		c.failed = errors.New("archive capture is closed")
	}
	return c.store.Close()
}

func (c *Capture) publish(source avro.Source) error {
	if err := c.store.Publish(c.active[source].segment.ID); err != nil {
		return c.fail(err)
	}
	delete(c.active, source)
	return nil
}

func (c *Capture) fail(err error) error {
	c.failed = err
	return err
}

func (c *Capture) oldest() avro.Source {
	var oldest avro.Source
	sequence := ^uint64(0)
	for source, p := range c.active {
		if p.touched < sequence {
			oldest, sequence = source, p.touched
		}
	}
	return oldest
}
