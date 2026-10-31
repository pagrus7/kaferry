package copy

import (
	"time"

	"github.com/pagrus7/kaferry/internal/config"
)

// workCounts is the progress information needed to enforce flow limits.
type workCounts interface {
	Total() (int, int64)
	Work(int32) (int, int64)
}

type deferredInput struct {
	input Input
	eof   *Cursor
}

// flow owns input waiting for admission, including its following EOF. Such input
// has no tracker token yet. Runner owns submissions already admitted to tracking.
type flow struct {
	deferred    *deferredInput
	peakRecords int
	peakBytes   int64
	lastCommit  time.Time
}

func (f *flow) observe(work workCounts) {
	n, b := work.Total()
	f.peakRecords = max(f.peakRecords, n)
	f.peakBytes = max(f.peakBytes, b)
}

func (f *flow) revoke(id int32) {
	if f.deferred != nil && f.deferred.input.Cursor.Partition == id {
		f.deferred = nil
	}
}

func (f *flow) admits(input Input, work workCounts, limits config.Worker) bool {
	n, b := work.Total()
	if n == 0 {
		return true // One oversized record is admitted alone.
	}
	pn, pb := work.Work(input.Cursor.Partition)
	size := input.Record.Size()
	return n < limits.RecordCap && pn < limits.PartitionRecordCap && size <= limits.ByteCap-b && size <= limits.PartitionByteCap-pb
}

func (f *flow) full(work workCounts, limits config.Worker) bool {
	n, b := work.Total()
	return n >= limits.RecordCap || b >= limits.ByteCap || f.deferred != nil
}

func (f *flow) commitDue(now time.Time, work workCounts, partitions map[int32]*partition, limits config.Worker) bool {
	if f.deferred != nil || now.Sub(f.lastCommit) >= time.Second {
		return true
	}
	n, b := work.Total()
	if n >= limits.RecordCap || b >= limits.ByteCap {
		return true
	}
	for id := range partitions {
		n, b := work.Work(id)
		if n >= limits.PartitionRecordCap || b >= limits.PartitionByteCap {
			return true
		}
	}
	return false
}

func (r *Runner) shouldCommit() bool {
	return r.flow.commitDue(r.clock.Now(), r.tracker, r.partitions, r.limits)
}

func (r *Runner) pause() error {
	full := r.pending != nil || r.flow.full(r.tracker, r.limits)
	for id, p := range r.partitions {
		n, b := r.tracker.Work(id)
		paused := full || p.stop != nil || n >= r.limits.PartitionRecordCap || b >= r.limits.PartitionByteCap
		if paused != p.paused {
			if err := r.source.Pause(id, paused); err != nil {
				return err
			}
			p.paused = paused
		}
	}
	return nil
}

func (r *Runner) admits(input Input) bool {
	return r.flow.admits(input, r.tracker, r.limits)
}

// deferEOF keeps a tip behind the input whose capacity is not yet available.
func (f *flow) deferEOF(cursor Cursor) bool {
	if f.deferred == nil || f.deferred.input.Cursor.Partition != cursor.Partition {
		return false
	}
	f.deferred.eof = &cursor
	return true
}

// takeDeferred releases input and its EOF together, only when the input fits.
func (f *flow) takeDeferred(work workCounts, limits config.Worker) *deferredInput {
	if f.deferred == nil || !f.admits(f.deferred.input, work, limits) {
		return nil
	}
	event := f.deferred
	f.deferred = nil
	return event
}

func (r *Runner) admitDeferred() error {
	event := r.flow.takeDeferred(r.tracker, r.limits)
	if event == nil {
		return nil
	}
	if err := r.input(event.input); err != nil {
		return err
	}
	if event.eof != nil {
		return r.eof(*event.eof)
	}
	return nil
}
