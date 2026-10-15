package copy

import (
	"time"

	"github.com/pagrus7/kaferry/internal/config"
)

type submission struct {
	input     Input
	token     Token
	busySince time.Time
}

// workCounts is the progress information needed to enforce flow limits.
type workCounts interface {
	Total() (int, int64)
	Work(int32) (int, int64)
}

// flow holds unsent input and decides when work limits require a pause or save.
type flow struct {
	pending     *submission
	deferred    *Input
	deferredEOF *Cursor
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
	if f.deferred != nil && f.deferred.Cursor.Partition == id {
		f.deferred, f.deferredEOF = nil, nil
	}
	if f.pending != nil && f.pending.token.Partition == id {
		f.pending = nil
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
	return n >= limits.RecordCap || b >= limits.ByteCap || f.pending != nil || f.deferred != nil
}

func (f *flow) commitDue(now time.Time, work workCounts, partitions map[int32]*partition, limits config.Worker) bool {
	if f.deferred != nil || now.Sub(f.lastCommit) >= time.Second {
		return true
	}
	n, b := work.Total()
	if n*2 >= limits.RecordCap || b*2 >= limits.ByteCap {
		return true
	}
	for id := range partitions {
		n, b := work.Work(id)
		if n*2 >= limits.PartitionRecordCap || b*2 >= limits.PartitionByteCap {
			return true
		}
	}
	return false
}

func (r *Runner) shouldCommit() bool {
	return r.flow.commitDue(r.clock.Now(), r.tracker, r.partitions, r.limits)
}

func (r *Runner) pause() error {
	full := r.flow.full(r.tracker, r.limits)
	for id, p := range r.partitions {
		n, b := r.tracker.Work(id)
		paused := full || p.stopped || n >= r.limits.PartitionRecordCap || b >= r.limits.PartitionByteCap
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

func (r *Runner) admitDeferred() error {
	if r.deferred == nil || !r.admits(*r.deferred) {
		return nil
	}
	input, eof := *r.deferred, r.deferredEOF
	r.deferred, r.deferredEOF = nil, nil
	if err := r.input(input); err != nil {
		return err
	}
	if eof != nil {
		return r.eof(*eof)
	}
	return nil
}
