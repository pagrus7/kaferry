package copy

import (
	"context"
	"time"
)

// Snapshot describes this invocation. Counts include work replayed after a restart.
// PendingEntries includes cursor gaps and excludes an input deferred before tracking.
type Snapshot struct {
	Submitted, Confirmed, Filtered, Skipped        uint64
	Iterations                                     uint64
	PendingEntries, PendingEstimatedBytes          int64
	EntryCap, EstimatedByteCap                     int64
	Partitions, TimestampStops, TipStops, EOFStops int64
	WorkerRecordStops                              int64
}

type SaveResult struct {
	Outcome  string
	Duration time.Duration
	At       time.Time
}

// Observer receives immutable observations without performing network or file I/O.
type Observer interface {
	Publish(Snapshot)
	Saved(SaveResult)
}

// SaveProgress observes real saves, including initialization and metadata-only writes.
// It returns the store's results unchanged.
func SaveProgress(ctx context.Context, store ProgressStore, cursors []Cursor, observer Observer) (map[int32]error, error) {
	if observer == nil || len(cursors) == 0 {
		return store.Save(ctx, cursors)
	}
	start := time.Now()
	results, err := store.Save(ctx, cursors)
	ok := 0
	if err == nil {
		for _, cursor := range cursors {
			if failure, present := results[cursor.Partition]; present && failure == nil {
				ok++
			}
		}
	}
	outcome := "failure"
	if ok == len(cursors) {
		outcome = "success"
	} else if ok > 0 {
		outcome = "partial"
	}
	observer.Saved(SaveResult{Outcome: outcome, Duration: time.Since(start), At: time.Now()})
	return results, err
}

func (r *Runner) report(final bool) {
	now := r.clock.Now()
	metricsDue := r.observer != nil && (final || !now.Before(r.nextObservation))
	logDue := !final && r.progressInterval > 0 && !now.Before(r.nextProgress)
	if !metricsDue && !logDue {
		return
	}
	s := r.snapshot()
	if metricsDue {
		r.observer.Publish(s)
		r.nextObservation = now.Add(time.Second)
	}
	if logDue {
		r.log.Info("progress", "submitted", s.Submitted, "confirmed", s.Confirmed,
			"filtered", s.Filtered, "skipped", s.Skipped, "partitions", s.Partitions,
			"stopped", s.TimestampStops+s.TipStops+s.EOFStops+s.WorkerRecordStops,
			"pending_entries", s.PendingEntries, "pending_estimated_bytes", s.PendingEstimatedBytes)
		r.nextProgress = now.Add(r.progressInterval)
	}
}

func (r *Runner) snapshot() Snapshot {
	s := Snapshot{Submitted: r.submitted, Confirmed: r.confirmed, Filtered: r.filtered, Skipped: r.skipped,
		Iterations: r.iterations, EntryCap: int64(r.limits.RecordCap), EstimatedByteCap: r.limits.ByteCap,
		Partitions: int64(len(r.partitions))}
	if r.tracker != nil {
		n, b := r.tracker.Total()
		s.PendingEntries, s.PendingEstimatedBytes = int64(n), b
	}
	for _, p := range r.partitions {
		if p.stop == nil {
			continue
		}
		switch p.stop.Reason {
		case "timestamp":
			s.TimestampStops++
		case "tip":
			s.TipStops++
		case "eof":
			s.EOFStops++
		case "worker_record_cap":
			s.WorkerRecordStops++
		}
	}
	return s
}
