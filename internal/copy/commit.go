package copy

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"
)

func (r *Runner) ids() []int32 {
	ids := make([]int32, 0, len(r.partitions))
	for id := range r.partitions {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (r *Runner) commit(ids []int32) error { return r.save(ids, false) }

// save commits confirmed progress. When releasing revoked partitions, a
// recoverable failure only replays work, so the partition is released anyway.
func (r *Runner) save(ids []int32, releasing bool) error {
	r.lastCommit = r.clock.Now()
	cursors := r.confirmedCursors(ids)
	if len(cursors) == 0 {
		return nil
	}
	r.stop.Stage("saving progress")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	results, err := SaveProgress(ctx, r.store, cursors, r.observer)
	if err != nil {
		if !r.released(releasing, cursors, err) {
			return err
		}
		return nil
	}
	var failed error
	for _, cursor := range cursors {
		err, present := results[cursor.Partition]
		if !present {
			err = fmt.Errorf("missing commit result for partition %d", cursor.Partition)
		}
		if err != nil {
			if !r.released(releasing, []Cursor{cursor}, err) {
				failed = errors.Join(failed, err)
			}
			continue
		}
		r.tracker.Committed(cursor)
		if cursor.End != nil {
			r.partitions[cursor.Partition].endSaved = true
		}
		if cursor.Stop != nil {
			r.partitions[cursor.Partition].stopSaved = true
		}
		r.log.Debug("progress committed", "partition", cursor.Partition, "position", cursor.Position)
	}
	return failed
}

func (r *Runner) confirmedCursors(ids []int32) []Cursor {
	var cursors []Cursor
	for _, id := range ids {
		p := r.partitions[id]
		endChanged := p.high != nil && !p.endSaved
		if !r.tracker.HasProgress(id) && (p.stop == nil || p.stopSaved) && !endChanged {
			continue
		}
		cursor := r.tracker.Confirmed(id)
		cursor.End = p.high
		stopped := p.stop != nil && cursor.Position >= p.stop.At
		if stopped {
			cursor.Stop = p.stop
		}
		if r.tracker.HasProgress(id) || stopped && !p.stopSaved || endChanged {
			cursors = append(cursors, cursor)
		}
	}
	return cursors
}

func (r *Runner) released(releasing bool, cursors []Cursor, err error) bool {
	var recoverable *Recoverable
	if !releasing || !errors.As(err, &recoverable) {
		return false
	}
	for _, cursor := range cursors {
		r.log.Warn("revoked partition released without saving progress; the next owner replays", "partition", cursor.Partition, "saved", r.tracker.Saved(cursor.Partition), "confirmed", cursor.Position, "reason", err.Error())
	}
	return true
}
