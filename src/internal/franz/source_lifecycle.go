package franz

import (
	"context"
	"fmt"
	"time"

	core "github.com/pagrus7/kaferry/src/internal/copy"
)

func (s *Source) lifecycle(ctx context.Context, event lifecycleEvent) error {
	if event.revoke {
		err := s.life.Revoke(event.ids, event.lost)
		for _, id := range event.ids {
			delete(s.partitions, id)
			s.store.revoke(id)
			s.client.ResumeFetchPartitions(map[string][]int32{s.plan.Source.Name: {id}})
		}
		event.answer <- lifecycleAnswer{err: err}
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for _, id := range event.ids {
		if !s.plan.Partitions.Matches(id) {
			err := fmt.Errorf("assignment contains an excluded source partition")
			event.answer <- lifecycleAnswer{err: err}
			return err
		}
	}
	if s.routing != nil && len(event.ids) > 0 {
		if err := s.routing.ValidatePartitions(ctx, event.ids); err != nil {
			event.answer <- lifecycleAnswer{err: err}
			return err
		}
	}
	saved, err := s.inspector.Saved(ctx, s.plan.Group, s.fingerprint, s.plan.Window, s.plan.MaxStalenessMS)
	if err != nil {
		event.answer <- lifecycleAnswer{err: err}
		return err
	}
	positioner := s.positioning.New(s.inspector, s.inspector, s.plan, s.fingerprint, s.topicID)
	positioned, err := positioner.Locate(ctx, event.ids, saved)
	if err != nil {
		event.answer <- lifecycleAnswer{err: err}
		return err
	}
	cursors := make([]core.Cursor, len(positioned.Assignments))
	for n, a := range positioned.Assignments {
		id := a.Cursor.Partition
		p := &sourcePartition{position: a.Cursor.Position}
		s.partitions[id] = p
		s.store.assign(id, positioned.Progress[id])
		if check, ok := positioned.Retention[id]; ok {
			p.retention = &check
		}
		cursors[n] = a.Cursor
	}
	// Let the group callback finish before committing with the member generation.
	event.answer <- lifecycleAnswer{assignments: positioned.Assignments}
	results, err := core.SaveProgress(ctx, s.store, cursors, s.observer)
	if err != nil {
		return err
	}
	for _, cursor := range cursors {
		failure, present := results[cursor.Partition]
		if !present {
			return fmt.Errorf("missing initial commit result for partition %d", cursor.Partition)
		}
		if failure != nil {
			return failure
		}
	}
	return s.life.Assign(positioned.Assignments)
}
