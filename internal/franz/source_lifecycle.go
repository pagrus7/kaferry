package franz

import (
	"context"
	"fmt"
	"time"

	core "github.com/pagrus7/kaferry/internal/copy"
)

func (s *Source) lifecycle(event lifecycleEvent) error {
	if event.revoke {
		err := s.life.Revoke(event.ids, event.lost)
		for _, id := range event.ids {
			delete(s.positions, id)
			delete(s.paused, id)
			delete(s.held, id)
			delete(s.retention, id)
			delete(s.store.metadata, id)
			s.client.ResumeFetchPartitions(map[string][]int32{s.plan.Source.Name: {id}})
		}
		event.answer <- lifecycleAnswer{err: err}
		return err
	}
	ctx, cancel := context.WithTimeout(s.query, 10*time.Second)
	defer cancel()
	if s.routing != nil {
		if err := s.routing.ValidatePartitions(ctx, event.ids); err != nil {
			event.answer <- lifecycleAnswer{err: err}
			return err
		}
	}
	saved, metadata, err := s.inspector.Saved(ctx, s.plan.Group, s.fingerprint, s.plan.Window, s.plan.CutoffMS)
	if err != nil {
		event.answer <- lifecycleAnswer{err: err}
		return err
	}
	positioner := s.positioning.New(s.inspector, s.inspector, s.plan, s.fingerprint, s.topicID)
	positioned, err := positioner.Locate(ctx, event.ids, saved, metadata)
	if err != nil {
		event.answer <- lifecycleAnswer{err: err}
		return err
	}
	cursors := make([]core.Cursor, len(positioned.Assignments))
	for n, a := range positioned.Assignments {
		id := a.Cursor.Partition
		s.positions[id] = a.Cursor.Position
		s.store.metadata[id] = positioned.Progress[id]
		s.retention[id] = positioned.Retention[id]
		if _, ok := positioned.Retention[id]; !ok {
			delete(s.retention, id)
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
