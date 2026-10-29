package franz

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/delivery"
	"github.com/pagrus7/kaferry/internal/kafkaport"
)

func (s *Source) lifecycle(event lifecycleEvent) error {
	if event.revoke {
		err := s.life.Revoke(event.ids, event.lost)
		err = errors.Join(err, s.delivery.Release(event.ids))
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
	saved, metadata, err := s.inspector.Saved(ctx, s.plan.Group, s.fingerprint, s.plan.Window, s.plan.MaxStalenessMS)
	if err != nil {
		event.answer <- lifecycleAnswer{err: err}
		return err
	}
	if err = s.delivery.Acquire(ctx, event.ids); err != nil {
		event.answer <- lifecycleAnswer{err: err}
		return err
	}
	native := saved
	nativeMetadata := metadata
	saved, metadata, err = authoritativeSaved(s.delivery, event.ids, s.plan, s.fingerprint, s.topicID)
	if err != nil {
		event.answer <- lifecycleAnswer{err: err}
		return err
	}
	for _, id := range event.ids {
		if _, present := saved[id]; !present {
			if _, old := native[id]; old {
				err = conflict("source offsets exist without an exactly-once checkpoint")
				event.answer <- lifecycleAnswer{err: err}
				return err
			}
		}
	}
	// The lowest initialized partition establishes START policy and the window
	// for new assignments. Its authority survives source-group offset expiry.
	lowest := int32(-1)
	for id := range saved {
		if lowest < 0 || id < lowest {
			lowest = id
		}
	}
	parts := s.inspector.PartitionIDs()
	slices.Sort(parts)
	for _, id := range parts {
		if lowest >= 0 && id >= lowest {
			break
		}
		assigned := false
		for _, current := range event.ids {
			if id == current {
				assigned = true
				break
			}
		}
		if assigned {
			continue
		}
		frame, e := s.delivery.Backend.Read(ctx, id)
		if e != nil {
			event.answer <- lifecycleAnswer{err: e}
			return e
		}
		if frame == nil {
			continue
		}
		positions, progress, e := kafkaport.AuthoritativeSaved(map[int32]*delivery.Frame{id: frame}, s.plan, s.fingerprint, s.topicID)
		if e != nil {
			event.answer <- lifecycleAnswer{err: e}
			return e
		}
		saved[id] = positions[id]
		metadata[id] = progress[id]
		break
	}
	for id, progress := range nativeMetadata {
		if _, owned := metadata[id]; !owned && progress.Window != nil {
			metadata[id] = progress
		}
	}
	positioner := s.positioning.New(s.inspector, s.inspector, s.plan, s.fingerprint, s.topicID)
	positioned, err := positioner.Locate(ctx, event.ids, saved, metadata)
	if err != nil {
		event.answer <- lifecycleAnswer{err: err}
		return err
	}
	// Save the resolved window/end before any advisory source-group offset.
	initial := []delivery.Frame{}
	for _, a := range positioned.Assignments {
		if _, exists := saved[a.Cursor.Partition]; !exists {
			progress := positioned.Progress[a.Cursor.Partition]
			progress.Stop = a.Cursor.Stop
			progress.End = a.Cursor.End
			text, e := s.store.codec.Encode(progress)
			if e != nil {
				event.answer <- lifecycleAnswer{err: e}
				return e
			}
			initial = append(initial, delivery.Frame{Cursor: a.Cursor, Metadata: text})
		}
	}
	if err = s.delivery.CommitBatch(ctx, initial); err != nil {
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
