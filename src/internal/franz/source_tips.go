package franz

import (
	"context"
	"errors"
	"sort"
	"time"

	core "github.com/pagrus7/kaferry/src/internal/copy"
)

func (s *Source) pollTips(ctx context.Context) error {
	ends, err := s.lookup.ReadableEnd(ctx, s.ownedIDs())
	if err != nil {
		return err
	}
	var gaps []int32
	for id, p := range s.partitions {
		end := ends[id]
		if p.paused {
			continue
		}
		if p.position >= end {
			s.readyEOF = append(s.readyEOF, core.Cursor{Partition: id, Position: p.position})
		} else if p.held == nil {
			gaps = append(gaps, id)
		}
	}
	if len(gaps) == 0 {
		return nil
	}
	// Probe one partition per poll, rotating so quiet partitions all make progress.
	sort.Slice(gaps, func(i, j int) bool { return gaps[i] < gaps[j] })
	index := sort.Search(len(gaps), func(i int) bool { return gaps[i] > s.lastGap }) % len(gaps)
	id := gaps[index]
	s.lastGap = id
	next, err := s.gaps.Next(ctx, id, s.partitions[id].position)
	if err != nil {
		return err
	}
	s.partitions[id].position = next
	end, _ := ends[id]
	if next >= end {
		s.readyEOF = append(s.readyEOF, core.Cursor{Partition: id, Position: next})
	}
	return nil
}

func (s *Source) ownedIDs() []int32 {
	ids := make([]int32, 0, len(s.partitions))
	for id := range s.partitions {
		ids = append(ids, id)
	}
	return ids
}
func (s *Source) checkTips() error {
	if len(s.partitions) == 0 || time.Since(s.lastTip) < time.Second {
		return nil
	}
	s.lastTip = time.Now()
	ctx, cancel := context.WithTimeout(s.query, 2*time.Second)
	defer cancel()
	err := s.pollTips(ctx)
	var retry *core.Recoverable
	if errors.As(transientFailure(err), &retry) {
		return nil
	}
	return err
}
