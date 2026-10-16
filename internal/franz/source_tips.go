package franz

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/kafkaport"
)

func (s *Source) pollTips(ctx context.Context) error {
	ends, err := s.inspector.Offsets(ctx, s.ownedIDs(), kafkaport.LatestOffset, true)
	if err != nil {
		return err
	}
	var gaps []int32
	for id, position := range s.positions {
		end, ok := ends[id]
		if !ok {
			return fmt.Errorf("offset response omitted partition %d", id)
		}
		if s.paused[id] {
			continue
		}
		if position >= end {
			s.readyEOF = append(s.readyEOF, core.Cursor{Partition: id, Position: position})
		} else if s.held[id] == nil {
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
	next, err := s.gaps.Next(ctx, id, s.positions[id])
	if err != nil {
		return err
	}
	s.positions[id] = next
	end, _ := ends[id]
	if next >= end {
		s.readyEOF = append(s.readyEOF, core.Cursor{Partition: id, Position: next})
	}
	return nil
}

func (s *Source) ownedIDs() []int32 {
	ids := make([]int32, 0, len(s.positions))
	for id := range s.positions {
		ids = append(ids, id)
	}
	return ids
}
func (s *Source) checkTips() error {
	if len(s.positions) == 0 || time.Since(s.lastTip) < time.Second {
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
