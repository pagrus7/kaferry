package kafka

import (
	"context"

	core "github.com/pagrus7/kaferry/src/internal/copy"
)

func (p *positioner) resolveOffsetRanges(ctx context.Context, assignments []core.Assignment, saved map[int32]SavedProgress, ends map[int32]int64) error {
	if p.plan.Offsets == nil && len(p.plan.PartitionOffsets) == 0 {
		return nil
	}
	latest, err := p.offsetEndLookups(assignments, saved, ends)
	if err != nil {
		return err
	}

	if len(latest) > 0 {
		fresh, err := p.lookup.ReadableEnd(ctx, latest)
		if err != nil {
			return err
		}
		for id, end := range fresh {
			ends[id] = end
		}
	}
	for i := range assignments {
		if err := p.applyOffsetRange(&assignments[i], saved, ends); err != nil {
			return err
		}
	}
	return nil
}

func (p *positioner) offsetEndLookups(assignments []core.Assignment, saved map[int32]SavedProgress, ends map[int32]int64) ([]int32, error) {
	var latest []int32
	// A timestamp start with no match and the end share a readable-end sample.
	for _, a := range assignments {
		id := a.Cursor.Partition
		r := p.plan.OffsetRange(id)
		if _, resuming := saved[id]; resuming && r.HasEnd() && a.Cursor.End == nil {
			return nil, conflict("partition %d saved offset range is missing its end", id)
		}
		if r.Upper != nil && a.Cursor.End != nil && *r.Upper != *a.Cursor.End {
			return nil, conflict("partition %d saved offset end differs from query", id)
		}
		if r.HasEnd() {
			if _, sampled := ends[id]; !sampled {
				latest = append(latest, id)
			}
		}
	}
	return latest, nil
}
func (p *positioner) applyOffsetRange(a *core.Assignment, saved map[int32]SavedProgress, ends map[int32]int64) error {
	id := a.Cursor.Partition
	r := p.plan.OffsetRange(id)
	if r.HasEnd() {
		p.high[id] = max(p.high[id], ends[id])
	}
	_, resuming := saved[id]
	if !resuming && r.Lower != nil {
		if *r.Lower < p.low[id] || *r.Lower > p.high[id] {
			return conflict("partition %d requested offset start is outside retained positions", id)
		}
		a.Cursor.Position = max(a.Cursor.Position, *r.Lower)
	}
	return p.applyOffsetEnd(a, resuming, ends[id])
}
func (p *positioner) applyOffsetEnd(a *core.Assignment, resuming bool, end int64) error {
	id := a.Cursor.Partition
	r := p.plan.OffsetRange(id)
	if a.Cursor.End != nil {
		if *a.Cursor.End > end {
			return conflict("partition %d saved offset end exceeds the readable end", id)
		}
		return nil
	}
	if !r.HasEnd() {
		return nil
	}
	if r.Upper != nil {
		if *r.Upper > end {
			return conflict("partition %d requested offset end %d exceeds the readable end %d. Lower the requested end or retry when more offsets are readable", id, *r.Upper, end)
		}
		end = *r.Upper
	}
	if end < p.low[id] || end > p.high[id] || r.Lower != nil && *r.Lower > end {
		return conflict("partition %d requested offset end is outside retained positions", id)
	}
	a.Cursor.End = &end
	if !resuming {
		a.Cursor.Position = min(a.Cursor.Position, end)
	}
	return nil
}
