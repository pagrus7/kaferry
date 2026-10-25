package kafkaport

import (
	"context"

	core "github.com/pagrus7/kaferry/internal/copy"
)

func (p *positioner) resolveOffsetRanges(ctx context.Context, assignments []core.Assignment, saved map[int32]int64, ends map[int32]int64) error {
	if p.plan.Offsets == nil && len(p.plan.PartitionOffsets) == 0 {
		return nil
	}
	var latest []int32
	// START=LATEST and an offset end use the same sampled reply.
	for _, a := range assignments {
		id := a.Cursor.Partition
		r := p.plan.OffsetRange(id)
		if _, resuming := saved[id]; resuming && r.HasEnd() && a.Cursor.End == nil {
			return conflict("partition %d saved offset range is missing its end", id)
		}
		if r.Upper != nil && a.Cursor.End != nil && *r.Upper != *a.Cursor.End {
			return conflict("partition %d saved offset end differs from query", id)
		}
		if r.Latest && a.Cursor.End == nil {
			if _, sampled := ends[id]; !sampled {
				latest = append(latest, id)
			}
		}
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
		a := &assignments[i]
		id := a.Cursor.Partition
		r := p.plan.OffsetRange(id)
		if r.Latest && a.Cursor.End == nil {
			p.high[id] = max(p.high[id], ends[id])
		}
		_, resuming := saved[id]
		if !resuming && r.Lower != nil {
			if *r.Lower < p.low[id] || *r.Lower > p.high[id] {
				return conflict("partition %d requested offset start is outside retained positions", id)
			}
			a.Cursor.Position = max(a.Cursor.Position, *r.Lower)
		}
		if a.Cursor.End == nil && r.HasEnd() {
			end := ends[id]
			if r.Upper != nil {
				end = *r.Upper
			}
			if end < p.low[id] || end > p.high[id] || r.Lower != nil && *r.Lower > end {
				return conflict("partition %d requested offset end is outside retained positions", id)
			}
			a.Cursor.End = &end
			if !resuming {
				a.Cursor.Position = min(a.Cursor.Position, end)
			}
		}
	}
	return nil
}
