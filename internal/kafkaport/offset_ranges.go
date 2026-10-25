package kafkaport

import (
	"context"

	core "github.com/pagrus7/kaferry/internal/copy"
)

func (p *positioner) resolveOffsetRanges(ctx context.Context, assignments []core.Assignment, saved map[int32]int64) error {
	if p.plan.Offsets == nil && len(p.plan.PartitionOffsets) == 0 {
		return nil
	}
	var latest []int32
	// Reuse the readable-end lookup already used by a LATEST start or timestamp no-match.
	readableStarts := make(map[int32]bool, len(p.ends))
	for _, id := range p.ends {
		readableStarts[id] = true
	}
	reused := make(map[int32]int64)
	for _, a := range assignments {
		if r := p.plan.OffsetRange(a.Cursor.Partition); r != nil && r.Latest && a.Cursor.End == nil {
			if readableStarts[a.Cursor.Partition] {
				reused[a.Cursor.Partition] = a.Cursor.Position
			}

			if _, ok := reused[a.Cursor.Partition]; !ok {
				latest = append(latest, a.Cursor.Partition)
			}
		}
	}
	ends := make(map[int32]int64)
	if len(latest) > 0 {
		var err error
		ends, err = p.lookup.ReadableEnd(ctx, latest)
		if err != nil {
			return err
		}
	}
	for id, end := range reused {
		ends[id] = end
	}
	for i := range assignments {
		a := &assignments[i]
		id := a.Cursor.Partition
		r := p.plan.OffsetRange(id)
		if r == nil {
			continue
		}
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
		if a.Cursor.End != nil && a.Cursor.Position > *a.Cursor.End {
			return conflict("partition %d saved position exceeds its offset end", id)
		}
	}
	return nil
}
