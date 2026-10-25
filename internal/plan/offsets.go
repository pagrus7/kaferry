package plan

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"strconv"

	"github.com/pagrus7/kaferry/internal/sql"
)

// OffsetRange uses original source offsets, with an inclusive lower and exclusive upper.
// Latest is a fixed source-end boundary; for archives it means the pinned selection's EOF.
type OffsetRange struct {
	Lower  *int64 `json:"lower,omitempty"`
	Upper  *int64 `json:"upper,omitempty"`
	Latest bool   `json:"latest,omitempty"`
}
type PartitionOffsetRange struct {
	Partition int32       `json:"partition"`
	Range     OffsetRange `json:"range"`
}

func (r OffsetRange) HasEnd() bool { return r.Upper != nil || r.Latest }
func (r OffsetRange) Contains(offset int64) bool {
	return (r.Lower == nil || offset >= *r.Lower) && (r.Upper == nil || offset < *r.Upper)
}
func (p Plan) OffsetRange(id int32) *OffsetRange {
	if p.Offsets != nil {
		return p.Offsets
	}
	if i, ok := slices.BinarySearchFunc(p.PartitionOffsets, id, func(r PartitionOffsetRange, id int32) int { return cmp.Compare(r.Partition, id) }); ok {
		return &p.PartitionOffsets[i].Range
	}

	return nil
}
func (p Plan) HasOffsetEnd() bool {
	// Every partition branch is validated to have an upper offset bound.
	return p.Offsets != nil && p.Offsets.HasEnd() || len(p.PartitionOffsets) > 0
}
func (p Plan) defaultDisorderMS() int64 {
	if p.Source.Kind != "TOPIC" || p.HasOffsetEnd() {
		return -1
	}
	return p.MaxStalenessMS
}
func (p Plan) DisorderMS() int64 {
	if p.MaxOutOfOrderMS != nil {
		return *p.MaxOutOfOrderMS
	}
	return p.defaultDisorderMS()
}
func (p Plan) Finite() bool {
	return p.Source.Kind == "ARCHIVE" || p.WorkerStopAfterRecords > 0 || p.HasOffsetEnd() || p.Window.Upper != nil && (p.MaxStalenessMS >= 0 || p.DisorderMS() >= 0)
}
func allowance(o sql.Option) (int64, error) {
	if o.Value == "OFF" && o.Interval == nil {
		return -1, nil
	}
	if o.Interval == nil {
		return 0, fmt.Errorf("%s requires an interval or OFF", o.Name)
	}
	return intervalMS(*o.Interval)
}
func normalizeOffsets(bounds []sql.OffsetBound) (*OffsetRange, error) {
	if len(bounds) == 0 {
		return nil, nil
	}
	r := &OffsetRange{}
	for _, b := range bounds {
		if b.Latest {
			if b.Operator != "<" {
				return nil, sql.At(b.Position, fmt.Errorf("LATEST requires RECORD_OFFSET < LATEST"))
			}
			if r.HasEnd() {
				return nil, sql.At(b.Position, fmt.Errorf("offset upper bound repeats; specify it once"))
			}
			r.Latest = true
			continue
		}
		n, err := strconv.ParseInt(b.Value, 10, 64)
		if err != nil || n < 0 {
			return nil, sql.At(b.ValuePosition, fmt.Errorf("record offset must be a non-negative signed 64-bit integer"))
		}
		if b.Operator == ">" || b.Operator == "<=" {
			if n == math.MaxInt64 {
				return nil, sql.At(b.Position, fmt.Errorf("offset boundary overflows; reduce the offset"))
			}
			n++
		}
		switch b.Operator {
		case ">", ">=":
			if r.Lower != nil {
				return nil, sql.At(b.Position, fmt.Errorf("offset lower bound repeats; specify it once"))
			}
			r.Lower = &n
		case "<", "<=":
			if r.HasEnd() {
				return nil, sql.At(b.Position, fmt.Errorf("offset upper bound repeats; specify it once"))
			}
			r.Upper = &n
		default:
			return nil, sql.At(b.Position, fmt.Errorf("unsupported offset comparison; use <, <=, >, or >="))
		}
	}
	if r.Lower != nil && r.Upper != nil && *r.Lower >= *r.Upper {
		return nil, sql.At(bounds[len(bounds)-1].Position, fmt.Errorf("offset range is empty; lower bound must precede upper bound"))
	}
	return r, nil
}
func (p *Plan) offsetCriteria(s sql.Statement) error {
	var err error
	p.Offsets, err = normalizeOffsets(s.OffsetBounds)
	if err != nil {
		return err
	}
	if len(s.OffsetBranches) == 0 {
		return nil
	}
	if p.Offsets != nil || p.Partitions != nil {
		return sql.At(s.WherePosition, fmt.Errorf("partition offset branches cannot be combined with shared partition or offset predicates"))
	}
	selected := &PartitionFilter{}
	seen := map[int32]bool{}
	for _, b := range s.OffsetBranches {
		if b.Partitions == nil || b.Partitions.Exclude {
			return sql.At(b.Position, fmt.Errorf("each offset branch requires PARTITION IN"))
		}
		parts, err := partitionFilter(b.Partitions)
		if err != nil {
			return err
		}
		r, err := normalizeOffsets(b.Bounds)
		if err != nil {
			return err
		}
		if r == nil || !r.HasEnd() {
			return sql.At(b.Position, fmt.Errorf("each partition offset branch requires an upper offset bound"))
		}
		for _, id := range parts.IDs {
			if seen[id] {
				return sql.At(b.Partitions.Position, fmt.Errorf("partition offset branches overlap; select each partition once"))
			}
			seen[id] = true
			selected.IDs = append(selected.IDs, id)
			p.PartitionOffsets = append(p.PartitionOffsets, PartitionOffsetRange{Partition: id, Range: *r})
		}
	}
	slices.Sort(selected.IDs)
	slices.SortFunc(p.PartitionOffsets, func(a, b PartitionOffsetRange) int { return cmp.Compare(a.Partition, b.Partition) })

	p.Partitions = selected
	return nil
}
