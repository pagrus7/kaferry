package replay

import (
	"errors"
	"time"

	"github.com/pagrus7/kaferry/internal/plan"
)

type Query struct {
	Group, Fingerprint string
	Window             plan.WindowSpec
	Partitions         *plan.PartitionFilter
	Offsets            *plan.OffsetRange
	PartitionOffsets   []plan.PartitionOffsetRange
	MaxOutOfOrderMS    *int64
	// PartitionCount bounds original IDs when replay requires an existing layout.
	// Zero leaves IDs unrestricted; the constraint is not persisted.
	PartitionCount int
}

// CheckSaved checks a structurally validated checkpoint against the query.
// It validates unfinished selections through inputs; completed selections need
// no files. It does not lock, save progress or create replay readers.
func (q Query) CheckSaved(saved State, inputs Snapshot) error {
	if saved.Group != q.Group || saved.Fingerprint != q.Fingerprint {
		return errors.New("replay checkpoint query identity mismatch")
	}
	if err := q.Window.ValidateStored(saved.Window, q.disorderAllowance()); err != nil {
		return err
	}
	if err := q.validatePartitions(saved.Files); err != nil {
		return err
	}
	if !saved.Complete {
		files := make([]File, len(saved.Files))
		for i, p := range saved.Files {
			files[i] = p.File
		}
		return inputs.Validate(files)
	}
	return nil
}

// NewWindow resolves a new group's window and validates its completion threshold.
func (q Query) NewWindow(now time.Time) (plan.Window, error) {
	window, err := q.Window.Resolve(now)
	if err != nil {
		return window, err
	}
	return window, q.Window.ValidateStored(window, q.disorderAllowance())
}

func (q Query) validatePartitions(files []Progress) error {
	if q.Partitions == nil && q.PartitionCount == 0 {
		return nil
	}
	for _, file := range files {
		id := file.File.Identity.Source.Partition
		if !q.Partitions.Matches(id) {
			return errors.New("replay selection contains an excluded original partition")
		}
		if q.PartitionCount != 0 {
			if err := plan.CheckPartition(id, q.PartitionCount); err != nil {
				return err
			}
		}
	}
	return nil
}

func (q Query) disorderAllowance() int64 {
	if q.MaxOutOfOrderMS != nil {
		return max(0, *q.MaxOutOfOrderMS)
	}
	return 0
}
