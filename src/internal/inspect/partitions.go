package inspect

import (
	"fmt"

	"github.com/pagrus7/kaferry/src/internal/kafkaport"
	"github.com/pagrus7/kaferry/src/internal/plan"
)

func partitionRows(p plan.Plan, located kafkaport.Positioning, saved map[int32]kafkaport.SavedProgress) ([]Partition, []string) {
	rows := make([]Partition, 0, len(located.Assignments))
	var warnings []string
	for _, assignment := range located.Assignments {
		id := assignment.Cursor.Partition
		progress, exists := saved[id]
		origin := windowOrigin(p.Window, progress.Window != nil, len(saved) > 0)
		endOrigin := offsetEndOrigin(p, id, assignment.Cursor.End, progress)

		rows = append(rows, Partition{EndOrigin: endOrigin, ID: id, Start: assignment.Cursor.Position, End: assignment.Cursor.End, Saved: exists, Window: assignment.Window, WindowOrigin: origin, LogStart: located.LogStart[id], LogEnd: located.LogEnd[id]})
		if exists && progress.Position < located.LogStart[id] {
			warnings = append(warnings, fmt.Sprintf("Partition %d: saved offset %d expired; start is log start %d.", id, progress.Position, located.LogStart[id]))
		} else if _, retention := located.Retention[id]; retention {
			warnings = append(warnings, fmt.Sprintf("Partition %d: earlier offsets are not retained; the start of the requested window may be unavailable.", id))
		}
	}
	return rows, warnings
}

func windowOrigin(window plan.WindowSpec, saved, hasSaved bool) string {
	if !window.Relative() {
		return "literal"
	}
	if saved {
		return "saved"
	}
	if hasSaved {
		return "adopted"
	}
	return "resolved"
}
func offsetEndOrigin(p plan.Plan, id int32, end *int64, progress kafkaport.SavedProgress) string {
	if end == nil {
		return ""
	}
	if progress.End != nil {
		return "saved"
	}
	if r := p.OffsetRange(id); r != nil && r.Latest {
		return "preview"
	}
	return "literal"
}
