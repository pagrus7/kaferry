package inspect

import (
	"fmt"

	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/plan"
)

func partitionRows(p plan.Plan, located kafkaport.Positioning, saved map[int32]int64, metadata map[int32]kafkaport.Progress) ([]Partition, []string) {
	rows := make([]Partition, 0, len(located.Assignments))
	var warnings []string
	for _, assignment := range located.Assignments {
		id := assignment.Cursor.Partition
		position, exists := saved[id]
		origin := "literal"
		if p.Window.Relative() {
			origin = "resolved"
			if metadata[id].Window != nil {
				origin = "saved"
			} else if len(metadata) > 0 {
				origin = "adopted"
			}
		}
		rows = append(rows, Partition{ID: id, Start: assignment.Cursor.Position, Saved: exists, Window: assignment.Window, WindowOrigin: origin, LogStart: located.LogStart[id], LogEnd: located.LogEnd[id]})
		if exists && position < located.LogStart[id] {
			warnings = append(warnings, fmt.Sprintf("Partition %d: saved offset %d expired; start is log start %d.", id, position, located.LogStart[id]))
		} else if _, retention := located.Retention[id]; retention {
			warnings = append(warnings, fmt.Sprintf("Partition %d: earlier offsets are not retained; the start of the requested window may be unavailable.", id))
		}
	}
	if p.Window.Relative() && len(metadata) == 0 {
		warnings = append(warnings, "NOW() resolved for this preview; a run without saved progress resolves it again.")
	}
	return rows, warnings
}
