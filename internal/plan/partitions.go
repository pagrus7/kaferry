package plan

import (
	"fmt"
	"slices"
	"strconv"

	"github.com/pagrus7/kaferry/internal/sql"
)

// PartitionFilter is a source predicate, independent of current topic topology.
type PartitionFilter struct {
	Exclude bool    `json:"exclude"`
	IDs     []int32 `json:"ids"`
}

func partitionFilter(ast *sql.PartitionFilter, kind string) (*PartitionFilter, error) {
	if ast == nil {
		return nil, nil
	}
	if kind != "TOPIC" {
		return nil, sql.At(ast.Position, fmt.Errorf("PARTITION requires a topic source"))
	}
	if len(ast.IDs) == 0 {
		return nil, sql.At(ast.Position, fmt.Errorf("PARTITION requires a nonempty integer list"))
	}
	f := &PartitionFilter{Exclude: ast.Exclude}
	for _, id := range ast.IDs {
		value, err := strconv.ParseInt(id.Value, 10, 32)
		if err != nil || value < 0 {
			return nil, sql.At(id.Position, fmt.Errorf("PARTITION requires integers from 0 to 2147483647"))
		}
		f.IDs = append(f.IDs, int32(value))
	}
	slices.Sort(f.IDs)
	f.IDs = slices.Compact(f.IDs)
	return f, nil
}

func (f *PartitionFilter) Matches(id int32) bool {
	return f == nil || slices.Contains(f.IDs, id) != f.Exclude
}

// Select validates every listed ID, including exclusions, against source metadata.
func (f *PartitionFilter) Select(available []int32) ([]int32, error) {
	if f != nil {
		for _, id := range f.IDs {
			if !slices.Contains(available, id) {
				return nil, fmt.Errorf("PARTITION predicate names an ID absent from source metadata")
			}
		}
	}
	selected := make([]int32, 0, len(available))
	for _, id := range available {
		if f.Matches(id) {
			selected = append(selected, id)
		}
	}
	slices.Sort(selected)
	return selected, nil
}
