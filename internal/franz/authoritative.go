package franz

import (
	"github.com/pagrus7/kaferry/internal/delivery"
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/plan"
)

func authoritativeSaved(s *delivery.Session, ids []int32, p plan.Plan, fp, tid string) (map[int32]int64, map[int32]kafkaport.Progress, error) {
	frames := make(map[int32]*delivery.Frame)
	for _, id := range ids {
		if part := s.Partition(id); part != nil {
			frames[id] = part.Saved()
		}
	}
	return kafkaport.AuthoritativeSaved(frames, p, fp, tid)
}
