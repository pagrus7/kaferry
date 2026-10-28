package kafkaport

import (
	"github.com/pagrus7/kaferry/internal/commitmeta"
	"github.com/pagrus7/kaferry/internal/delivery"
	"github.com/pagrus7/kaferry/internal/plan"
)

func AuthoritativeSaved(frames map[int32]*delivery.Frame, p plan.Plan, fp, tid string) (map[int32]int64, map[int32]Progress, error) {
	positions := make(map[int32]int64)
	metadata := make(map[int32]Progress)
	for id, f := range frames {
		if f == nil {
			continue
		}
		progress, err := DecodeProgress(commitmeta.NewCodec(), &f.Metadata, fp, tid, p.Window.Relative())
		if err != nil {
			return nil, nil, err
		}
		if err = ValidateWindow(progress, p.Window, p.MaxStalenessMS); err != nil {
			return nil, nil, err
		}
		positions[id] = f.Cursor.Position
		metadata[id] = progress
	}
	return positions, metadata, nil
}
