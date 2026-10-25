package inspect

import (
	"context"

	"github.com/pagrus7/kaferry/internal/config"
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/plan"
)

func (s *Service) source(ctx context.Context, p plan.Plan, limits config.Worker, connection kafkaport.InspectionConnection, report *Report) ([]int32, error) {
	if p.Source.Kind == "ARCHIVE" {
		info, err := s.files.Source(p, report.Fingerprint, limits)
		if err != nil {
			return nil, err
		}
		report.Source.File = fileSummary(info.ArchiveInspection)
		report.Source.File.Window = &info.Window
		report.Source.File.PositionRecords = &info.Position
		report.Warnings = append(report.Warnings, info.Warnings...)
		report.Source.File.Checkpoint, report.Source.File.CheckpointExists = info.CheckpointPath, &info.Saved
		report.Source.File.WindowOrigin = "literal"
		if p.Window.Relative() {
			report.Source.File.WindowOrigin = "resolved"
			if info.Saved {
				report.Source.File.WindowOrigin = "saved"
			}
		}
		return info.Partitions, nil
	}
	if err := s.topicSource(ctx, p, connection, report); err != nil {
		return nil, err
	}
	ids := make([]int32, len(report.Partitions))
	for i, row := range report.Partitions {
		ids[i] = row.ID
	}
	return ids, nil
}

func (s *Service) topicSource(ctx context.Context, p plan.Plan, connection kafkaport.InspectionConnection, report *Report) error {
	if p.Partitions != nil {
		topic, err := connection.Describe(ctx, false)
		if err != nil {
			return err
		}
		if _, err = p.Partitions.Select(topic.Partitions); err != nil {
			return &kafkaport.Conflict{Err: err}
		}
	}
	saved, metadata := map[int32]int64{}, map[int32]kafkaport.Progress{}
	if !p.OneOff {
		if err := connection.CheckMembers(ctx, p.Group, kafkaport.GroupProtocol(p, report.Fingerprint)); err != nil {
			return err
		}
		var err error
		saved, metadata, err = connection.Saved(ctx, p.Group, report.Fingerprint, p.Window, p.CutoffMS)
		if err != nil {
			return err
		}
	}
	if p.Window.Lower != nil || p.Window.Upper != nil {
		report.StartRule = "WINDOW"
	} else if len(saved) > 0 {
		report.StartRule = "EARLIEST"
	}
	topic, err := connection.Describe(ctx, false)
	if err != nil {
		return err
	}
	selected, err := p.Partitions.Select(topic.Partitions)
	if err != nil {
		return &kafkaport.Conflict{Err: err}
	}
	report.Source.PartitionCount = len(topic.Partitions)
	located, err := s.positioning.New(connection, connection, p, report.Fingerprint, topic.TopicID).Locate(ctx, selected, saved, metadata)
	if err != nil {
		return err
	}
	var warnings []string
	report.Partitions, warnings = partitionRows(p, located, saved, metadata)
	report.Warnings = append(report.Warnings, warnings...)
	return nil
}
