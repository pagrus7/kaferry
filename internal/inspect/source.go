package inspect

import (
	"context"
	"slices"

	"github.com/pagrus7/kaferry/internal/config"
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/plan"
)

func (s *Service) source(ctx context.Context, p plan.Plan, limits config.Worker, connection kafkaport.InspectionConnection, report *Report) error {
	if p.Source.Kind == "FILE" {
		info, err := s.files.Source(p, report.Fingerprint, limits)
		if err != nil {
			return err
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
		return nil
	}
	return s.topicSource(ctx, p, connection, report)
}

func (s *Service) topicSource(ctx context.Context, p plan.Plan, connection kafkaport.InspectionConnection, report *Report) error {
	saved, metadata := map[int32]int64{}, map[int32]kafkaport.Progress{}
	if !p.OneOff {
		if err := connection.CheckMembers(ctx, p.Group); err != nil {
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
	report.Source.PartitionCount = len(topic.Partitions)
	slices.Sort(topic.Partitions)
	located, err := s.positioning.New(connection, connection, p, report.Fingerprint, topic.TopicID).Locate(ctx, topic.Partitions, saved, metadata)
	if err != nil {
		return err
	}
	var warnings []string
	report.Partitions, warnings = partitionRows(p, located, saved, metadata)
	report.Warnings = append(report.Warnings, warnings...)
	return nil
}
