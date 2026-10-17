package inspect

import (
	"context"
	"fmt"
	"slices"

	"github.com/pagrus7/kaferry/internal/config"
	"github.com/pagrus7/kaferry/internal/file"
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/plan"
)

func (s *Service) source(ctx context.Context, p plan.Plan, limits config.Worker, connection kafkaport.InspectionConnection, report *Report) (file.DestinationProgress, error) {
	if p.Source.Kind == "FILE" {
		info, err := s.files.Source(p, report.Fingerprint, limits)
		if err != nil {
			return file.DestinationProgress{}, err
		}
		report.Source.File = fileSummary(info.ArchiveInspection)
		report.Source.File.StartBytes, report.Source.File.Window = &info.Position, &info.Window
		report.Source.File.Checkpoint, report.Source.File.CheckpointExists = info.CheckpointPath, &info.Saved
		if p.Window.Relative() && !info.Saved {
			report.Warnings = append(report.Warnings, "NOW() resolved for this preview; a run without saved progress resolves it again.")
		}
		return info.Output, nil
	}
	return s.topicSource(ctx, p, connection, report)
}

func (s *Service) topicSource(ctx context.Context, p plan.Plan, connection kafkaport.InspectionConnection, report *Report) (file.DestinationProgress, error) {
	saved, metadata := map[int32]int64{}, map[int32]kafkaport.Progress{}
	if !p.OneOff {
		if err := connection.CheckMembers(ctx, p.Group); err != nil {
			return file.DestinationProgress{}, err
		}
		var err error
		saved, metadata, err = connection.Saved(ctx, p.Group, report.Fingerprint, p.Window, p.CutoffMS)
		if err != nil {
			return file.DestinationProgress{}, err
		}
	}
	topic, err := connection.Describe(ctx, false)
	if err != nil {
		return file.DestinationProgress{}, err
	}
	report.Source.PartitionCount = len(topic.Partitions)
	slices.Sort(topic.Partitions)
	located, err := s.positioning.New(connection, connection, p, report.Fingerprint, topic.TopicID).Locate(ctx, topic.Partitions, saved, metadata)
	if err != nil {
		return file.DestinationProgress{}, err
	}
	var warnings []string
	report.Partitions, warnings = partitionRows(p, located, saved, metadata)
	report.Warnings = append(report.Warnings, warnings...)
	if err := lookupEnds(ctx, connection, p.CutoffMS, report); err != nil {
		return file.DestinationProgress{}, err
	}
	id, extent, err := kafkaport.FileExtent(metadata)
	return file.DestinationProgress{ID: id, Extent: extent, Exists: len(metadata) > 0}, err
}

func lookupEnds(ctx context.Context, reader kafkaport.TimeReader, cutoff int64, report *Report) error {
	queries := make(map[int32]int64)
	for _, partition := range report.Partitions {
		if partition.Window.Upper == nil {
			continue
		}
		threshold, err := partition.Window.StopThreshold(cutoff)
		if err != nil {
			return &kafkaport.Conflict{Err: err}
		}
		if threshold < 0 {
			report.Warnings = append(report.Warnings, fmt.Sprintf("Partition %d: timestamp lookup is unavailable for a negative cutoff boundary.", partition.ID))
			continue
		}
		queries[partition.ID] = threshold
	}
	if len(queries) == 0 {
		return nil
	}
	found, err := reader.OffsetsForTimes(ctx, queries)
	if err != nil {
		return err
	}
	for n := range report.Partitions {
		partition := &report.Partitions[n]
		if _, queried := queries[partition.ID]; !queried {
			continue
		}
		end, exists := found[partition.ID]
		if !exists {
			return fmt.Errorf("missing timestamp lookup result for partition %d", partition.ID)
		}
		if end >= 0 {
			span := max(0, end-partition.Start)
			partition.LookupEnd, partition.Span = &end, &span
			if end < partition.Start {
				report.Warnings = append(report.Warnings, fmt.Sprintf("Partition %d: start position is past the timestamp lookup.", partition.ID))
			}
		}
	}
	return nil
}
