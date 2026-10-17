package inspect

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/file"
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/plan"
)

const StatusMeaning = "Stopped means a stop was saved for every current partition; the cutoff rule can still miss records within the time range."

type StatusReader interface {
	Status(context.Context, plan.Plan, config.Worker, core.StopObserver) (StatusReport, error)
}

type StatusReport struct {
	Version     int              `json:"version"`
	Verdict     string           `json:"verdict"`
	Reason      string           `json:"reason,omitempty"`
	Meaning     string           `json:"meaning"`
	Group       string           `json:"group"`
	Fingerprint string           `json:"fingerprint"`
	Source      Endpoint         `json:"source"`
	Destination Endpoint         `json:"destination"`
	Partitions  []SavedPartition `json:"partitions"`
	OpeningSize *int64           `json:"opening_size_bytes,omitempty"`
	Activity    string           `json:"activity"`
}
type SavedPartition struct {
	ID       int32        `json:"id"`
	Position *int64       `json:"position,omitempty"`
	Stop     *core.Stop   `json:"stop,omitempty"`
	Window   *plan.Window `json:"window,omitempty"`
}

func (r StatusReport) ExitCode() int {
	switch r.Verdict {
	case "stopped":
		return 0
	case "incomplete":
		return 4
	case "running":
		return 6
	default:
		return 5
	}
}

func (s *Service) Status(ctx context.Context, p plan.Plan, limits config.Worker, stop core.StopObserver) (report StatusReport, result error) {
	report = StatusReport{Version: 1, Group: p.Group, Meaning: StatusMeaning, Partitions: []SavedPartition{}, Activity: "not checked for files"}
	if p.Source.Kind == "TOPIC" {
		report.Activity = "group check did not finish"
	}
	if p.OneOff {
		return report, &core.Exit{Code: 2, Err: fmt.Errorf("status requires a named GROUP; use the logged group name")}
	}
	if err := ctx.Err(); err != nil {
		return report, err
	}
	source, sourceInfo, err := s.open(ctx, p.Source, false, stop)
	if err != nil {
		return report, err
	}
	defer closeConnection(source, stop, &result)
	destination, destinationInfo, err := s.open(ctx, p.Destination, false, stop)
	if err != nil {
		return report, err
	}
	defer closeConnection(destination, stop, &result)
	report.Source, report.Destination = sourceInfo, destinationInfo
	report.Fingerprint, err = p.Fingerprint(sourceInfo.ClusterID, destinationInfo.ClusterID)
	if err != nil {
		return report, err
	}
	stop.Stage("reading saved status")
	if source != nil {
		err = s.topicStatus(ctx, p, source, &report)
	} else {
		err = s.fileStatus(p, limits, &report)
	}
	var kafkaConflict *kafkaport.Conflict
	var fileConflict *file.Conflict
	if errors.As(err, &kafkaConflict) || errors.As(err, &fileConflict) {
		report.Verdict, report.Reason = "unknown", "saved progress does not match this query or source"
		return report, nil
	}
	if err == nil {
		err = ctx.Err()
	}
	return report, err
}

func (s *Service) topicStatus(ctx context.Context, p plan.Plan, c kafkaport.InspectionConnection, r *StatusReport) error {
	before, err := c.Membership(ctx, p.Group)
	if err != nil {
		return err
	}
	saved, metadata, err := c.Saved(ctx, p.Group, r.Fingerprint, p.Window, p.CutoffMS)
	if err != nil {
		return err
	}
	after, err := c.Membership(ctx, p.Group)
	if err != nil {
		return err
	}
	topic, err := c.Describe(ctx, false)
	if err != nil {
		return err
	}
	if topic.TopicID != r.Source.TopicID {
		return &kafkaport.Conflict{Err: fmt.Errorf("source topic identity changed")}
	}
	r.Source.PartitionCount = len(topic.Partitions)
	slices.Sort(topic.Partitions)
	for _, id := range topic.Partitions {
		row := SavedPartition{ID: id}
		if position, ok := saved[id]; ok {
			row.Position = &position
			row.Stop = metadata[id].Stop
			row.Window = metadata[id].Window
		}
		r.Partitions = append(r.Partitions, row)
	}
	r.Activity = "group checked twice; observations are not atomic"
	r.observed(before, after)
	return nil
}

func (r *StatusReport) observed(before, after kafkaport.Membership) {
	switch {
	case before.Foreign || after.Foreign:
		r.Verdict, r.Reason = "unknown", "group has an incompatible active member"
	case !before.Idle || !after.Idle || before.State != after.State:
		r.Verdict = "running"
	default:
		r.classify()
	}
}

func (s *Service) fileStatus(p plan.Plan, limits config.Worker, r *StatusReport) error {
	info, err := s.files.Source(p, r.Fingerprint, limits)
	if err != nil {
		return err
	}
	r.Source.File = fileSummary(info.ArchiveInspection)
	r.OpeningSize = info.OpeningSize
	row := SavedPartition{ID: 0}
	if info.Saved {
		row.Position, row.Stop, row.Window = &info.Position, info.Stop, &info.Window
	}
	r.Partitions = append(r.Partitions, row)
	r.classify()
	if info.Saved && info.Stop != nil && info.OpeningSize != nil && info.Size > *info.OpeningSize {
		r.Verdict, r.Reason = "incomplete", "source file grew"
	}
	return nil
}

func (r *StatusReport) classify() {
	anySaved, allStopped := false, len(r.Partitions) > 0
	for _, p := range r.Partitions {
		anySaved = anySaved || p.Position != nil
		allStopped = allStopped && p.Position != nil && p.Stop != nil
	}
	switch {
	case !anySaved:
		r.Verdict, r.Reason = "unknown", "no saved progress"
	case !allStopped:
		r.Verdict, r.Reason = "incomplete", "partitions have no saved stop"
	default:
		r.Verdict = "stopped"
	}
}
