package inspect

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/pagrus7/kaferry/internal/archiveio"
	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/plan"
)

const StatusMeaning = "Complete means source processing finished for the selected scope. Complete within staleness depends on enabled MAX_STALENESS (arrival delay and clock lead) and MAX_OUT_OF_ORDER (timestamp disorder) assumptions. Limit reached means a worker record cap stopped processing. Completion does not verify destination contents, exclude filtered or skipped records, or guarantee exactly-once writes."

var stopDescriptions = map[string]string{
	"timestamp":         "timestamp disorder threshold reached",
	"tip":               "saved partition end reached",
	"eof":               "file selection completed",
	"worker_record_cap": "worker record threshold reached",
}

// StopDescription describes a reason validated by the progress reader.
func StopDescription(reason string) string { return stopDescriptions[reason] }

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
	Activity    string           `json:"activity"`
}
type SavedPartition struct {
	ID       int32        `json:"id"`
	Position *int64       `json:"position,omitempty"`
	End      *int64       `json:"captured_end,omitempty"`
	Stop     *core.Stop   `json:"stop,omitempty"`
	Window   *plan.Window `json:"window,omitempty"`
}

func (r StatusReport) ExitCode() int {
	switch r.Verdict {
	case "complete", "complete_within_staleness":
		return 0
	case "limit_reached":
		return 7
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
	report.Source.PartitionFilter = p.Partitions
	if p.Source.Kind == "TOPIC" && p.Partitions != nil {
		topic, err := source.Describe(ctx, false)
		if err != nil {
			return report, err
		}
		if _, err = p.Partitions.Select(topic.Partitions); err != nil {
			return report, &kafkaport.Conflict{Err: err}
		}
	}
	report.Fingerprint, err = p.Fingerprint(sourceInfo.ClusterID, destinationInfo.ClusterID)
	if err != nil {
		return report, &kafkaport.Conflict{Err: err}
	}
	stop.Stage("reading saved status")
	if source != nil {
		err = s.topicStatus(ctx, p, source, &report)
	} else {
		err = s.fileStatus(p, limits, &report)
	}
	var kafkaConflict *kafkaport.Conflict
	var archiveConflict *archiveio.Conflict
	if errors.As(err, &kafkaConflict) || errors.As(err, &archiveConflict) {
		report.Verdict, report.Reason = "unknown", "saved progress does not match this query or source"
		return report, nil
	}
	if err == nil {
		err = ctx.Err()
	}
	return report, err
}

func (s *Service) topicStatus(ctx context.Context, p plan.Plan, c kafkaport.InspectionConnection, r *StatusReport) error {
	before, err := c.Membership(ctx, p.Group, kafkaport.GroupProtocol(p, r.Fingerprint))
	if err != nil {
		return err
	}
	saved, metadata, err := c.Saved(ctx, p.Group, r.Fingerprint, p.Window, p.MaxStalenessMS)
	if err != nil {
		return err
	}
	after, err := c.Membership(ctx, p.Group, kafkaport.GroupProtocol(p, r.Fingerprint))
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
	selected, err := p.Partitions.Select(topic.Partitions)
	if err != nil {
		return &kafkaport.Conflict{Err: err}
	}
	for _, id := range selected {
		row := SavedPartition{ID: id}
		if position, ok := saved[id]; ok {
			row.Position = &position
			row.Stop = metadata[id].Stop
			row.End = metadata[id].End
			row.Window = metadata[id].Window
		}
		r.Partitions = append(r.Partitions, row)
	}
	r.Activity = "group checked twice; observations are not atomic"
	r.observed(before, after, p)
	return nil
}

func (r *StatusReport) observed(before, after kafkaport.Membership, p plan.Plan) {
	switch {
	case before.Foreign || after.Foreign:
		r.Verdict, r.Reason = "unknown", "group has an incompatible active member"
	case !before.Idle || !after.Idle || before.State != after.State:
		r.Verdict = "running"
	default:
		r.classify(p)
	}
}

func (s *Service) fileStatus(p plan.Plan, limits config.Worker, r *StatusReport) error {
	info, err := s.files.Source(p, r.Fingerprint, limits)
	if err != nil {
		return err
	}
	r.Source.File = fileSummary(info.ArchiveInspection)
	r.Source.File.PositionRecords = &info.Position
	r.Meaning = StatusMeaning + " Archive completion applies to this group’s fixed file selection. A new group selects current completed files."
	row := SavedPartition{ID: 0}
	if info.Saved {
		row.Position, row.Stop, row.Window = &info.Position, info.Stop, &info.Window
	}
	r.Partitions = append(r.Partitions, row)
	r.classify(p)
	return nil
}

func (r *StatusReport) classify(p plan.Plan) {
	anySaved, allStopped := false, len(r.Partitions) > 0
	reasons := make(map[string]bool)
	for _, p := range r.Partitions {
		anySaved = anySaved || p.Position != nil
		allStopped = allStopped && p.Position != nil && p.Stop != nil
		if p.Stop != nil {
			reasons[p.Stop.Reason] = true
		}
	}
	switch {
	case !anySaved:
		r.Verdict, r.Reason = "unknown", "no saved progress"
	case !allStopped:
		r.Verdict, r.Reason = "incomplete", "partitions have no saved stop"
	default:
		r.Verdict = "complete"
		switch {
		case reasons["worker_record_cap"]:
			r.Verdict = "limit_reached"
		case reasons["timestamp"] || !p.HasOffsetEnd() && (reasons["tip"] || reasons["eof"] && p.DisorderMS() >= 0):
			r.Verdict = "complete_within_staleness"
		}
		var descriptions []string
		for reason := range reasons {
			descriptions = append(descriptions, StopDescription(reason))
		}
		slices.Sort(descriptions)
		r.Reason = strings.Join(descriptions, "; ")
	}
}
