package inspect

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/pagrus7/kaferry/src/internal/archiveio"
	"github.com/pagrus7/kaferry/src/internal/config"
	core "github.com/pagrus7/kaferry/src/internal/copy"
	"github.com/pagrus7/kaferry/src/internal/kafkaport"
	"github.com/pagrus7/kaferry/src/internal/plan"
)

const StatusMeaning = `Status                    | Meaning
--------------------------|--------
unknown                   | No saved progress, or incompatible progress or group membership.
running                   | Kafka group activity or a membership change was observed.
incomplete                | At least one selected partition has no saved stop.
limit_reached             | All selected partitions stopped; at least one reached a worker record cap.
complete_within_staleness | Source processing finished under the configured timestamp assumptions.
complete                  | Source processing finished for the selected finite scope.

Timestamp completion depends on enabled MAX_STALENESS (arrival delay and clock lead) and MAX_OUT_OF_ORDER (timestamp disorder) assumptions.
Completion permits filtered or skipped records, does not guarantee exactly-once writes and does not establish that destination data still exists.`

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
	ID          int32        `json:"id"`
	Fingerprint string       `json:"fingerprint,omitempty"`
	Position    *int64       `json:"position,omitempty"`
	End         *int64       `json:"captured_end,omitempty"`
	Stop        *core.Stop   `json:"stop,omitempty"`
	Window      *plan.Window `json:"window,omitempty"`
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
	source, sourceInfo, err := s.open(ctx, p.Source, false, p.Partitions, stop)
	if err != nil {
		return report, err
	}
	defer closeConnection(source, stop, &result)
	destination, destinationInfo, err := s.open(ctx, p.Destination, false, nil, stop)
	if err != nil {
		return report, err
	}
	defer closeConnection(destination, stop, &result)
	report.Source, report.Destination = sourceInfo, destinationInfo
	report.Source.PartitionFilter = p.Partitions
	report.Fingerprint, err = p.Fingerprint(sourceInfo.ClusterID, destinationInfo.ClusterID, destinationInfo.TopicID)
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
		report.Verdict = "unknown"
		if report.Reason == "" {
			report.Reason = "saved progress does not match this query or endpoints"
		}
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
	saved, progressErr := c.Saved(ctx, p.Group, r.Fingerprint, p.Window, p.MaxStalenessMS)
	if progressErr != nil && !errors.Is(progressErr, plan.ErrFingerprintMismatch) {
		return progressErr
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
	if progressErr != nil {
		selected = topic.Partitions
		slices.Sort(selected)
	}
	for _, id := range selected {
		row := SavedPartition{ID: id}
		if progress, ok := saved[id]; ok {
			row.Position = &progress.Position
			row.Stop = progress.Stop
			row.End = progress.End
			row.Window = progress.Window
			row.Fingerprint = progress.Fingerprint
		}
		r.Partitions = append(r.Partitions, row)
	}
	r.Activity = "group observed idle twice; observations are not atomic"
	if before.Foreign || after.Foreign {
		r.Activity = "group has an incompatible active member"
	} else if !before.Idle || !after.Idle || before.State != after.State {
		r.Activity = "group activity or a membership change was observed"
	}
	if progressErr != nil {
		r.Reason = "saved query or endpoint fingerprint mismatch; saved positions are for inspection"
		if before.Foreign || after.Foreign || !before.Idle || !after.Idle || before.State != after.State {
			r.Reason += "; " + r.Activity
		}
		return progressErr
	}
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
	if err != nil && !errors.Is(err, plan.ErrFingerprintMismatch) {
		return err
	}
	r.Source.File = fileSummary(info.ArchiveInspection)
	r.Source.File.PositionRecords = &info.Position
	r.Meaning = StatusMeaning + "\nArchive completion applies to this group’s fixed file selection. A new group selects current completed files."
	row := SavedPartition{ID: 0}
	if info.Saved {
		row.Position, row.Stop, row.Window = &info.Position, info.Stop, &info.Window
		row.Fingerprint = info.Fingerprint
	}
	r.Partitions = append(r.Partitions, row)
	if err != nil {
		return err
	}
	r.classify(p)
	return nil
}

func (r *StatusReport) classify(p plan.Plan) {
	anySaved, allStopped := false, len(r.Partitions) > 0
	usesTime := false
	reasons := make(map[string]bool)
	for _, row := range r.Partitions {
		anySaved = anySaved || row.Position != nil
		allStopped = allStopped && row.Position != nil && row.Stop != nil
		if row.Stop != nil {
			reasons[row.Stop.Reason] = true
			usesTime = usesTime || p.StopUsesTime(row.Stop.Reason)
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
		case usesTime:
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
