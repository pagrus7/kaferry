package inspect

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/pagrus7/kaferry/src/internal/archive/io"
	"github.com/pagrus7/kaferry/src/internal/config"
	core "github.com/pagrus7/kaferry/src/internal/copy"
	"github.com/pagrus7/kaferry/src/internal/kafka"
	"github.com/pagrus7/kaferry/src/internal/plan"
)

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
	Notes       []string         `json:"notes,omitempty"`
	Next        string           `json:"next,omitempty"`
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

const (
	verdictComplete          = "complete"
	verdictTimestampComplete = "complete_within_disorder"
	verdictIncomplete        = "incomplete"
	verdictUnknown           = "unknown"
	verdictRunning           = "running"
	verdictLimitReached      = "limit_reached"
)

// StatusVerdict describes a saved status verdict.
type StatusVerdict struct {
	ExitCode int
	Meaning  string
}

var statusVerdicts = map[string]StatusVerdict{
	verdictComplete:          {0, "Source processing finished for the selected finite scope: saved offset ends or the saved archive selection."},
	verdictTimestampComplete: {0, "All selected partitions finished and at least one stopped early under MAX_OUT_OF_ORDER."},
	verdictIncomplete:        {4, "Part of the selection has no saved stop."},
	verdictUnknown:           {5, "Saved progress is absent, invalid or incompatible, or the group has an incompatible active member."},
	verdictRunning:           {6, "Kafka group activity, unsettled state or a membership change was observed."},
	verdictLimitReached:      {7, "All selected partitions have saved stops and at least one stopped at a worker record cap."},
}

// StatusVerdicts returns the descriptions of the saved status verdicts.
func StatusVerdicts() map[string]StatusVerdict { return maps.Clone(statusVerdicts) }

func (r StatusReport) ExitCode() int {
	if info, ok := statusVerdicts[r.Verdict]; ok {
		return info.ExitCode
	}
	return statusVerdicts[verdictUnknown].ExitCode
}

func (s *Service) Status(ctx context.Context, p plan.Plan, limits config.Worker, stop core.StopObserver) (report StatusReport, result error) {
	report = StatusReport{Version: 3, Group: p.Group, Partitions: []SavedPartition{}, Activity: "not checked for files"}
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
		return report, &kafka.Conflict{Err: err}
	}
	stop.Stage("reading saved status")
	var savedArchiveSelection bool
	if source != nil {
		err = s.topicStatus(ctx, p, source, &report)
	} else {
		savedArchiveSelection, err = s.archiveStatus(p, limits, &report)
	}
	var kafkaConflict *kafka.Conflict
	var archiveConflict *archiveio.Conflict
	if errors.As(err, &kafkaConflict) || errors.As(err, &archiveConflict) {
		report.Verdict = verdictUnknown
		if report.Reason == "" {
			report.Reason = "saved progress does not match this query or endpoints"
		}
		err = nil
	} else if err == nil {
		err = ctx.Err()
	}
	if err == nil {
		report.addGuidance(p.Follow, savedArchiveSelection)
	}
	return report, err
}

func (r *StatusReport) addGuidance(follow, savedArchiveSelection bool) {
	r.Meaning = statusVerdicts[r.Verdict].Meaning
	switch r.Verdict {
	case verdictUnknown:
		r.Next = "Check the reason, then verify the group, query, and endpoints."
	case verdictRunning:
		r.Next = "Wait briefly, then check status again."
	case verdictIncomplete:
		if follow {
			r.Notes = append(r.Notes, "FOLLOW = ON has no source end. An incomplete verdict is expected when following stops.")
			r.Next = "To keep following, rerun with the same query, group, and configuration."
		} else {
			r.Next = "To resume, rerun with the same query, group, and configuration."
		}
	case verdictLimitReached:
		r.Next = "Rerun the same job for another allowance."
	case verdictTimestampComplete:
		r.Notes = append(r.Notes, "Later records in the time window can be missed if timestamps move backwards by more than the allowance.")
		fallthrough
	case verdictComplete:
		r.Notes = append(r.Notes, "Completion permits filtered or skipped records, does not guarantee exactly-once writes and does not verify destination data.")
	}
	if savedArchiveSelection {
		r.Notes = append(r.Notes, "This group's saved archive checkpoint fixes the file selection.")
	}
}

func (s *Service) topicStatus(ctx context.Context, p plan.Plan, c kafka.InspectionConnection, r *StatusReport) error {
	before, err := c.Membership(ctx, p.Group, kafka.GroupProtocol(p, r.Fingerprint))
	if err != nil {
		return err
	}
	saved, progressErr := c.Saved(ctx, p, r.Fingerprint)
	if progressErr != nil && !errors.Is(progressErr, plan.ErrFingerprintMismatch) {
		return progressErr
	}
	after, err := c.Membership(ctx, p.Group, kafka.GroupProtocol(p, r.Fingerprint))
	if err != nil {
		return err
	}
	topic, err := c.Describe(ctx, false)
	if err != nil {
		return err
	}
	if topic.TopicID != r.Source.TopicID {
		return &kafka.Conflict{Err: fmt.Errorf("source topic identity changed")}
	}
	r.Source.PartitionCount = len(topic.Partitions)
	selected, err := p.Partitions.Select(topic.Partitions)
	if err != nil {
		return &kafka.Conflict{Err: err}
	}
	if progressErr != nil {
		selected = topic.Partitions
		slices.Sort(selected)
	}
	r.Partitions = append(r.Partitions, savedPartitionRows(selected, saved)...)
	r.Verdict, r.Activity = membershipVerdict(before, after)
	if progressErr != nil {
		r.Reason = "saved query or endpoint fingerprint mismatch; saved positions are for inspection"
		if r.Verdict != "" {
			r.Reason += "; " + r.Activity
		}
		return progressErr
	}
	switch r.Verdict {
	case verdictUnknown:
		r.Reason = r.Activity
	case "":
		r.classify()
	}
	return nil
}

// An idle group leaves the verdict to the saved partition stops.
func membershipVerdict(before, after kafka.Membership) (verdict, activity string) {
	switch {
	case before.Foreign || after.Foreign:
		return verdictUnknown, "group has an incompatible active member"
	case !before.Idle || !after.Idle || before.State != after.State:
		return verdictRunning, "group activity or a membership change was observed"
	default:
		return "", "group observed idle twice; observations are not atomic"
	}
}

func savedPartitionRows(selected []int32, saved map[int32]kafka.SavedProgress) []SavedPartition {
	var rows []SavedPartition
	for _, id := range selected {
		row := SavedPartition{ID: id}
		if progress, ok := saved[id]; ok {
			row.Position = &progress.Position
			row.Stop = progress.Stop
			row.End = progress.End
			row.Window = progress.Window
			row.Fingerprint = progress.Fingerprint
		}
		rows = append(rows, row)
	}
	return rows
}

func (s *Service) archiveStatus(p plan.Plan, limits config.Worker, r *StatusReport) (savedSelection bool, err error) {
	info, err := s.archives.Source(p, r.Fingerprint, limits)
	if err != nil && !errors.Is(err, plan.ErrFingerprintMismatch) {
		return false, err
	}
	r.Source.File = archiveSummary(info.ArchiveInspection)
	r.Source.File.PositionRecords = &info.Position
	row := SavedPartition{ID: 0}
	if info.Saved {
		row.Position, row.Stop, row.Window = &info.Position, info.Stop, &info.Window
		row.Fingerprint = info.Fingerprint
	}
	r.Partitions = append(r.Partitions, row)
	if err == nil {
		r.classify()
	}
	return info.Saved, err
}

func (r *StatusReport) classify() {
	anySaved, allStopped := false, len(r.Partitions) > 0
	reasons := make(map[string]bool)
	for _, row := range r.Partitions {
		anySaved = anySaved || row.Position != nil
		allStopped = allStopped && row.Position != nil && row.Stop != nil
		if row.Stop != nil {
			reasons[row.Stop.Reason] = true
		}
	}
	switch {
	case !anySaved:
		r.Verdict, r.Reason = verdictUnknown, "no saved progress"
	case !allStopped:
		r.Verdict, r.Reason = verdictIncomplete, "partitions have no saved stop"
	default:
		r.Verdict = verdictComplete
		switch {
		case reasons["worker_record_cap"]:
			r.Verdict = verdictLimitReached
		case reasons["timestamp"]:
			r.Verdict = verdictTimestampComplete
		}
		var descriptions []string
		for reason := range reasons {
			descriptions = append(descriptions, StopDescription(reason))
		}
		slices.Sort(descriptions)
		r.Reason = strings.Join(descriptions, "; ")
	}
}
