package kafkaport

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"

	copy "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
)

// positioner resolves one assignment, batching broker requests by offset kind.
type positioner struct {
	offsets     OffsetReader
	times       TimeReader
	plan        plan.Plan
	fingerprint string
	topicID     string
	clock       copy.Clock
	log         *slog.Logger
	low, high   map[int32]int64
	lookups     map[int32]int64
	ends        []int32
}

type Positioning struct {
	Assignments []copy.Assignment
	Progress    map[int32]Progress
	Retention   map[int32]RetentionCheck
	LogStart    map[int32]int64
	LogEnd      map[int32]int64
}
type AssignmentPositioner interface {
	Locate(context.Context, []int32, map[int32]int64, map[int32]Progress) (Positioning, error)
}
type PositionerFactory interface {
	New(OffsetReader, TimeReader, plan.Plan, string, string) AssignmentPositioner
}
type positionerFactory struct {
	clock copy.Clock
	log   *slog.Logger
}

func NewPositionerFactory(clock copy.Clock, logger *slog.Logger) PositionerFactory {
	return &positionerFactory{clock: clock, log: logger}
}
func (f *positionerFactory) New(offsets OffsetReader, times TimeReader, p plan.Plan, fingerprint, topicID string) AssignmentPositioner {
	return &positioner{offsets: offsets, times: times, plan: p, fingerprint: fingerprint, topicID: topicID, clock: f.clock, log: f.log}
}

func (p *positioner) Locate(ctx context.Context, parts []int32, saved map[int32]int64, metadata map[int32]Progress) (Positioning, error) {
	p.ends = nil
	if len(parts) == 0 {
		return Positioning{}, nil
	}
	ids := slices.Clone(parts)
	if err := p.watermarks(ctx, ids); err != nil {
		return Positioning{}, err
	}
	now, adopted := p.clock.Now(), adoptedWindow(metadata)
	assignments := make([]copy.Assignment, len(parts))
	progress := make(map[int32]Progress, len(parts))
	p.lookups = make(map[int32]int64)
	for n, part := range parts {
		window, stored, position, err := p.window(part, saved, metadata, adopted, now)
		if err != nil {
			return Positioning{}, err
		}
		position, err = p.start(part, position, window, len(saved) > 0)
		if err != nil {
			return Positioning{}, err
		}
		if stored.End != nil && (*stored.End > p.high[part] || position > *stored.End) {
			return Positioning{}, conflict("partition %d captured end is outside retained positions", part)
		}
		cursor := copy.Cursor{Partition: part, Position: position, End: stored.End}
		if window.Upper != nil && stored.Stop != nil && stored.Stop.Reason != "worker_record_cap" {
			cursor.Stop = stored.Stop
		}
		progress[part] = stored
		assignments[n] = copy.Assignment{Cursor: cursor, Window: window}
	}
	if err := p.resolveTimes(ctx, assignments); err != nil {
		return Positioning{}, err
	}
	if err := p.resolveEnds(ctx, assignments); err != nil {
		return Positioning{}, err
	}
	retention := make(map[int32]RetentionCheck)
	for _, assignment := range assignments {
		id := assignment.Cursor.Partition
		if p.low[id] > 0 && assignment.Cursor.Position == p.low[id] && assignment.Window.Lower != nil {
			retention[id] = RetentionCheck{Low: p.low[id], Lower: *assignment.Window.Lower}
		}
	}
	return Positioning{Assignments: assignments, Progress: progress, Retention: retention, LogStart: p.low, LogEnd: p.high}, nil
}

func (p *positioner) watermarks(ctx context.Context, ids []int32) error {
	var err error
	p.low, err = p.offsets.Offsets(ctx, ids, EarliestOffset, false)
	if err != nil {
		return err
	}
	p.high, err = p.offsets.Offsets(ctx, ids, LatestOffset, false)
	if err != nil {
		return err
	}
	if err := validateOffsets(ids, p.low); err != nil {
		return err
	}
	return validateOffsets(ids, p.high)
}

func validateOffsets(ids []int32, offsets map[int32]int64) error {
	for _, id := range ids {
		if offset, present := offsets[id]; !present || offset < 0 {
			return fmt.Errorf("missing or invalid offset for partition %d", id)
		}
	}
	return nil
}

func adoptedWindow(metadata map[int32]Progress) *plan.Window {
	var window *plan.Window
	var lowest int32
	first := true
	for id, progress := range metadata {
		if first || id < lowest {
			lowest, window, first = id, progress.Window, false
		}
	}
	return window
}

func (p *positioner) window(id int32, saved map[int32]int64, metadata map[int32]Progress, adopted *plan.Window, now time.Time) (plan.Window, Progress, int64, error) {
	var window plan.Window
	progress := Progress{Fingerprint: p.fingerprint, TopicID: p.topicID}
	position, origin := int64(-1), "resolved"
	if stored, ok := saved[id]; ok {
		position, progress, origin = stored, metadata[id], "saved"
		if progress.Window != nil {
			window = *progress.Window
		}
	} else if adopted != nil && p.plan.Window.Relative() {
		window, origin = *adopted, "adopted"
	}
	if progress.Window == nil && origin != "adopted" {
		var err error
		window, err = p.plan.Window.Resolve(now)
		if err != nil {
			return window, progress, position, conflict("invalid resolved time window: %v", err)
		}
	}
	if err := p.plan.Window.ValidateStored(window, p.plan.MaxStalenessMS); err != nil {
		return window, progress, position, conflict("invalid time window: %v", err)
	}
	if p.plan.Window.Relative() {
		progress.Window = &window
		p.log.Info("partition window positioned", "partition", id, "origin", origin, "window", window)
	}
	return window, progress, position, nil
}

func (p *positioner) start(id int32, position int64, window plan.Window, hasSaved bool) (int64, error) {
	if position > p.high[id] {
		return 0, conflict("partition %d committed offset %d exceeds log end %d", id, position, p.high[id])
	}
	if position >= 0 {
		if position < p.low[id] {
			p.log.Warn("expired source position", "partition", id, "saved", position, "log_start", p.low[id], "offset_gap", p.low[id]-position)
			return p.low[id], nil
		}
		return position, nil
	}
	switch {
	case window.Lower != nil && *window.Lower > 0:
		p.lookups[id] = *window.Lower
	case window.Lower != nil || window.Upper != nil || hasSaved || p.plan.Start == "EARLIEST":
		// Negative timestamps are reserved offset specs in Kafka.
		if window.Lower == nil && window.Upper == nil && hasSaved {
			p.log.Info("new partition starts at earliest retained offset", "partition", id)
		}
		return p.low[id], nil
	default:
		p.ends = append(p.ends, id)
	}
	return position, nil
}

func (p *positioner) resolveTimes(ctx context.Context, assignments []copy.Assignment) error {
	if len(p.lookups) == 0 {
		return nil
	}
	found, err := p.times.OffsetsForTimes(ctx, p.lookups)
	if err != nil {
		return err
	}
	for n, assignment := range assignments {
		id := assignment.Cursor.Partition
		if _, queried := p.lookups[id]; !queried {
			continue
		}
		offset, ok := found[id]
		if !ok {
			return fmt.Errorf("missing timestamp lookup result for partition %d", id)
		}
		if offset < 0 {
			p.ends = append(p.ends, id) // No match starts at the read-committed end.
			continue
		}
		assignments[n].Cursor.Position = offset
	}
	return nil
}

func (p *positioner) resolveEnds(ctx context.Context, assignments []copy.Assignment) error {
	if len(p.ends) == 0 {
		return nil
	}
	stable, err := p.offsets.Offsets(ctx, p.ends, LatestOffset, true)
	if err != nil {
		return err
	}
	if err := validateOffsets(p.ends, stable); err != nil {
		return err
	}
	for n, assignment := range assignments {
		if assignment.Cursor.Position < 0 {
			assignments[n].Cursor.Position = stable[assignment.Cursor.Partition]
		}
	}
	return nil
}
