package copy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	"github.com/pagrus7/kaferry/internal/plan"
)

type Runner struct {
	flow
	source         Source
	sink           Sink
	store          ProgressStore
	clock          Clock
	log            *slog.Logger
	plan           plan.Plan
	limits         config.Worker
	tracker        ProgressTracker
	partitions     map[int32]*partition
	submitted      uint64
	confirmed      uint64
	filtered       uint64
	skipped        uint64
	timestampStops uint64
	tipStops       uint64
	assignmentSeen bool
	idleSince      time.Time
	failure        error
	intake         bool
	stop           StopObserver

	observer                      Observer
	iterations                    uint64
	progressInterval              time.Duration
	nextProgress, nextObservation time.Time
}

func NewRunner(source Source, sink Sink, store ProgressStore, clock Clock, logger *slog.Logger, p plan.Plan, limits config.Worker, tracker ProgressTracker, stop StopObserver, observer Observer) *Runner {
	interval, _ := time.ParseDuration(limits.ProgressInterval)
	return &Runner{source: source, sink: sink, store: store, clock: clock, log: logger, plan: p, limits: limits, tracker: tracker, partitions: make(map[int32]*partition), stop: stop, observer: observer, progressInterval: interval}
}
func (r *Runner) Assign(assignments []Assignment) error {
	r.assignmentSeen = true
	if len(assignments) == 0 && len(r.partitions) == 0 {
		r.idleSince = r.clock.Now()
	}
	for _, a := range assignments {
		r.tracker.Assign(a.Cursor)
		r.partitions[a.Cursor.Partition] = &partition{window: a.Window, position: a.Cursor.Position}
		r.log.Info("partition assigned", "partition", a.Cursor.Partition, "position", a.Cursor.Position, "window", a.Window)
	}
	return nil
}
func (r *Runner) Revoke(partitions []int32, lost bool) error {
	if !lost {
		if err := r.save(partitions, true); err != nil {
			r.failure = err
		}
	}
	for _, id := range partitions {
		r.flow.revoke(id)
		r.tracker.Revoke(id)
		delete(r.partitions, id)
		r.log.Info("partition revoked", "partition", id)
	}
	if len(r.partitions) == 0 {
		r.assignmentSeen = false
	}
	return r.failure
}

func (r *Runner) Run(ctx context.Context) error {
	defer func() {
		r.stop.Begin()
		r.report(true)
		r.stop.Stage("printing copy summary")
		r.log.Info("copy summary", "submitted", r.submitted, "confirmed", r.confirmed, "filtered", r.filtered, "skipped", r.skipped, "timestamp_stops", r.timestampStops, "tip_stops", r.tipStops, "peak_uncommitted_records", r.peakRecords, "peak_uncommitted_bytes", r.peakBytes)
	}()
	if err := r.run(ctx); err != nil {
		return r.exit(err)
	}
	return nil
}

func (r *Runner) run(ctx context.Context) error {
	r.nextProgress = r.clock.Now().Add(r.progressInterval)
	if err := r.source.Start(r, r.observer); err != nil {
		return err
	}
	for {
		if ctx.Err() != nil {
			return r.interrupted()
		}
		if r.failure != nil {
			return errors.Join(r.failure, r.drain())
		}
		done, err := r.step(ctx)
		r.iterations++
		r.report(false)
		if done {
			return err
		}
		if err != nil {
			if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
				continue
			}
			r.failure = err
			continue
		}
	}
}

func (r *Runner) step(ctx context.Context) (bool, error) {
	r.stop.Stage("copying records")
	if err := r.advance(); err != nil {
		return false, err
	}
	if r.workerStopReached() {
		return true, r.finishWorkerStop()
	}
	if r.readyToExit() {
		r.stop.Begin()
		if len(r.partitions) == 0 {
			r.log.Info("local idle exit", "idle_exit", r.limits.IdleExit, "exit_code", 3)
			return true, &Exit{Err: fmt.Errorf("worker held no partitions for %s", r.limits.IdleExit), Code: 3}
		}
		return true, r.commit(r.ids())
	}
	if err := r.checkpoint(); err != nil {
		return false, err
	}
	if err := r.pause(); err != nil {
		return false, err
	}
	event, err := r.source.Poll(20 * time.Millisecond)
	if ctx.Err() != nil && (err == nil || errors.Is(err, ctx.Err())) {
		return false, nil // A signal during Poll must not submit its input.
	}
	if err != nil {
		return false, err
	}
	return false, r.accept(event)
}

func (r *Runner) advance() error {
	if err := r.deliveries(); err != nil {
		return err
	}
	if err := r.admitDeferred(); err != nil {
		return err
	}
	if err := r.submitPending(); err != nil {
		return err
	}
	if r.workerStopReached() {
		return nil
	}
	return r.tips()
}

func (r *Runner) checkpoint() error {
	if !r.shouldCommit() {
		return nil
	}
	if err := r.flush(); err != nil {
		return err
	}
	return r.commit(r.ids())
}

func (r *Runner) accept(event Event) error {
	var failed error
	if event.Input != nil {
		r.intake = true // Filtered and skipped records count: intake has started.
		failed = r.input(*event.Input)
	}
	if event.EOF != nil {
		if err := r.eof(*event.EOF); err != nil {
			failed = err
		}
	}
	if event.Input == nil {
		if err := r.flush(); err != nil {
			failed = err
		}
	}
	return failed
}

func (r *Runner) input(input Input) error {
	p := r.partitions[input.Cursor.Partition]
	if p == nil || p.stop != nil {
		return nil
	}
	if r.pending != nil || r.deferred != nil {
		return fmt.Errorf("source delivered a record while intake was paused")
	}
	p.atTip = false
	if p.reachedHigh(input.Cursor.Position) {
		cursor := Cursor{Partition: input.Cursor.Partition, Position: *p.high}
		err := r.tracker.Advance(cursor)
		p.position = cursor.Position
		return err // A later visible record proves control/aborted gaps reached H.
	}
	if !r.admits(input) {
		r.deferred = &input
		return nil
	}
	if stopped, err := r.stopAtTimestamp(p, input); stopped || err != nil {
		return err
	}

	token, err := r.tracker.Observe(input.Cursor, input.Next, input.Record.Size())
	if err != nil {
		return err
	}
	p.position = input.Next
	r.flow.observe(r.tracker)
	if filtered, err := r.filter(p.window, input.Record, token); filtered || err != nil {
		return err
	}

	r.pending = &submission{input: input, token: token}
	return r.submitPending()
}
func (r *Runner) stopAtTimestamp(p *partition, input Input) (bool, error) {
	if r.plan.Source.Kind != "TOPIC" {
		return false, nil
	}
	stop, err := p.pastTimestamp(input.Record.TimestampMS, r.plan.CutoffMS)
	if !stop || err != nil {
		return false, err
	}
	if err := r.tracker.Advance(input.Cursor); err != nil {
		return false, err
	}
	p.stop = &Stop{Reason: "timestamp", At: input.Cursor.Position}
	r.timestampStops++
	r.log.Info("partition stopped", "reason", "timestamp", "partition", input.Cursor.Partition, "position", input.Cursor.Position, "timestamp", *input.Record.TimestampMS, "upper", *p.window.Upper, "past_upper_ms", *input.Record.TimestampMS-*p.window.Upper, "future_dated", *input.Record.TimestampMS > r.clock.Now().Add(time.Minute).UnixMilli())
	return true, nil
}

func (r *Runner) filter(window plan.Window, record Record, token Token) (bool, error) {
	if record.TimestampMS == nil && (window.Lower != nil || window.Upper != nil) {
		return true, r.recordError(token, &RecordError{Class: "missing_timestamp", Err: fmt.Errorf("record lacks required timestamp")})
	}
	if record.TimestampMS != nil && !window.Contains(*record.TimestampMS) {
		r.tracker.Settle(token, true)
		r.filtered++
		return true, nil
	}
	return false, nil
}

func (r *Runner) submitPending() error {
	if r.pending == nil {
		return nil
	}
	s := r.pending
	err := r.sink.Submit(s.input.Record, s.token)
	var busy *Busy
	if errors.As(err, &busy) {
		if s.busySince.IsZero() {
			s.busySince = r.clock.Now()
			r.log.Warn("destination queue full; intake paused")
		}
		budget, _ := time.ParseDuration(r.limits.DeliveryTimeout)
		if r.clock.Now().Sub(s.busySince) >= budget {
			return fmt.Errorf("destination queue remained full for %s", budget)
		}
		return nil
	}
	r.pending = nil
	if err != nil {
		return r.recordError(s.token, err)
	}
	r.tracker.Submitted(s.token)
	r.submitted++
	return nil
}
func (r *Runner) recordError(token Token, err error) error {
	var record *RecordError
	if !errors.As(err, &record) {
		return err
	}
	policy := r.plan.Policies.OtherRecord
	switch record.Class {
	case "missing_timestamp":
		policy = r.plan.Policies.MissingTimestamp
	case "rejected_timestamp":
		policy = r.plan.Policies.RejectedTimestamp
	}
	if policy != "SKIP" {
		return fmt.Errorf("partition %d: %w", token.Partition, err)
	}
	r.tracker.Settle(token, true)
	r.skipped++
	r.log.Warn("record skipped", "partition", token.Partition, "position", r.tracker.PositionOf(token), "class", record.Class, "reason", record.Error())
	return nil
}
func (r *Runner) deliveries() error {
	for {
		select {
		case delivery, open := <-r.sink.Deliveries():
			if !open {
				return fmt.Errorf("destination delivery stream closed unexpectedly")
			}
			if delivery.Fatal {
				return delivery.Err
			}
			if !r.tracker.Settle(delivery.Token, delivery.Err == nil) {
				continue
			}
			if delivery.Err != nil {
				return delivery.Err
			}
			r.confirmed++
		default:
			return nil
		}
	}
}
func (r *Runner) eof(cursor Cursor) error {
	if r.workerStopReached() {
		return nil
	}
	p := r.partitions[cursor.Partition]
	if p == nil || p.stop != nil {
		return nil
	}
	if r.deferred != nil && r.deferred.Cursor.Partition == cursor.Partition {
		r.deferredEOF = &cursor
		return nil
	}
	err := r.tracker.Advance(cursor)
	if err != nil {
		return err
	}
	p.reachedTip(cursor.Position, r.plan.Source.Kind == "ARCHIVE")

	return nil
}
func (r *Runner) tips() error {
	wait, _ := time.ParseDuration(r.limits.TipWait)
	sampled := false // Interleave watermark queries with consumer polling.
	for id, p := range r.partitions {
		if p.stop != nil || p.window.Upper == nil || r.plan.Source.Kind != "TOPIC" {
			continue
		}
		action, err := p.tipAction(r.clock.Now(), r.plan.CutoffMS, wait)
		if err != nil {
			return err
		}
		if action == tipSample {
			if sampled {
				continue
			}
			sampled = true
			if err := r.sampleHigh(id, p); err != nil {
				return err
			}
			action, err = p.tipAction(r.clock.Now(), r.plan.CutoffMS, wait)
			if err != nil {
				return err
			}
		}
		switch action {
		case tipStop:
			p.stop = &Stop{Reason: "tip", At: p.position}
			r.tipStops++
			r.log.Info("partition stopped", "reason", "tip", "partition", id, "position", p.position, "high_watermark", *p.high)
		case tipTimeout:
			return fmt.Errorf("partition %d tip wait exceeded: H=%d stable=%d consumed=%d", id, *p.high, p.stable, p.position)
		}
	}
	return nil
}

func (r *Runner) sampleHigh(id int32, p *partition) error {
	high, err := r.source.HighWatermark(id)
	if err != nil {
		var recoverable *Recoverable
		if !errors.As(err, &recoverable) {
			return err
		}
		if p.sampling.failed(r.clock.Now()) {
			r.log.Warn("tip watermark query retrying", "partition", id, "attempt", p.sampling.attempts, "reason", err)
		}
		return nil
	}
	if attempts := p.sampling.recovered(); attempts > 0 {
		r.log.Info("tip watermark query recovered", "partition", id, "attempts", attempts)
	}
	p.sampled(high, r.clock.Now())
	r.log.Info("tip watermark sampled", "partition", id, "high_watermark", high, "stable", p.stable, "position", p.position)
	return nil
}
func (r *Runner) flush() error {
	if err := r.sink.Flush(); err != nil {
		return err
	}
	return r.deliveries()
}
