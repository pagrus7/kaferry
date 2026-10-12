package copy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	"github.com/pagrus7/kaferry/internal/plan"
)

type partition struct {
	window      plan.Window
	stopped     bool
	paused      bool
	position    int64
	stable      int64
	atTip       bool
	high        *int64
	waitStarted time.Time
	sampling    retrySchedule
}
type submission struct {
	input     Input
	token     Token
	busySince time.Time
}
type Runner struct {
	source         Source
	sink           Sink
	store          ProgressStore
	clock          Clock
	log            *slog.Logger
	plan           plan.Plan
	limits         config.Worker
	tracker        ProgressTracker
	partitions     map[int32]*partition
	pending        *submission
	deferred       *Input
	deferredEOF    *Cursor
	submitted      uint64
	confirmed      uint64
	filtered       uint64
	skipped        uint64
	peakRecords    int
	peakBytes      int64
	timestampStops uint64
	tipStops       uint64
	lastCommit     time.Time
	assignmentSeen bool
	idleSince      time.Time
	failure        error
	intake         bool
}

func NewRunner(source Source, sink Sink, store ProgressStore, clock Clock, logger *slog.Logger, p plan.Plan, limits config.Worker, tracker ProgressTracker) *Runner {
	return &Runner{source: source, sink: sink, store: store, clock: clock, log: logger, plan: p, limits: limits, tracker: tracker, partitions: make(map[int32]*partition)}
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
		if r.deferred != nil && r.deferred.Cursor.Partition == id {
			r.deferred, r.deferredEOF = nil, nil
		}
		if r.pending != nil && r.pending.token.Partition == id {
			r.pending = nil
		}
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
		r.log.Info("copy summary", "submitted", r.submitted, "confirmed", r.confirmed, "filtered", r.filtered, "skipped", r.skipped, "timestamp_stops", r.timestampStops, "tip_stops", r.tipStops, "peak_uncommitted_records", r.peakRecords, "peak_uncommitted_bytes", r.peakBytes)
	}()
	if err := r.run(ctx); err != nil {
		return r.exit(err)
	}
	return nil
}

// exit preserves classified outcomes; other errors use the first failure and
// execution stage to distinguish conflicts before intake from runtime failures.
func (r *Runner) exit(err error) error {
	primary := r.failure
	if primary == nil {
		primary = err
	}
	code := 1
	var classified *Exit
	var coded interface{ ExitCode() int }
	if errors.As(primary, &classified) {
		code = classified.Code
	} else if !r.intake && errors.As(primary, &coded) {
		code = coded.ExitCode()
	}
	return &Exit{Err: err, Code: code}
}

func (r *Runner) run(ctx context.Context) error {
	if err := r.source.Start(r); err != nil {
		return err
	}
	for {
		if ctx.Err() != nil {
			return r.interrupted()
		}
		if r.failure != nil {
			_ = r.drain()
			return r.failure
		}
		done, err := r.step(ctx)
		if done {
			return err
		}
		if err != nil {
			r.failure = err
			continue
		}
	}
}

func (r *Runner) interrupted() error {
	err := r.drain()
	if r.failure != nil {
		return errors.Join(r.failure, err)
	}
	if err != nil {
		return err
	}
	if r.bounded() {
		return fmt.Errorf("bounded copy interrupted before completion")
	}
	return nil
}

func (r *Runner) step(ctx context.Context) (bool, error) {
	if err := r.advance(); err != nil {
		return false, err
	}
	if r.readyToExit() {
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
	if err != nil {
		return false, err
	}
	if ctx.Err() != nil {
		return false, nil
	} // A signal during Poll must not submit its input.
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
	if p == nil || p.stopped {
		return nil
	}
	if r.pending != nil || r.deferred != nil {
		return fmt.Errorf("source delivered a record while intake was paused")
	}
	p.atTip = false
	if p.high != nil && input.Cursor.Position >= *p.high {
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
	n, b := r.tracker.Total()
	if n > r.peakRecords {
		r.peakRecords = n
	}
	if b > r.peakBytes {
		r.peakBytes = b
	}
	if filtered, err := r.filter(p.window, input.Record, token); filtered || err != nil {
		return err
	}

	r.pending = &submission{input: input, token: token}
	return r.submitPending()
}
func (r *Runner) stopAtTimestamp(p *partition, input Input) (bool, error) {
	if input.Record.TimestampMS == nil || p.window.Upper == nil || r.plan.Source.Kind != "TOPIC" {
		return false, nil
	}
	threshold, err := stopThreshold(p.window, r.plan.CutoffMS)
	if err != nil {
		return false, err
	}
	if *input.Record.TimestampMS <= threshold {
		return false, nil
	}
	if err := r.tracker.Advance(input.Cursor); err != nil {
		return false, err
	}
	p.stopped = true
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
	p := r.partitions[cursor.Partition]
	if p == nil || p.stopped {
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
	p.atTip = true
	p.position = cursor.Position
	p.stable = cursor.Position
	if r.plan.Source.Kind == "FILE" {
		p.stopped = true
	}
	return nil
}
func stopThreshold(window plan.Window, cutoff int64) (int64, error) {
	hi := *window.Upper
	if cutoff > 0 && hi > int64(^uint64(0)>>1)-cutoff {
		return 0, fmt.Errorf("cutoff overflows timestamp")
	}
	return hi + cutoff, nil
}
func (r *Runner) tips() error {
	wait, _ := time.ParseDuration(r.limits.TipWait)
	sampled := false // Interleave native watermark queries with consumer polling.
	for id, p := range r.partitions {
		if p.stopped || p.window.Upper == nil || r.plan.Source.Kind != "TOPIC" {
			continue
		}
		threshold, err := stopThreshold(p.window, r.plan.CutoffMS)
		if err != nil {
			return err
		}
		if p.high == nil {
			if sampled || !p.atTip || r.clock.Now().UnixMilli() <= threshold || !p.sampling.due(r.clock.Now()) {
				continue
			}
			sampled = true
			if err := r.sampleHigh(id, p); err != nil {
				return err
			}
			if p.high == nil {
				continue
			}
		}
		if p.position >= *p.high {
			p.stopped = true
			r.tipStops++
			r.log.Info("partition stopped", "reason", "tip", "partition", id, "position", p.position, "high_watermark", *p.high)
		} else if r.clock.Now().Sub(p.waitStarted) > wait {
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
	p.high = &high
	p.waitStarted = r.clock.Now()
	return nil
}
func (r *Runner) ids() []int32 {
	ids := make([]int32, 0, len(r.partitions))
	for id := range r.partitions {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
func (r *Runner) commit(ids []int32) error { return r.save(ids, false) }

// save commits confirmed progress. When releasing revoked partitions, a
// recoverable failure only replays work, so the partition is released anyway.
func (r *Runner) save(ids []int32, releasing bool) error {
	r.lastCommit = r.clock.Now()
	var cursors []Cursor
	for _, id := range ids {
		if r.tracker.HasProgress(id) {
			cursors = append(cursors, r.tracker.Confirmed(id))
		}
	}
	if len(cursors) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	results, err := r.store.Save(ctx, cursors)
	if err != nil {
		if !r.released(releasing, cursors, err) {
			return err
		}
		return nil
	}
	var failed error
	for _, cursor := range cursors {
		err, present := results[cursor.Partition]
		if !present {
			err = fmt.Errorf("missing commit result for partition %d", cursor.Partition)
		}
		if err != nil {
			if !r.released(releasing, []Cursor{cursor}, err) {
				failed = errors.Join(failed, err)
			}
			continue
		}
		r.tracker.Committed(cursor)
		r.log.Debug("progress committed", "partition", cursor.Partition, "position", cursor.Position)
	}
	return failed
}
func (r *Runner) released(releasing bool, cursors []Cursor, err error) bool {
	var recoverable *Recoverable
	if !releasing || !errors.As(err, &recoverable) {
		return false
	}
	for _, cursor := range cursors {
		r.log.Warn("revoked partition released without saving progress; the next owner replays", "partition", cursor.Partition, "saved", r.tracker.Saved(cursor.Partition), "confirmed", cursor.Position, "reason", err.Error())
	}
	return true
}
func (r *Runner) shouldCommit() bool {
	if r.deferred != nil {
		return true
	}
	if r.clock.Now().Sub(r.lastCommit) >= time.Second {
		return true
	}
	n, b := r.tracker.Total()
	if n*2 >= r.limits.RecordCap || b*2 >= r.limits.ByteCap {
		return true
	}
	for id := range r.partitions {
		n, b := r.tracker.Work(id)
		if n*2 >= r.limits.PartitionRecordCap || b*2 >= r.limits.PartitionByteCap {
			return true
		}
	}
	return false
}
func (r *Runner) pause() error {
	n, b := r.tracker.Total()
	full := n >= r.limits.RecordCap || b >= r.limits.ByteCap || r.pending != nil || r.deferred != nil
	for id, p := range r.partitions {
		n, b := r.tracker.Work(id)
		paused := full || p.stopped || n >= r.limits.PartitionRecordCap || b >= r.limits.PartitionByteCap
		if paused != p.paused {
			if err := r.source.Pause(id, paused); err != nil {
				return err
			}
			p.paused = paused
		}
	}
	return nil
}
func (r *Runner) bounded() bool { return r.plan.Source.Kind == "FILE" || r.plan.Window.Upper != nil }
func (r *Runner) readyToExit() bool {
	if r.deferred != nil || r.pending != nil {
		return false
	}
	if len(r.partitions) == 0 {
		idle, _ := time.ParseDuration(r.limits.IdleExit)
		if r.bounded() && r.assignmentSeen && r.clock.Now().Sub(r.idleSince) >= idle {
			return true
		}
		return false
	}
	for id, p := range r.partitions {
		if !p.stopped || !r.tracker.Resolved(id) {
			return false
		}
	}
	return true
}
func (r *Runner) drain() error {
	r.pending = nil // Unsubmitted input replays from the confirmed cursor.
	r.deferred, r.deferredEOF = nil, nil
	deadline := r.clock.Now().Add(10 * time.Second)
	var failed error
	for id := range r.partitions {
		if err := r.source.Pause(id, true); err != nil {
			failed = errors.Join(failed, err)
		}
	}
	if err := r.sink.Flush(); err != nil {
		return errors.Join(failed, err, r.commit(r.ids()))
	}
	for r.clock.Now().Before(deadline) {
		if err := r.deliveries(); err != nil {
			failed = errors.Join(failed, err)
		}
		if !r.tracker.InFlight() {
			return errors.Join(failed, r.commit(r.ids()))
		}
		r.clock.Sleep(10 * time.Millisecond)
	}
	if err := r.commit(r.ids()); err != nil {
		return err
	}
	return errors.Join(failed, fmt.Errorf("outstanding writes did not finish within 10 seconds"))
}

func (r *Runner) admits(input Input) bool {
	n, b := r.tracker.Total()
	if n == 0 {
		return true
	} // One oversized record is admitted alone.
	pn, pb := r.tracker.Work(input.Cursor.Partition)
	size := input.Record.Size()
	return n < r.limits.RecordCap && pn < r.limits.PartitionRecordCap && size <= r.limits.ByteCap-b && size <= r.limits.PartitionByteCap-pb
}

func (r *Runner) admitDeferred() error {
	if r.deferred == nil || !r.admits(*r.deferred) {
		return nil
	}
	input, eof := *r.deferred, r.deferredEOF
	r.deferred, r.deferredEOF = nil, nil
	if err := r.input(input); err != nil {
		return err
	}
	if eof != nil {
		return r.eof(*eof)
	}
	return nil
}
func (r *Runner) flush() error {
	if err := r.sink.Flush(); err != nil {
		return err
	}
	return r.deliveries()
}
