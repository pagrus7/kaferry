package copy

import (
	"errors"
	"fmt"
	"time"
)

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
	return errors.Join(failed, r.commit(r.ids()), fmt.Errorf("outstanding writes did not finish within 10 seconds"))
}
