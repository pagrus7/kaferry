package copy

import "errors"

func (r *Runner) workerStopReached() bool {
	return r.plan.WorkerStopAfterRecords > 0 && r.submitted >= r.plan.WorkerStopAfterRecords
}

func (r *Runner) finishWorkerStop() error {
	err := r.finishWrites()
	if err == nil {
		for id, p := range r.partitions {
			if p.stop == nil {
				p.stop = &Stop{Reason: "worker_record_cap", At: r.tracker.Confirmed(id).Position}
			}
		}
	}
	err = errors.Join(err, r.commit(r.ids()))
	if err == nil {
		r.log.Info("worker stopped", "reason", "worker record threshold reached", "worker_stop_after_records", r.plan.WorkerStopAfterRecords)
	}
	return err
}
