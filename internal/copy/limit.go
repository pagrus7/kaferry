package copy

func (r *Runner) limitReached() bool {
	return r.plan.Limit > 0 && r.submitted >= r.plan.Limit
}

func (r *Runner) finishLimit() error {
	r.stop.Begin()
	r.stop.Stage("reporting record limit")
	r.log.Info("record limit reached", "limit", r.plan.Limit)
	return r.drain()
}
