package copy

import (
	"time"

	"github.com/pagrus7/kaferry/internal/plan"
)

// partition holds one assignment's completion state; it does not perform I/O.
type partition struct {
	stop        *Stop
	stopSaved   bool
	window      plan.Window
	paused      bool
	position    int64
	stable      int64
	atTip       bool
	high        *int64
	endSaved    bool
	waitStarted time.Time
	sampling    retrySchedule
}

type tipAction uint8

const (
	tipWait tipAction = iota
	tipSample
	tipStop
	tipTimeout
)

func (p *partition) reachedHigh(position int64) bool {
	return p.high != nil && position >= *p.high
}

func (p *partition) pastTimestamp(stamp *int64, staleness int64) (bool, error) {
	if stamp == nil || p.window.Upper == nil {
		return false, nil
	}
	threshold, err := p.window.StopThreshold(staleness)
	return err == nil && *stamp > threshold, err
}

func (p *partition) reachedTip(position int64, fileSource bool) {
	if position > p.position {
		p.waitStarted = time.Time{}
	}
	p.atTip = true
	p.position, p.stable = position, position
	if fileSource {
		p.stop = &Stop{Reason: "eof", At: position}
	}
}

func (p *partition) sampled(high int64) {
	p.high = &high
}

// tipAction is called for active, bounded Kafka partitions. It decides whether
// to sample, stop or time out. A successful sample
// stays fixed across assignments through saved progress.
func (p *partition) tipAction(now time.Time, staleness int64, wait time.Duration) (tipAction, error) {
	if p.high == nil {
		if staleness < 0 {
			return tipWait, nil
		}
		threshold, err := p.window.StopThreshold(staleness)
		if err != nil {
			return tipWait, err
		}
		if now.UnixMilli() > threshold && p.sampling.due(now) {
			return tipSample, nil
		}
		return tipWait, nil
	}
	if p.position >= *p.high {
		return tipStop, nil
	}
	if !p.atTip {
		p.waitStarted = time.Time{}
		return tipWait, nil
	}
	if p.waitStarted.IsZero() {
		p.waitStarted = now
	}
	if now.Sub(p.waitStarted) > wait {
		return tipTimeout, nil
	}
	return tipWait, nil
}
