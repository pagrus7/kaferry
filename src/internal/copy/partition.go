package copy

import "github.com/pagrus7/kaferry/src/internal/plan"

// partition holds one assignment's completion state; it does not perform I/O.
type partition struct {
	stop      *Stop
	stopSaved bool
	window    plan.Window
	paused    bool
	position  int64
	high      *int64
}

func (p *partition) reachedHigh(position int64) bool {
	return p.high != nil && position >= *p.high
}

func (p *partition) pastTimestamp(stamp *int64, disorder int64) (bool, error) {
	if stamp == nil || p.window.Upper == nil {
		return false, nil
	}
	threshold, err := p.window.StopThreshold(disorder)
	return err == nil && *stamp > threshold, err
}

func (p *partition) reachedTip(position int64, fileSource bool) {
	p.position = position
	if fileSource {
		p.stop = &Stop{Reason: "eof", At: position}
	}
}
