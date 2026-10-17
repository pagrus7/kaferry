package app

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	core "github.com/pagrus7/kaferry/internal/copy"
)

type Shutdown interface {
	core.StopObserver
	Configure(time.Duration)
}

type shutdownBudget struct {
	mu        sync.Mutex
	timeout   time.Duration
	started   time.Time
	stage     string
	timer     *time.Timer
	done      chan struct{}
	closed    bool
	expired   bool
	terminate func(string)
}

func newShutdownBudget(ctx context.Context, timeout time.Duration, terminate func(string)) *shutdownBudget {
	b := &shutdownBudget{timeout: timeout, stage: "starting query", done: make(chan struct{}), terminate: terminate}
	go func() {
		select {
		case <-ctx.Done():
			b.Begin()
		case <-b.done:
		}
	}()
	return b
}
func (b *shutdownBudget) Configure(timeout time.Duration) {
	if timeout <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.timeout = timeout
	if b.timer != nil && !b.closed && !b.expired {
		b.timer.Reset(max(0, timeout-time.Since(b.started)))
	}
}
func (b *shutdownBudget) Stage(stage string) { b.mu.Lock(); b.stage = stage; b.mu.Unlock() }
func (b *shutdownBudget) Begin() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || b.timer != nil {
		return
	}
	b.started = time.Now()
	b.timer = time.AfterFunc(b.timeout, func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if !b.closed && !b.expired && time.Since(b.started) >= b.timeout {
			b.expired = true
			b.terminate(b.stage)
		}
	})
}
func (b *shutdownBudget) Done() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	if b.timer != nil {
		b.timer.Stop()
	}
	close(b.done)
}
func forceShutdown(stage string) {
	// A full stderr pipe must not prevent the exit. Printing is best effort.
	printed := make(chan struct{})
	go func() {
		fmt.Fprintf(os.Stderr, "shutdown timed out while %s; exiting without further cleanup\n", stage)
		close(printed)
	}()
	select {
	case <-printed:
	case <-time.After(20 * time.Millisecond):
	}
	os.Exit(1)
}
