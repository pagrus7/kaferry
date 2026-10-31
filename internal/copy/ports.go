package copy

import (
	"context"
	"time"
)

// StopObserver starts the shutdown budget and reports the operation in progress.
type StopObserver interface {
	Begin()
	Stage(string)
}
type NoopStopObserver struct{}

func (NoopStopObserver) Begin()       {}
func (NoopStopObserver) Stage(string) {}

// Lifecycle runs synchronously inside Source.Start or Source.Poll, on the
// runner's event loop. Sources assign positions before returning their inputs.
type Lifecycle interface {
	Assign([]Assignment) error
	// Revoke releases tracked work without waiting for deliveries. With lost=false,
	// the runner first tries to save confirmed progress; lost=true forbids that save.
	Revoke(partitions []int32, lost bool) error
}

// Source methods are called serially. Poll must continue servicing ownership
// changes while intake is paused. Close releases resources without saving progress.
type Source interface {
	// Start installs callbacks; Observer may be nil.
	Start(Lifecycle, Observer) error
	// Poll returns owned record buffers that remain valid after later polls.
	// An empty event means no input is available. If both fields are set, the
	// runner handles Input before EOF. The timeout bounds waiting for input,
	// not synchronous metadata, lifecycle or filesystem operations.
	Poll(time.Duration) (Event, error)
	// Pause suppresses input for this partition until resumed, while retaining ownership.
	Pause(int32, bool) error
	// HighWatermark returns Kafka's replicated end; archive sources do not support it.
	HighWatermark(int32) (int64, error)
	Close() error
}

// Sink methods are called serially; adapters synchronize their background work.
// Delivery reports, rather than successful submission or flushing, confirm output.
type Sink interface {
	// Submit accepts a record on nil and may retain its buffers until its delivery
	// outcome. Busy means no acceptance: retry the same record and token later.
	// RecordError identifies a rejected, unsent record that policy may skip.
	// Other errors stop execution; they do not establish that output is absent.
	Submit(Record, Token) error
	// Flush requests completion of accepted writes. It may block for archive sync
	// or return while Kafka deliveries are pending; callers still drain Deliveries.
	Flush() error
	// Deliveries stays open during execution. Reports may arrive out of order or
	// after revocation; the runner ignores tokens from earlier assignments. A Fatal
	// report stops execution without matching a token. Delivery errors are not skippable.
	Deliveries() <-chan Delivery
	// Close releases resources; it does not save source progress.
	Close()
}

// ProgressStore saves next positions and eligible stop/end metadata, serially.
// Callers provide only confirmed prefixes (or validated initial positions).
type ProgressStore interface {
	// Save confirms a partition only when the request error is nil and its result
	// is present and nil. Missing results are failures, not implicit successes.
	// A failure may leave some positions saved; callers must allow replay.
	Save(context.Context, []Cursor) (map[int32]error, error)
}

type Clock interface {
	Now() time.Time
	Sleep(time.Duration)
}
type RealClock struct{}

func (RealClock) Now() time.Time               { return time.Now() }
func (RealClock) Sleep(duration time.Duration) { time.Sleep(duration) }
