// Package copy contains transport-independent delivery and progress logic.
package copy

import (
	"context"
	"time"

	"kafka-fwd2/internal/plan"
)

type Header struct {
	Name  string `cbor:"name"`
	Value []byte `cbor:"value"`
}
type Provenance struct {
	Topic     string `cbor:"topic"`
	Partition int32  `cbor:"partition"`
	Offset    int64  `cbor:"offset"`
}
type Record struct {
	Key           []byte      `cbor:"key"`
	Value         []byte      `cbor:"value"`
	Headers       []Header    `cbor:"headers"`
	TimestampMS   *int64      `cbor:"timestamp_ms"`
	TimestampType string      `cbor:"timestamp_type"`
	Source        *Provenance `cbor:"source,omitempty"`
}

func (r Record) Size() int64 {
	n := int64(len(r.Key) + len(r.Value) + 64)
	for _, h := range r.Headers {
		n += int64(len(h.Name) + len(h.Value) + 24)
	}
	if r.Source != nil {
		n += int64(len(r.Source.Topic) + 24)
	}
	return n
}

type Cursor struct {
	Partition int32
	Position  int64
}
type Input struct {
	Cursor Cursor
	Next   int64
	Record Record
}
type Assignment struct {
	Cursor Cursor
	Window plan.Window
}
type Event struct {
	Input *Input
	EOF   *Cursor
}
type Token struct {
	Partition  int32
	Generation uint64
	Sequence   uint64
}
type Delivery struct {
	Token Token
	Err   error
	Fatal bool
}
type Lifecycle interface {
	Assign([]Assignment) error
	Revoke(partitions []int32, lost bool) error
}
type Source interface {
	Start(Lifecycle) error
	Poll(time.Duration) (Event, error)
	Pause(int32, bool) error
	HighWatermark(int32) (int64, error)
	Close() error
}
type Sink interface {
	Submit(Record, Token) error
	Flush() error
	Deliveries() <-chan Delivery
	Close()
}
type OutputProgress interface{ DurableOutput() (string, int64) }
type ProgressStore interface {
	Save(context.Context, []Cursor) (map[int32]error, error)
}
type Clock interface {
	Now() time.Time
	Sleep(time.Duration)
}
type RealClock struct{}

func (RealClock) Now() time.Time               { return time.Now() }
func (RealClock) Sleep(duration time.Duration) { time.Sleep(duration) }

// RecordError is safe to skip: the failed input is identified and was not sent.
type RecordError struct {
	Class string
	Err   error
}

func (e *RecordError) Error() string { return e.Err.Error() }
func (e *RecordError) Unwrap() error { return e.Err }

// Recoverable marks a progress-store failure after which releasing a revoked
// partition only replays work from its last saved position.
type Recoverable struct{ Err error }

func (e *Recoverable) Error() string { return e.Err.Error() }
func (e *Recoverable) Unwrap() error { return e.Err }

// Exit carries the exit code implied by the stage a run failed in: conflicts
// found before intake need operator action (2); anything after intake is 1.
type Exit struct {
	Err  error
	Code int
}

func (e *Exit) Error() string { return e.Err.Error() }
func (e *Exit) Unwrap() error { return e.Err }
func (e *Exit) ExitCode() int { return e.Code }

type Busy struct{}

func (*Busy) Error() string { return "destination queue is full" }
