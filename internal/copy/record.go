// Package copy runs delivery and progress logic through source and sink ports.
package copy

import (
	"github.com/pagrus7/kaferry/internal/plan"
)

type Header struct {
	Name  string
	Value []byte
}
type Provenance struct {
	Cluster   string
	TopicID   [16]byte
	Topic     string
	Partition int32
	Offset    int64
}
type Record struct {
	Key           []byte
	Value         []byte
	Headers       []Header
	TimestampMS   *int64
	TimestampType string
	Source        *Provenance
}

func (r Record) Size() int64 {
	n := int64(len(r.Key) + len(r.Value) + 64)
	for _, h := range r.Headers {
		n += int64(len(h.Name) + len(h.Value) + 24)
	}
	if r.Source != nil {
		n += int64(len(r.Source.Cluster) + len(r.Source.Topic) + 40)
	}
	return n
}

// Stop records the source position where a partition reached its stopping rule.
type Stop struct {
	Reason string `json:"reason"`
	At     int64  `json:"at"`
}

// Cursor is a source's next position, independent of original record provenance.
// Kafka uses one cursor per partition; archive replay uses virtual partition zero.
type Cursor struct {
	Partition int32
	Position  int64
	Stop      *Stop
	End       *int64
}

// Input spans Cursor.Position through Next (exclusive), including source gaps.
type Input struct {
	Cursor Cursor
	Next   int64
	Record Record
}

type Assignment struct {
	Cursor Cursor
	Window plan.Window
}

// Event may carry input, an observed end position, both, or neither. EOF is a
// Kafka tip observation, not necessarily permanent completion; archives end there.
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

// RecordError is safe to skip: the failed input is identified and was not sent.
type RecordError struct {
	Class string
	Err   error
}

func (e *RecordError) Error() string { return e.Err.Error() }
func (e *RecordError) Unwrap() error { return e.Err }

// Recoverable marks a classified transient operation error. The caller decides
// whether to retry a tip query or release revoked progress for replay; initial
// and periodic commit failures remain terminal.
type Recoverable struct{ Err error }

func (e *Recoverable) Error() string { return e.Err.Error() }
func (e *Recoverable) Unwrap() error { return e.Err }

// Exit carries a classified CLI outcome, including an unassigned timeout (3).
// Unclassified conflicts use 2 before intake and 1 after intake.
type Exit struct {
	Err  error
	Code int
}

func (e *Exit) Error() string { return e.Err.Error() }
func (e *Exit) Unwrap() error { return e.Err }
func (e *Exit) ExitCode() int { return e.Code }

type Busy struct{}

func (*Busy) Error() string { return "destination queue is full" }
