package archiveio

import (
	"errors"

	"github.com/pagrus7/kaferry/internal/archive/avro"
	core "github.com/pagrus7/kaferry/internal/copy"
)

type Capture interface {
	Append(avro.Record, core.Cursor) error
	Flush() error
	Finish() error
	Close() error
}

type Sink struct {
	capture Capture
	pending []core.Token
	reports chan core.Delivery
	failed  error
}

func NewSink(capture Capture, capacity int) *Sink {
	return &Sink{capture: capture, reports: make(chan core.Delivery, capacity+1)}
}

func (s *Sink) Submit(record core.Record, token core.Token) error {
	if s.failed != nil {
		return s.failed
	}
	r, err := toArchive(record)
	if err != nil {
		return err
	}
	if err = s.capture.Append(r, core.Cursor{Partition: token.Partition, Position: token.Next}); err != nil {
		var invalid *avro.InvalidRecord
		if errors.As(err, &invalid) {
			return &core.RecordError{Class: "validation", Err: err}
		}
		s.failed = err
		return err
	}
	s.pending = append(s.pending, token)
	return nil
}
func (s *Sink) Flush() error {
	if s.failed != nil {
		return s.failed
	}
	if err := s.capture.Flush(); err != nil {
		s.failed = err
		return err
	}
	for _, t := range s.pending {
		s.reports <- core.Delivery{Token: t}
	}
	s.pending = s.pending[:0]
	return nil
}
func (s *Sink) Finish() error {
	if s.failed != nil {
		return s.failed
	}
	s.failed = s.capture.Finish()
	return s.failed
}
func (s *Sink) Close()                           { _ = s.capture.Close() }
func (s *Sink) Deliveries() <-chan core.Delivery { return s.reports }
