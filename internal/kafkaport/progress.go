package kafkaport

import (
	"fmt"

	"github.com/pagrus7/kaferry/internal/commitmeta"
	"github.com/pagrus7/kaferry/internal/plan"
)

type Conflict struct{ Err error }

func (e *Conflict) Error() string               { return e.Err.Error() }
func (e *Conflict) Unwrap() error               { return e.Err }
func (*Conflict) ExitCode() int                 { return 2 }
func conflict(format string, args ...any) error { return &Conflict{Err: fmt.Errorf(format, args...)} }

type Progress = commitmeta.Progress

func DecodeProgress(codec commitmeta.Codec, metadata *string, fp, topicID string, relative bool) (Progress, error) {
	if metadata == nil {
		return Progress{}, conflict("foreign commit metadata")
	}
	progress, err := codec.Decode(*metadata)
	if err != nil {
		return Progress{}, &Conflict{Err: err}
	}

	if progress.Fingerprint != fp {
		return progress, conflict("query fingerprint mismatch")
	}
	if progress.TopicID == "" || progress.TopicID != topicID {
		return progress, conflict("source topic identity mismatch: saved=%s current=%s", progress.TopicID, topicID)
	}
	if relative && progress.Window == nil {
		return progress, conflict("NOW() commit is missing its window")
	}
	return progress, nil
}

func ValidateWindow(progress Progress, spec plan.WindowSpec, staleness int64) error {
	if progress.End != nil && spec.Upper == nil {
		return conflict("captured end requires an upper timestamp bound")
	}
	if progress.Window != nil {
		if err := spec.ValidateStored(*progress.Window, staleness); err != nil {
			return conflict("invalid stored time window: %v", err)
		}
	}
	return nil
}
