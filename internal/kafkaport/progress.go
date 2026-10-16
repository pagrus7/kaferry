package kafkaport

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/pagrus7/kaferry/internal/plan"
)

type Conflict struct{ Err error }

func (e *Conflict) Error() string               { return e.Err.Error() }
func (e *Conflict) Unwrap() error               { return e.Err }
func (*Conflict) ExitCode() int                 { return 2 }
func conflict(format string, args ...any) error { return &Conflict{Err: fmt.Errorf(format, args...)} }

type Progress struct {
	Fingerprint string       `json:"fp"`
	TopicID     string       `json:"tid"`
	Window      *plan.Window `json:"win,omitempty"`
	FileID      string       `json:"fid,omitempty"`
	Extent      int64        `json:"extent,omitempty"`
}

// The v1 wire prefix is fixed; changing it makes saved progress unreadable.
func (p Progress) Encode() string { data, _ := json.Marshal(p); return "kfwd2:1;" + string(data) }

func DecodeProgress(metadata *string, fp, topicID string, relative bool) (Progress, error) {
	var progress Progress
	if metadata == nil || !strings.HasPrefix(*metadata, "kfwd2:1;") {
		return progress, conflict("foreign or unsupported commit metadata")
	}
	decoder := json.NewDecoder(strings.NewReader(strings.TrimPrefix(*metadata, "kfwd2:1;")))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&progress); err != nil || decoder.Decode(new(any)) != io.EOF {
		return progress, conflict("invalid commit metadata")
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
	if progress.Extent < 0 || (progress.FileID == "") != (progress.Extent == 0) {
		return progress, conflict("invalid destination file progress")
	}
	if progress.Window != nil && progress.Window.Lower != nil && progress.Window.Upper != nil && *progress.Window.Lower >= *progress.Window.Upper {
		return progress, conflict("invalid stored time window")
	}
	return progress, nil
}

func ValidateWindow(progress Progress, spec plan.WindowSpec, cutoff int64) error {
	if progress.Window != nil {
		if err := spec.ValidateStored(*progress.Window, cutoff); err != nil {
			return conflict("invalid stored time window: %v", err)
		}
	}
	return nil
}
