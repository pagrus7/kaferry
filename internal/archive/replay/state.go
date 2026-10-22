package replay

import (
	"errors"
	"strings"
	"unicode/utf8"

	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
)

const maxCheckpointBytes = 64 << 20

type State struct {
	Version     int         `json:"version"`
	Group       string      `json:"group"`
	Fingerprint string      `json:"fingerprint"`
	Window      plan.Window `json:"window"`
	Position    int64       `json:"position"`
	Complete    bool        `json:"complete"`
	Files       []Progress  `json:"files"`
	Stop        *core.Stop  `json:"stop,omitempty"`
}

func (s State) validate() error {
	if s.Version != 1 || !validGroup(s.Group) || s.Fingerprint == "" || len(s.Fingerprint) > 4096 || !utf8.ValidString(s.Fingerprint) || s.Position < 0 || s.Files == nil {
		return errors.New("invalid or unsupported replay checkpoint")
	}
	if s.Stop != nil && (s.Stop.At != s.Position || s.Stop.Reason != "worker_record_cap" && s.Stop.Reason != "eof" || s.Stop.Reason == "eof" && !s.Complete) {
		return errors.New("invalid replay stop")
	}
	if s.Window.Lower != nil && s.Window.Upper != nil && *s.Window.Lower >= *s.Window.Upper {
		return errors.New("invalid replay time window")
	}
	if err := validateProgress(s.Files); err != nil {
		return err
	}
	allDone, allInitial := true, true
	for _, p := range s.Files {
		allDone = allDone && p.ended()
		allInitial = allInitial && p.Next == initial(p.File)
	}
	if s.Complete != allDone || (s.Position == 0) != allInitial {
		return errors.New("replay completion or sequence disagrees with file cursors")
	}
	return nil
}

func validateProgress(files []Progress) error {
	seen := make(map[string]bool, len(files))
	for _, p := range files {
		if err := p.validate(); err != nil {
			return err
		}
		if seen[p.File.Identity.FileID] {
			return errors.New("duplicate archive file ID")
		}
		seen[p.File.Identity.FileID] = true
	}
	return nil
}

func (s State) clone() State {
	s.Files = append([]Progress{}, s.Files...)
	s.Window = cloneWindow(s.Window)
	if s.Stop != nil {
		s.Stop = new(*s.Stop)
	}
	return s
}
func cloneWindow(w plan.Window) plan.Window {
	if w.Lower != nil {
		w.Lower = new(*w.Lower)
	}
	if w.Upper != nil {
		w.Upper = new(*w.Upper)
	}
	return w
}

func validGroup(group string) bool {
	return strings.TrimSpace(group) != "" && len(group) <= 4096 && utf8.ValidString(group)
}
