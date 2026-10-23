// Package commitmeta encodes Kafka offset metadata independently of Kafka I/O.
package commitmeta

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"unicode/utf8"

	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
)

const MaxBytes = 4096
const prefix = "kaferry:2;"

// Progress is independent of the serialized field names and representation.
type Progress struct {
	Fingerprint string
	TopicID     string
	Window      *plan.Window
	Stop        *core.Stop
}

type Codec interface {
	Encode(Progress) (string, error)
	Decode(string) (Progress, error)
}

type Error struct{ Kind string }

func (e *Error) Error() string { return e.Kind + " commit metadata" }

type JSONCodec struct{}

func NewCodec() Codec { return JSONCodec{} }

// These private types freeze the wire contract independently of domain types.
type wire struct {
	Fingerprint string  `json:"fp"`
	TopicID     string  `json:"tid"`
	Window      *window `json:"win,omitempty"`
	Stop        *stop   `json:"stop,omitempty"`
}
type window struct {
	Lower *int64 `json:"lower"`
	Upper *int64 `json:"upper"`
}
type stop struct {
	Reason string `json:"reason"`
	At     *int64 `json:"at"`
}

func (JSONCodec) Encode(p Progress) (string, error) {
	if err := validate(p); err != nil {
		return "", err
	}
	w := wire{Fingerprint: p.Fingerprint, TopicID: p.TopicID}
	if p.Window != nil {
		w.Window = &window{p.Window.Lower, p.Window.Upper}
	}
	if p.Stop != nil {
		at := p.Stop.At
		w.Stop = &stop{p.Stop.Reason, &at}
	}
	data, err := json.Marshal(w)
	if err != nil {
		return "", err
	}
	encoded := prefix + string(data)
	if len(encoded) > MaxBytes {
		return "", &Error{Kind: "oversized"}
	}
	return encoded, nil
}

func (JSONCodec) Decode(encoded string) (Progress, error) {
	var p Progress
	if len(encoded) > MaxBytes {
		return p, &Error{Kind: "oversized"}
	}
	if !strings.HasPrefix(encoded, "kaferry:") {
		return p, &Error{Kind: "foreign"}
	}
	if !strings.HasPrefix(encoded, prefix) {
		return p, &Error{Kind: "unsupported version of"}
	}
	data := []byte(strings.TrimPrefix(encoded, prefix))
	if !utf8.Valid(data) || checkObject(data, "fp", "tid", "win", "stop") != nil {
		return p, &Error{Kind: "invalid"}
	}
	var w wire
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&w) != nil {
		return p, &Error{Kind: "invalid"}
	}
	p = Progress{Fingerprint: w.Fingerprint, TopicID: w.TopicID}
	if w.Window != nil {
		p.Window = &plan.Window{Lower: w.Window.Lower, Upper: w.Window.Upper}
	}
	if w.Stop != nil {
		if w.Stop.At == nil {
			return Progress{}, &Error{Kind: "invalid"}
		}
		p.Stop = &core.Stop{Reason: w.Stop.Reason, At: *w.Stop.At}
	}
	if err := validate(p); err != nil {
		return Progress{}, err
	}
	return p, nil
}

func validate(p Progress) error {
	if p.Fingerprint == "" || p.TopicID == "" || !utf8.ValidString(p.Fingerprint+p.TopicID) {
		return &Error{Kind: "invalid"}
	}
	if p.Window != nil && p.Window.Lower != nil && p.Window.Upper != nil && *p.Window.Lower >= *p.Window.Upper {
		return &Error{Kind: "invalid"}
	}
	if p.Stop != nil && (p.Stop.At < 0 || (p.Stop.Reason != "timestamp" && p.Stop.Reason != "tip" && p.Stop.Reason != "worker_record_cap")) {
		return &Error{Kind: "invalid"}
	}
	return nil
}

// checkObject checks exact field names before encoding/json can apply Unicode
// case folding. It also rejects duplicate keys, null objects and trailing data.
func checkObject(data []byte, fields ...string) error {
	d := json.NewDecoder(bytes.NewReader(data))
	first, err := d.Token()
	if err != nil || first != json.Delim('{') {
		return fmt.Errorf("expected object")
	}
	seen := map[string]bool{}
	for d.More() {
		token, err := d.Token()
		if err != nil {
			return err
		}
		name, ok := token.(string)
		if !ok || !slices.Contains(fields, name) || seen[name] {
			return fmt.Errorf("invalid field")
		}
		seen[name] = true
		var raw json.RawMessage
		if err := d.Decode(&raw); err != nil {
			return err
		}
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) && name != "lower" && name != "upper" {
			return fmt.Errorf("null field")
		}
		switch name {
		case "win":
			if err := checkObject(raw, "lower", "upper"); err != nil {
				return err
			}
		case "stop":
			if err := checkObject(raw, "reason", "at"); err != nil {
				return err
			}
		}
	}
	if _, err := d.Token(); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return fmt.Errorf("trailing JSON")
	}
	return nil
}
