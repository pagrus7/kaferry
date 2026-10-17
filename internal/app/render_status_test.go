package app

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/inspect"
)

func TestStatusTextIsBoundedAndJSONRetainsAllPartitions(t *testing.T) {
	// Given twelve partitions, including a missing offset, offset zero and different stops.
	zero := int64(0)
	r := inspect.StatusReport{Version: 1, Verdict: "incomplete", Reason: "partitions have no saved stop", Group: "group\nforged", Meaning: inspect.StatusMeaning, Source: inspect.Endpoint{Kind: "TOPIC", Name: "source"}, Destination: inspect.Endpoint{Kind: "TOPIC", Name: "dest"}}
	for n := range 12 {
		row := inspect.SavedPartition{ID: int32(n)}
		if n > 0 {
			row.Position = &zero
		}
		if n > 1 {
			row.Stop = &core.Stop{Reason: "tip"}
		}
		r.Partitions = append(r.Partitions, row)
	}
	var text, data bytes.Buffer
	// When rendering text and JSON.
	if err := renderStatus(&text, r, false); err != nil {
		t.Fatal(err)
	}
	if err := renderStatus(&data, r, true); err != nil {
		t.Fatal(err)
	}
	// Then text is bounded, user names are quoted and zero is retained in JSON.
	if strings.Count(text.String(), "  Partition ") != 8 || !strings.Contains(text.String(), "4 more partitions") || !strings.Contains(text.String(), "offset 0") || strings.Contains(text.String(), "group\nforged") {
		t.Fatal(text.String())
	}
	var decoded struct {
		Version    int    `json:"version"`
		Verdict    string `json:"verdict"`
		Partitions []struct {
			ID       int32  `json:"id"`
			Position *int64 `json:"position"`
			Stop     *struct {
				Reason string `json:"reason"`
				At     int64  `json:"at"`
			} `json:"stop"`
		} `json:"partitions"`
	}
	if err := json.Unmarshal(data.Bytes(), &decoded); err != nil || decoded.Version != 1 || decoded.Verdict != "incomplete" || len(decoded.Partitions) != 12 || decoded.Partitions[0].Position != nil || decoded.Partitions[1].Position == nil || *decoded.Partitions[1].Position != 0 || decoded.Partitions[11].Stop.Reason != "tip" {
		t.Fatal(data.String(), err)
	}
}

type statusWriter struct{ err error }

func (w statusWriter) Write([]byte) (int, error) { return 0, w.err }
func TestStatusOutputFailuresRemainErrors(t *testing.T) {
	for _, asJSON := range []bool{false, true} {
		t.Run(fmt.Sprint(asJSON), func(t *testing.T) {
			// Given a failed or short stdout write, when rendering a status, then the caller gets an error.
			failed := errors.New("stdout failed")
			if err := renderStatus(statusWriter{failed}, inspect.StatusReport{}, asJSON); !errors.Is(err, failed) {
				t.Fatal(err)
			}
			if err := renderStatus(statusWriter{}, inspect.StatusReport{}, asJSON); !errors.Is(err, io.ErrShortWrite) {
				t.Fatal(err)
			}
		})
	}
}
