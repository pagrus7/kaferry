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
	"github.com/stretchr/testify/require"
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
	require.NoError(t, renderStatus(&text, r, false))
	require.NoError(t, renderStatus(&data, r, true))
	// Then text is bounded, user names are quoted and zero is retained in JSON.
	require.EqualValues(t, 8, strings.Count(text.String(), "  Partition "))
	require.Contains(t, text.String(), "4 more partitions")
	require.Contains(t, text.String(), "offset 0")
	require.NotContains(t, text.String(), "group\nforged")
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
	require.NoError(t, json.Unmarshal(data.Bytes(), &decoded))
	require.EqualValues(t, 1, decoded.Version)
	require.Equal(t, "incomplete", decoded.Verdict)
	require.Len(t, decoded.Partitions, 12)
	require.Nil(t, decoded.Partitions[0].Position)
	require.NotNil(t, decoded.Partitions[1].Position)
	require.Zero(t, *decoded.Partitions[1].Position)
	require.Equal(t, "tip", decoded.Partitions[11].Stop.Reason)
}

type statusWriter struct{ err error }

func (w statusWriter) Write([]byte) (int, error) { return 0, w.err }
func TestStatusOutputFailuresRemainErrors(t *testing.T) {
	for _, asJSON := range []bool{false, true} {
		t.Run(fmt.Sprint(asJSON), func(t *testing.T) {
			// Given a failed or short stdout write, when rendering a status, then the caller gets an error.
			failed := errors.New("stdout failed")
			require.ErrorIs(t, renderStatus(statusWriter{failed}, inspect.StatusReport{}, asJSON), failed)
			require.ErrorIs(t, renderStatus(statusWriter{}, inspect.StatusReport{}, asJSON), io.ErrShortWrite)
		})
	}
}

func TestFileStatusTextReportsBytePositionStopAndCurrentSize(t *testing.T) {
	// Given saved EOF progress and the current source archive size.
	position := int64(120)
	report := inspect.StatusReport{Verdict: "stopped", Group: "restore", Source: inspect.Endpoint{Kind: "FILE", Name: "in.cbor", File: &inspect.FileSummary{SizeBytes: 150}}, Destination: inspect.Endpoint{Kind: "FILE", Name: "out.cbor"}, Partitions: []inspect.SavedPartition{{ID: 0, Position: &position, Stop: &core.Stop{Reason: "eof", At: 120}}}}
	var output bytes.Buffer
	// When rendering status as text.
	err := renderStatus(&output, report, false)
	// Then file cursor and stop are explicit and source size uses bytes.
	require.NoError(t, err)
	require.Contains(t, output.String(), "Partition 0: byte position 120; file end reached at 120")
	require.Contains(t, output.String(), "Current source size: 150 bytes")
}
