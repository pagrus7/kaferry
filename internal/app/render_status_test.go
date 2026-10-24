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

func TestArchiveStatusTextReportsSavedSnapshot(t *testing.T) {
	// Given an Avro group's selected files, which may differ from current directory contents.
	report := inspect.StatusReport{Source: inspect.Endpoint{Kind: "FILE", File: &inspect.FileSummary{FileCount: 3, SizeBytes: 150}}}
	var output bytes.Buffer
	// When rendering status, then bytes describe that snapshot rather than the current directory.
	require.NoError(t, renderStatus(&output, report, false))
	require.Contains(t, output.String(), "Snapshot: 3 completed files, 150 encoded bytes.")
}

func TestFileStatusLabelsReplayProgressInsteadOfAnOriginalPartition(t *testing.T) {
	for _, saved := range []bool{false, true} {
		// Given file replay whose original partition differs from the internal progress cursor.
		r := inspect.StatusReport{Source: inspect.Endpoint{Kind: "FILE"}, Partitions: []inspect.SavedPartition{{ID: 0}}}
		if saved {
			position := int64(2)
			r.Partitions[0].Position = &position
		}
		var out bytes.Buffer
		// When rendering, then describe replay progress without implying original partition zero was selected.
		require.NoError(t, renderStatus(&out, r, false))
		require.NotContains(t, out.String(), "Partition 0:")
		if saved {
			require.Contains(t, out.String(), "Replay: input records 2")
		} else {
			require.Contains(t, out.String(), "Replay: no saved progress")
		}
	}
}
