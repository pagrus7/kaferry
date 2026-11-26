package app

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	core "github.com/pagrus7/kaferry/src/internal/copy"
	"github.com/pagrus7/kaferry/src/internal/inspect"
	"github.com/stretchr/testify/require"
)

func TestStatusTextIsBoundedAndJSONRetainsAllPartitions(t *testing.T) {
	// Given twelve partitions, including a missing offset, offset zero and different stops.
	zero := int64(0)
	r := inspect.StatusReport{Version: 3, Verdict: "incomplete", Reason: "partitions have no saved stop", Group: "group\nforged", Source: inspect.Endpoint{Kind: "TOPIC", Name: "source"}, Destination: inspect.Endpoint{Kind: "TOPIC", Name: "dest"}}
	for n := range 12 {
		row := inspect.SavedPartition{ID: int32(n)}
		if n > 0 {
			row.Position = &zero
			row.End = &zero
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
	require.Contains(t, text.String(), "captured end 0")
	require.NotContains(t, text.String(), "group\nforged")
	var decoded struct {
		Version    int    `json:"version"`
		Verdict    string `json:"verdict"`
		Partitions []struct {
			ID       int32  `json:"id"`
			Position *int64 `json:"position"`
			End      *int64 `json:"captured_end"`
			Stop     *struct {
				Reason string `json:"reason"`
				At     int64  `json:"at"`
			} `json:"stop"`
		} `json:"partitions"`
	}
	require.NoError(t, json.Unmarshal(data.Bytes(), &decoded))
	require.EqualValues(t, 3, decoded.Version)
	require.Equal(t, "incomplete", decoded.Verdict)
	require.Len(t, decoded.Partitions, 12)
	require.Nil(t, decoded.Partitions[0].Position)
	require.NotNil(t, decoded.Partitions[1].Position)
	require.Zero(t, *decoded.Partitions[1].Position)
	require.Equal(t, &zero, decoded.Partitions[1].End)
	require.Equal(t, "tip", decoded.Partitions[11].Stop.Reason)
}

func TestStatusRendersOnlyReportGuidance(t *testing.T) {
	const timestampNote = "Timestamp assumptions apply."
	const completionNote = "Destination data was not verified."
	const archiveNote = "The saved file selection is fixed."
	verdicts := inspect.StatusVerdicts()
	for _, tc := range []struct {
		verdict, next string
		notes         []string
		archive       bool
	}{
		{verdict: "unknown", next: "Check the reason."},
		{verdict: "running", next: "Check again later."},
		{verdict: "incomplete", next: "Resume the job."},
		{verdict: "limit_reached", next: "Rerun the job.", notes: []string{archiveNote}, archive: true},
		{verdict: "complete_within_disorder", notes: []string{timestampNote, completionNote}},
		{verdict: "complete", notes: []string{completionNote, archiveNote}, archive: true},
	} {
		t.Run(tc.verdict, func(t *testing.T) {
			// Given a report with only the guidance relevant to its verdict and source.
			r := inspect.StatusReport{Version: 3, Verdict: tc.verdict, Meaning: verdicts[tc.verdict].Meaning, Notes: tc.notes, Next: tc.next, Activity: "group observed idle twice"}
			r.Source.Kind = "TOPIC"
			if tc.archive {
				r.Source.Kind, r.Activity = "ARCHIVE", "not checked for files"
			}
			var output, data bytes.Buffer

			// When rendering the report in both formats.
			require.NoError(t, renderStatus(&output, r, false))
			require.NoError(t, renderStatus(&data, r, true))

			// Then the text contains one meaning and exactly the supplied notes and action.
			want := append([]string{"Meaning: " + r.Meaning}, tc.notes...)
			if tc.next != "" {
				want = append(want, "Next: "+tc.next)
			}
			want = append(want, "Activity: "+r.Activity+".", "All statuses: kaferry help status", "")
			_, guidance, found := strings.Cut(output.String(), "\n\nMeaning: ")
			require.True(t, found, "%s", output.String())
			require.Equal(t, strings.Join(want, "\n"), "Meaning: "+guidance)
			require.NotRegexp(t, `(?m)^Status +\| Meaning`, output.String())

			// Then JSON carries the same guidance and omits empty optional fields.
			var decoded map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(data.Bytes(), &decoded))
			require.JSONEq(t, "3", string(decoded["version"]))
			require.JSONEq(t, strconv.Quote(tc.verdict), string(decoded["verdict"]))
			require.JSONEq(t, strconv.Quote(r.Meaning), string(decoded["meaning"]))
			if tc.notes == nil {
				require.NotContains(t, decoded, "notes")
			} else {
				var notes []string
				require.NoError(t, json.Unmarshal(decoded["notes"], &notes))
				require.Equal(t, tc.notes, notes)
			}
			if tc.next == "" {
				require.NotContains(t, decoded, "next")
			} else {
				require.JSONEq(t, strconv.Quote(tc.next), string(decoded["next"]))
			}
		})
	}
}

func TestArchiveStatusTextReportsSavedSnapshot(t *testing.T) {
	// Given an Avro group's selected files, which may differ from current directory contents.
	report := inspect.StatusReport{Source: inspect.Endpoint{Kind: "ARCHIVE", File: &inspect.ArchiveSummary{FileCount: 3, SizeBytes: 150}}}
	var output bytes.Buffer
	// When rendering status, then bytes describe that snapshot rather than the current directory.
	require.NoError(t, renderStatus(&output, report, false))
	require.Contains(t, output.String(), "Snapshot: 3 completed files, 150 encoded bytes.")
}

func TestFileStatusLabelsReplayProgressInsteadOfAnOriginalPartition(t *testing.T) {
	for _, saved := range []bool{false, true} {
		// Given file replay whose original partition differs from the internal progress cursor.
		r := inspect.StatusReport{Source: inspect.Endpoint{Kind: "ARCHIVE"}, Partitions: []inspect.SavedPartition{{ID: 0}}}
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
