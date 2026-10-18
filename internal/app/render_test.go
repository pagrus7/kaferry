package app

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/pagrus7/kaferry/internal/inspect"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
)

func explanationFixture() inspect.Report {
	lo, hi := int64(1000), int64(2000)
	return inspect.Report{Version: 1, Fingerprint: "query-id", Group: "backfill", CutoffMS: 1000, StartRule: "WINDOW",
		Source:       inspect.Endpoint{Kind: "TOPIC", Name: "input", Connection: "prod", ClusterID: "cluster-id", TopicID: "topic-id", PartitionCount: 1},
		Destination:  inspect.Endpoint{Kind: "FILE", Name: "out.cbor", File: &inspect.FileSummary{}},
		Partitions:   []inspect.Partition{{ID: 0, Start: 4, LogEnd: 12, Saved: true, Window: plan.Window{Lower: &lo, Upper: &hi}, WindowOrigin: "literal"}},
		RecordErrors: inspect.ErrorPolicies{MissingTimestamp: "FAIL", RejectedTimestamp: "FAIL", OtherRecord: "FAIL"}, Warnings: []string{}}
}

func TestExplanationJSONContract(t *testing.T) {
	// Given the public version-one JSON report.
	want, err := os.ReadFile("testdata/explain.json")
	require.NoError(t, err)
	var output bytes.Buffer
	// When rendering, then field names, units and outcomes match the contract.
	require.NoError(t, renderExplanation(&output, explanationFixture(), true))
	require.Equal(t, want, output.Bytes())
}

func textExplanation(t *testing.T, report inspect.Report) string {
	t.Helper()
	var output bytes.Buffer
	require.NoError(t, renderExplanation(&output, report, false))
	return output.String()
}

func TestTextExplanationStructuresBehaviorAndEscapesNames(t *testing.T) {
	// Given a bounded report with one saved partition and names containing terminal controls.
	report := explanationFixture()
	report.Source.Name, report.Group = "input\n\x1b", "group\n\x1b"
	report.Warnings = []string{"Partition 0: saved offset 1 expired; start is log start 4."}
	// When rendering, then lead with endpoints and progress, followed by behavior and limitations.
	output := textExplanation(t, report)
	previous := -1
	for _, section := range []string{"Endpoints\n", "\nProgress\n", "\nTime window\n", "\nBehavior\n", "\nAttention\n", "\nPreview limits\n"} {
		position := strings.Index(output, section)
		require.Greater(t, position, previous)
		previous = position
	}
	for _, want := range []string{`input\n\x1b`, `group\n\x1b`, "Resume: 1 of 1 partitions", "1970-01-01T00:00:01Z (inclusive)", "1970-01-01T00:00:02Z (exclusive)", "1970-01-01T00:00:03Z", "clock passes it and reading reaches a sampled log end", "Open transactions", "worker.tip_wait", "saved offset 1 expired", "Record errors: stop", "Output: create a new file"} {
		require.Contains(t, output, want)
	}
	for _, absent := range []string{"query-id", "topic-id", "lookup end", "span", "Run permissions", "\x1b"} {
		require.NotContains(t, output, absent)
	}
}

type shortOutput struct{}

func (shortOutput) Write([]byte) (int, error) { return 0, nil }

func TestExplanationOutputFailures(t *testing.T) {
	for _, asJSON := range []bool{false, true} {
		// Given closed and short-writing output streams.
		// When writing the buffered report.
		closedErr := renderExplanation(closedOutput{}, explanationFixture(), asJSON)
		shortErr := renderExplanation(shortOutput{}, explanationFixture(), asJSON)
		// Then both output failures reach the caller with their original classification.
		require.EqualError(t, closedErr, "output closed")
		require.ErrorIs(t, shortErr, io.ErrShortWrite)
	}
}

func TestTextExplanationDistinguishesSavedAndFreshWindows(t *testing.T) {
	// Given equal saved/adopted bounds and different fresh NOW bounds.
	report := explanationFixture()
	report.Partitions[0].WindowOrigin = "saved"
	adopted, fresh := report.Partitions[0], report.Partitions[0]
	adopted.ID, adopted.WindowOrigin, adopted.Saved = 1, "adopted", false
	hi := int64(6000)
	fresh.ID, fresh.WindowOrigin, fresh.Window.Upper, fresh.Saved = 2, "resolved", &hi, false
	report.Partitions = append(report.Partitions, adopted, fresh)
	// When rendering, then keep each window's origin, bounds and stop threshold distinct.
	output := textExplanation(t, report)
	for _, want := range []string{"Resume: 1 of 3", "Without saved offsets: 2", "saved progress", "adopted from the lowest saved partition", "NOW() preview only", "first timestamp lookup match", "1970-01-01T00:00:02Z", "1970-01-01T00:00:06Z", "1970-01-01T00:00:07Z"} {
		require.Contains(t, output, want)
	}
}

func TestTextExplanationFollowingRoutingAndSkipPolicies(t *testing.T) {
	for _, policy := range []string{"LogAppendTime", ""} {
		t.Run(policy, func(t *testing.T) {
			// Given a following copy, destination routing and one explicit SKIP policy.
			report := explanationFixture()
			report.Partitions[0].Window, report.Partitions[0].Saved = plan.Window{}, false
			report.StartRule = "LATEST"
			report.Destination = inspect.Endpoint{Kind: "TOPIC", Name: "output", Connection: "backup", ClusterID: "other-cluster", PartitionCount: 3, TimestampType: policy}
			report.RecordErrors.RejectedTimestamp = "SKIP"
			// When rendering, then state continuous follow, dropped-record scope, and whether the timestamp policy is known.
			output := textExplanation(t, report)
			for _, want := range []string{"current read-committed end", "Filter: none", "no end time; follow", "original key bytes; destination partitioner", "Skip before writing: rejected timestamps (records dropped)", "Delivery and unclassified errors: stop"} {
				require.Contains(t, output, want)
			}
			if policy == "" {
				require.Contains(t, output, "Destination timestamp policy: unknown.")
			} else {
				require.Contains(t, output, policy)
			}
			require.NotContains(t, output, "Stop threshold")
			require.NotContains(t, output, "Attention")
		})
	}
}

func TestTextExplanationLimitedStopRules(t *testing.T) {
	for _, source := range []string{"file", "following topic", "time-bounded topic"} {
		t.Run(source, func(t *testing.T) {
			// Given a limited copy that can also reach its source's normal stop.
			report := explanationFixture()
			report.WorkerStopAfterRecords = 5
			want := "Stop: worker record threshold or all assigned partitions stopping, whichever comes first."
			switch source {
			case "file":
				position, window := int64(0), plan.Window{}
				saved := false
				report.Source = inspect.Endpoint{Kind: "FILE", File: &inspect.FileSummary{StartBytes: &position, Window: &window, CheckpointExists: &saved}}
				report.Partitions = nil
				want = "Stop: worker record threshold or end of input as observed when the run opens the file, whichever comes first"
			case "following topic":
				report.Partitions[0].Window = plan.Window{}
				want = "Stop: follow new records until the worker record threshold is reached or interrupted."
			}
			// When rendering, then the stop description includes the limit and source completion.
			output := textExplanation(t, report)
			require.Contains(t, output, want)
			require.NotContains(t, output, "follow new records until stopped")
		})
	}
}

func TestTextExplanationFileProgressAndOpeningSize(t *testing.T) {
	for _, saved := range []bool{false, true} {
		t.Run(fmt.Sprint(saved), func(t *testing.T) {
			// Given a time-filtered file source with 300 encoded bytes after the selected position.
			report := explanationFixture()
			position, upper, extent := int64(200), int64(2000), int64(100)
			window := plan.Window{Upper: &upper}
			report.Source = inspect.Endpoint{Kind: "FILE", Name: "input.cbor", File: &inspect.FileSummary{Exists: true, SizeBytes: 500, StartBytes: &position, Window: &window, CheckpointExists: &saved}}
			report.Partitions = nil
			report.OneOff = !saved
			if saved {
				report.Source.File.Checkpoint = "saved.json"
				report.Destination.File = &inspect.FileSummary{Exists: true, SizeBytes: 120, AcknowledgedExtent: &extent}
			}
			// When rendering, then report bytes before filtering and EOF, never a timestamp stop or work estimate.
			output := textExplanation(t, report)
			for _, want := range []string{"Input position: 200 bytes", "Encoded input after position: 300 bytes (before filtering)", "Stop: end of input as observed when the run opens the file"} {
				require.Contains(t, output, want)
			}
			if saved {
				for _, want := range []string{"saved file checkpoint", "saved.json", "resume existing file", "Acknowledged output: 100 bytes", "reads every existing output frame"} {
					require.Contains(t, output, want)
				}
			} else {
				require.Contains(t, output, "each run starts fresh")
				require.Contains(t, output, "Resume an unfinished run with its logged generated group name")
				require.Contains(t, output, "no saved checkpoint")
			}
			require.NotContains(t, output, "Stop threshold")
		})
	}
}

func TestTextExplanationShowsOnlyApplicableSkipCategories(t *testing.T) {
	for _, filtered := range []bool{false, true} {
		t.Run(fmt.Sprint(filtered), func(t *testing.T) {
			// Given a file destination and SKIP policies for missing/rejected timestamps.
			report := explanationFixture()
			report.RecordErrors.MissingTimestamp, report.RecordErrors.RejectedTimestamp = "SKIP", "SKIP"
			report.Partitions[0].Saved = false
			report.Partitions[0].Window.Lower = nil
			if !filtered {
				report.Partitions[0].Window, report.StartRule = plan.Window{}, "EARLIEST"
			}
			// When rendering, then missing timestamps matter only with a filter;
			// timestamp rejection does not apply to a file destination.
			output := textExplanation(t, report)
			require.Equal(t, filtered, strings.Contains(output, "Skip before writing: missing timestamps"))
			require.NotContains(t, output, "rejected timestamps")
			if filtered {
				require.Contains(t, output, "Fresh start: earliest retained offset; apply the time filter while reading.")
				require.Contains(t, output, "Stop threshold: 1970-01-01T00:00:03Z")
			} else {
				require.Contains(t, output, "Record errors: stop")
				require.Contains(t, output, "start at the earliest retained offset.")
			}
		})
	}
}

func TestTextExplanationSummarizesLargePartitionSets(t *testing.T) {
	for _, windows := range []int{1, 2, 1000} {
		t.Run(fmt.Sprint(windows), func(t *testing.T) {
			// Given a thousand partitions with equal-valued bounds, two groups, or distinct windows.
			report := explanationFixture()
			for id := int32(1); id < 1000; id++ {
				part := report.Partitions[0]
				upper := int64(2000 + id%int32(windows))
				part.ID, part.Window.Upper = id, &upper
				report.Partitions = append(report.Partitions, part)
			}
			for id := range 20 {
				report.Warnings = append(report.Warnings, fmt.Sprintf("Partition %d: expired offset.", id))
			}
			// When rendering, then aggregate bounds, disclose omitted IDs/windows, and retain all JSON rows.
			output := textExplanation(t, report)
			require.Contains(t, output, "Resume: 1000 of 1000")
			require.Contains(t, output, "12 more warnings; see --json")
			require.Equal(t, min(8, windows), strings.Count(output, "  From:"))
			if windows == 1000 {
				require.Contains(t, output, "992 more distinct windows; see --json")
			}
			if windows == 2 {
				require.Contains(t, output, "Partitions: [0 2 4 6 8 10 12 14] (+492 more; see --json)")
				var raw bytes.Buffer
				require.NoError(t, renderExplanation(&raw, report, true))
				var decoded inspect.Report
				require.NoError(t, json.Unmarshal(raw.Bytes(), &decoded))
				require.Len(t, decoded.Partitions, 1000)
				require.EqualValues(t, 999, decoded.Partitions[999].ID)
				require.Len(t, decoded.Warnings, 20)
			}
		})
	}
}
