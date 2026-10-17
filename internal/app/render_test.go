package app

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/pagrus7/kaferry/internal/inspect"
	"github.com/pagrus7/kaferry/internal/plan"
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
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	// When rendering, then field names, units and outcomes match the contract.
	if err := renderExplanation(&output, explanationFixture(), true); err != nil || !bytes.Equal(output.Bytes(), want) {
		t.Fatalf("JSON differs: %v\n%s", err, output.String())
	}
}

func textExplanation(t *testing.T, report inspect.Report) string {
	t.Helper()
	var output bytes.Buffer
	if err := renderExplanation(&output, report, false); err != nil {
		t.Fatal(err)
	}
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
		if position <= previous {
			t.Fatalf("missing or misplaced %q:\n%s", section, output)
		}
		previous = position
	}
	for _, want := range []string{`input\n\x1b`, `group\n\x1b`, "Resume: 1 of 1 partitions", "1970-01-01T00:00:01Z (inclusive)", "1970-01-01T00:00:02Z (exclusive)", "1970-01-01T00:00:03Z", "clock past it", "saved offset 1 expired", "Record errors: stop", "Output: create a new file"} {
		if !strings.Contains(output, want) {
			t.Fatalf("missing %q:\n%s", want, output)
		}
	}
	for _, absent := range []string{"query-id", "topic-id", "lookup end", "span", "Run permissions", "\x1b"} {
		if strings.Contains(output, absent) {
			t.Fatalf("unexpected %q:\n%s", absent, output)
		}
	}
}

type shortOutput struct{}

func (shortOutput) Write([]byte) (int, error) { return 0, nil }

func TestExplanationOutputFailures(t *testing.T) {
	// Given a closed or short-writing output stream.
	for _, output := range []io.Writer{closedOutput{}, shortOutput{}} {
		for _, asJSON := range []bool{false, true} {
			// When writing a report, then output loss is reported to the CLI.
			err := renderExplanation(output, explanationFixture(), asJSON)
			if err == nil || !errors.Is(err, io.ErrShortWrite) && err.Error() != "output closed" {
				t.Fatal(err)
			}
		}
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
		if !strings.Contains(output, want) {
			t.Fatalf("missing %q:\n%s", want, output)
		}
	}
}

func TestTextExplanationFollowingRoutingAndSkipPolicies(t *testing.T) {
	// Given a fresh following copy with destination routing and one explicit SKIP policy.
	report := explanationFixture()
	report.Partitions[0].Window, report.Partitions[0].Saved = plan.Window{}, false
	report.StartRule = "LATEST"
	report.Destination = inspect.Endpoint{Kind: "TOPIC", Name: "output", Connection: "backup", ClusterID: "other-cluster", PartitionCount: 3, TimestampType: "LogAppendTime"}
	report.RecordErrors.RejectedTimestamp = "SKIP"
	// When rendering, then state the latest start, continuous follow and the dropped-record policy.
	output := textExplanation(t, report)
	for _, want := range []string{"current read-committed end", "Filter: none", "no end time; follow", "original key bytes; destination partitioner", "LogAppendTime", "Skip before writing: rejected timestamps (records dropped)", "Delivery and unclassified errors: stop"} {
		if !strings.Contains(output, want) {
			t.Fatalf("missing %q:\n%s", want, output)
		}
	}
	if strings.Contains(output, "Stop threshold") || strings.Contains(output, "Attention") {
		t.Fatal(output)
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
				if !strings.Contains(output, want) {
					t.Fatalf("missing %q:\n%s", want, output)
				}
			}
			if saved {
				for _, want := range []string{"saved file checkpoint", "saved.json", "resume existing file", "Acknowledged output: 100 bytes", "reads every existing output frame"} {
					if !strings.Contains(output, want) {
						t.Fatalf("missing %q:\n%s", want, output)
					}
				}
			} else if !strings.Contains(output, "fresh group") || !strings.Contains(output, "no saved checkpoint") {
				t.Fatal(output)
			}
			if strings.Contains(output, "Stop threshold") {
				t.Fatal(output)
			}
		})
	}
}

func TestTextExplanationShowsOnlyApplicableSkipCategories(t *testing.T) {
	for _, filtered := range []bool{false, true} {
		t.Run(fmt.Sprint(filtered), func(t *testing.T) {
			// Given a file destination and SKIP policies for missing/rejected timestamps.
			report := explanationFixture()
			report.RecordErrors.MissingTimestamp, report.RecordErrors.RejectedTimestamp = "SKIP", "SKIP"
			if !filtered {
				report.Partitions[0].Window, report.StartRule = plan.Window{}, "EARLIEST"
			}
			// When rendering, then missing timestamps matter only with a filter;
			// timestamp rejection does not apply to a file destination.
			output := textExplanation(t, report)
			if strings.Contains(output, "Skip before writing: missing timestamps") != filtered || strings.Contains(output, "rejected timestamps") {
				t.Fatal(output)
			}
			if !filtered && !strings.Contains(output, "Record errors: stop") {
				t.Fatal(output)
			}
		})
	}
}

func TestTextExplanationSummarizesLargePartitionSets(t *testing.T) {
	for _, distinct := range []bool{false, true} {
		t.Run(fmt.Sprint(distinct), func(t *testing.T) {
			// Given a thousand partitions with either equal-valued bounds or distinct windows.
			report := explanationFixture()
			for id := int32(1); id < 1000; id++ {
				part := report.Partitions[0]
				upper := int64(2000)
				if distinct {
					upper += int64(id)
				}
				part.ID, part.Window.Upper = id, &upper
				report.Partitions = append(report.Partitions, part)
			}
			for id := range 20 {
				report.Warnings = append(report.Warnings, fmt.Sprintf("Partition %d: expired offset.", id))
			}
			// When rendering, then aggregate equal bounds and explicitly point to omitted details.
			output := textExplanation(t, report)
			if !strings.Contains(output, "Resume: 1000 of 1000") || !strings.Contains(output, "12 more warnings; see --json") {
				t.Fatal(output)
			}
			if distinct {
				if strings.Count(output, "  From:") != 8 || !strings.Contains(output, "992 more distinct windows; see --json") {
					t.Fatal(output)
				}
			} else if strings.Count(output, "  From:") != 1 {
				t.Fatal(output)
			}
		})
	}
}
