package app

import (
	"bytes"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/pagrus7/kaferry/internal/inspect"
	"github.com/pagrus7/kaferry/internal/plan"
)

func explanationFixture() inspect.Report {
	lo, hi, end, span := int64(1000), int64(2000), int64(10), int64(6)
	return inspect.Report{Version: 1, Fingerprint: "query-id", Group: "backfill", CutoffMS: 1000,
		Source:         inspect.Endpoint{Kind: "TOPIC", Name: "input", Connection: "prod", ClusterID: "cluster-id", TopicID: "topic-id", PartitionCount: 1},
		Destination:    inspect.Endpoint{Kind: "FILE", Name: "out.cbor", File: &inspect.FileSummary{}},
		Partitions:     []inspect.Partition{{ID: 0, Start: 4, LookupEnd: &end, Span: &span, LogEnd: 12, Saved: true, Window: plan.Window{Lower: &lo, Upper: &hi}, WindowOrigin: "literal"}},
		RunPermissions: []string{"source topic READ", "source group READ", "destination file and directory write"}, Warnings: []string{"Spans count offsets, not records."}}
}

func TestExplanationJSONContract(t *testing.T) {
	// Given the public version-one JSON report.
	want, err := os.ReadFile("testdata/explain.json")
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	// When rendering, then field names, units, nulls and outcomes match the contract.
	if err := renderExplanation(&output, explanationFixture(), true); err != nil || !bytes.Equal(output.Bytes(), want) {
		t.Fatalf("JSON differs: %v\n%s", err, output.String())
	}
}

func TestTextExplanationShowsBoundsAndEscapesEndpointNames(t *testing.T) {
	// Given a bounded report and names containing terminal controls.
	report := explanationFixture()
	report.Source.Name, report.Group = "input\n\x1b", "group\n\x1b"
	var output bytes.Buffer
	// When rendering, then include usable bounds and lookup labels with escaped names.
	if err := renderExplanation(&output, report, false); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"lookup end", "span", "1970-01-01T00:00:01Z", "Run permissions (not verified)", "Warning:", `input\n\x1b`} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("missing %q:\n%s", want, output.String())
		}
	}
	if strings.ContainsRune(output.String(), '\x1b') {
		t.Fatal("terminal control was not escaped")
	}
	// Given an unbounded query, then stopping-lookup columns are absent.
	report.Partitions[0].Window.Upper, report.Warnings = nil, nil
	output.Reset()
	if err := renderExplanation(&output, report, false); err != nil || strings.Contains(output.String(), "lookup end") || strings.Contains(output.String(), "span") || !strings.Contains(output.String(), "unbounded") {
		t.Fatal(err, output.String())
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
	// Given partitions with saved bounds, an adopted window and a fresh NOW preview.
	report := explanationFixture()
	report.Partitions[0].WindowOrigin = "saved"
	adopted, fresh := report.Partitions[0], report.Partitions[0]
	adopted.ID, adopted.WindowOrigin = 1, "adopted"
	hi := int64(6000)
	fresh.ID, fresh.WindowOrigin, fresh.Window.Upper = 2, "resolved", &hi
	report.Partitions = append(report.Partitions, adopted, fresh)
	var output bytes.Buffer
	// When rendering, then describe each window's source and retain differing bounds.
	if err := renderExplanation(&output, report, false); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"saved progress", "from saved partition", "NOW() preview", "1970-01-01T00:00:02Z", "1970-01-01T00:00:06Z"} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("missing %q: %s", want, output.String())
		}
	}
}
