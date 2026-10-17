//go:build integration && load

package compat

import (
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// Reports are optional build artifacts, never part of the archive or CLI API.
type loadReport struct {
	Test                   string  `json:"test"`
	Kind                   string  `json:"kind"`
	Passed                 bool    `json:"passed"`
	Skipped                bool    `json:"skipped"`
	Platform               string  `json:"platform"`
	Client                 string  `json:"client"`
	BinarySHA256           string  `json:"binary_sha256,omitempty"`
	Records                int     `json:"records"`
	ElapsedSeconds         float64 `json:"elapsed_seconds"`
	RecordsPerSecond       float64 `json:"records_per_second"`
	ChildCPUSeconds        float64 `json:"child_cpu_seconds"`
	SampledPeakRSSBytes    int64   `json:"sampled_peak_rss_bytes"`
	WarmMedianRSSBytes     int64   `json:"warm_median_rss_bytes,omitempty"`
	FinalMedianRSSBytes    int64   `json:"final_median_rss_bytes,omitempty"`
	WorkerRecordCap        int     `json:"worker_record_cap"`
	WorkerByteCap          int64   `json:"worker_byte_cap"`
	PeakUncommittedRecords int     `json:"peak_uncommitted_records"`
	PeakUncommittedBytes   int64   `json:"peak_uncommitted_bytes"`
	SourcePartitions       int     `json:"source_partitions"`
	DestinationPartitions  int     `json:"destination_partitions"`
	MaxSampledCommitLag    int     `json:"max_sampled_commit_lag,omitempty"`
	ShutdownSeconds        float64 `json:"shutdown_seconds,omitempty"`
	Bursts                 int     `json:"bursts,omitempty"`
	DestinationStalls      int     `json:"destination_stalls,omitempty"`
	cli                    *cliProcess
}

func TestLoadReportIdentifiesExecutedBinary(t *testing.T) {
	// Given a CLI binary and an unusable fallback build path.
	binary := testBinary(t)
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	// When the process finishes and its test cleanup writes a report.
	t.Run("collect", func(t *testing.T) {
		t.Setenv("KAFERRY_REPORT_DIR", directory)
		t.Setenv("KAFERRY_TEST_BINARY", filepath.Join(directory, "unused-binary"))
		report := newLoadReport(t, "identity")
		report.cli = startCLI(t, t.Context(), binary, "", "--version")
		if err := report.cli.wait(); err != nil {
			t.Fatal(err)
		}
	})
	// Then the report identifies the binary that ran, without rebuilding it.
	files, err := filepath.Glob(filepath.Join(directory, "*.metrics.json"))
	if err != nil || len(files) != 1 {
		t.Fatal("missing report", files, err)
	}
	encoded, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	var report loadReport
	if err := json.Unmarshal(encoded, &report); err != nil {
		t.Fatal(err)
	}
	if !report.Passed || report.Skipped || report.BinarySHA256 != fmt.Sprintf("%x", sha256.Sum256(data)) || report.Client == "" {
		t.Fatal("report does not identify the tested binary", report)
	}
}

func newLoadReport(t *testing.T, kind string) *loadReport {
	t.Helper()
	r := &loadReport{Test: t.Name(), Kind: kind, Platform: runtime.GOOS + "/" + runtime.GOARCH,
		SourcePartitions: 4, DestinationPartitions: 5}
	if directory := os.Getenv("KAFERRY_REPORT_DIR"); directory != "" {
		t.Cleanup(func() {
			if r.cli != nil {
				binaryPath := r.cli.command.Path
				info, err := buildinfo.ReadFile(binaryPath)
				if err != nil {
					t.Error("read tested binary build information:", err)
				} else {
					for _, dep := range info.Deps {
						if dep.Path == "github.com/twmb/franz-go" || dep.Path == "github.com/confluentinc/confluent-kafka-go/v2" {
							r.Client = dep.Path + " " + dep.Version
						}
					}
				}
				data, err := os.ReadFile(binaryPath)
				if err != nil {
					t.Error("hash tested binary:", err)
				} else {
					r.BinarySHA256 = fmt.Sprintf("%x", sha256.Sum256(data))
				}
			}
			prefix := filepath.Join(directory, fmt.Sprintf("%s-%x", kind, sha256.Sum256([]byte(t.Name()))))
			if err := os.MkdirAll(directory, 0755); err != nil {
				t.Error("create build report directory:", err)
				return
			}
			if r.cli != nil {
				if err := os.WriteFile(prefix+".cli.log", []byte(r.cli.logs()), 0644); err != nil {
					t.Error("write CLI build log:", err)
				}
			}
			r.Passed, r.Skipped = !t.Failed() && !t.Skipped(), t.Skipped()
			data, err := json.MarshalIndent(r, "", "  ")
			if err == nil {
				err = os.WriteFile(prefix+".metrics.json", append(data, '\n'), 0644)
			}
			if err != nil {
				t.Error("write build report:", err)
			}
		})
	}
	return r
}
