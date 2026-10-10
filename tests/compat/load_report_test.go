//go:build integration && load

package compat

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Reports are optional build artifacts, never part of the archive or CLI API.
type loadReport struct {
	Test                   string  `json:"test"`
	Kind                   string  `json:"kind"`
	Passed                 bool    `json:"passed"`
	Skipped                bool    `json:"skipped"`
	Platform               string  `json:"platform"`
	Librdkafka             string  `json:"librdkafka"`
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

func newLoadReport(t *testing.T, kind string) *loadReport {
	t.Helper()
	_, native := k.LibraryVersion()
	r := &loadReport{Test: t.Name(), Kind: kind, Platform: runtime.GOOS + "/" + runtime.GOARCH,
		Librdkafka: native, SourcePartitions: 4, DestinationPartitions: 5}
	if directory := os.Getenv("KAFERRY_REPORT_DIR"); directory != "" {
		t.Cleanup(func() {
			r.Passed, r.Skipped = !t.Failed() && !t.Skipped(), t.Skipped()
			prefix := filepath.Join(directory, fmt.Sprintf("%s-%x", kind, sha256.Sum256([]byte(t.Name()))))
			if err := os.MkdirAll(directory, 0755); err != nil {
				t.Error("create build report directory:", err)
				return
			}
			data, err := json.MarshalIndent(r, "", "  ")
			if err == nil {
				err = os.WriteFile(prefix+".metrics.json", append(data, '\n'), 0644)
			}
			if err != nil {
				t.Error("write build report:", err)
			}
			if r.cli != nil {
				if err := os.WriteFile(prefix+".cli.log", []byte(r.cli.logs()), 0644); err != nil {
					t.Error("write CLI build log:", err)
				}
			}
		})
	}
	return r
}
