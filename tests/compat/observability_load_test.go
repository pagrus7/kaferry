//go:build integration && load

package compat

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/pagrus7/kaferry/internal/copy"
	archive "github.com/pagrus7/kaferry/internal/file"
)

func TestLoadObservability(t *testing.T) {
	// Given the same file workload for disabled, healthy and unavailable export.
	const count = 40000
	codec, err := archive.NewCodec(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	source := filepath.Join(dir, "source.cbor")
	header, err := codec.EncodeHeader(archive.Header{Version: 1, ID: "load-input", Group: "fixture", Fingerprint: "fixture"})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := codec.EncodeRecord(core.Record{Key: []byte("key"), Value: make([]byte, 1024), TimestampType: "unknown"})
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(source)
	if err != nil {
		t.Fatal(err)
	}
	buffer := bufio.NewWriter(f)
	if _, err = buffer.Write(header); err != nil {
		t.Fatal(err)
	}
	for range count {
		if _, err = buffer.Write(frame); err != nil {
			t.Fatal(err)
		}
	}
	if err = buffer.Flush(); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	binary := testBinary(t)
	modes := []string{"disabled", "enabled", "receiver unavailable"}
	for round := 0; round < 3; round++ {
		for i := 0; i < len(modes); i++ {
			mode := modes[(i+round)%len(modes)]
			t.Run(fmt.Sprintf("round-%d/%s", round, mode), func(t *testing.T) {
				caseDir := t.TempDir()
				var requests atomic.Int64
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					requests.Add(1)
					_, _ = io.Copy(io.Discard, r.Body)
					if mode == "receiver unavailable" {
						w.WriteHeader(503)
					} else {
						w.Header().Set("Content-Type", "application/x-protobuf")
					}
				}))
				defer server.Close()
				t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", server.URL+"/v1/metrics")
				t.Setenv("OTEL_METRIC_EXPORT_INTERVAL", "100")
				output := filepath.Join(caseDir, mode+".cbor")
				config := filepath.Join(caseDir, mode+".toml")
				settings := fmt.Sprintf("[worker]\ncheckpoint_dir=%q\nprogress_interval='0'\n[metrics]\nenabled=%t\n", caseDir, mode != "disabled")
				if err := os.WriteFile(config, []byte(settings), 0600); err != nil {
					t.Fatal(err)
				}
				statement := fmt.Sprintf("INSERT INTO FILE cbor('%s') SELECT * FROM FILE cbor('%s') OPTIONS(GROUP='%s')", output, source, mode)
				report := newLoadReport(t, "observability")
				report.WorkerRecordCap = 20000
				report.WorkerByteCap = 128 << 20
				ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
				defer cancel()
				// When the CLI copies, with CPU and resident memory sampled independently.
				start := time.Now()
				process := startCLI(t, ctx, binary, statement, "run", "-f", "-", "-c", config)
				report.cli = process
				stopSampling := watchRSS(process)
				err := process.wait()
				elapsed := time.Since(start)
				memory := stopSampling()
				report.ElapsedSeconds = elapsed.Seconds()
				report.ChildCPUSeconds = process.cpu().Seconds()
				report.SampledPeakRSSBytes = memory.peak * 1024
				if err != nil {
					t.Fatal(err, process.logs())
				}
				summary := summaryFrom(t, process.logs())
				report.Records = summary.Confirmed
				report.RecordsPerSecond = float64(report.Records) / elapsed.Seconds()
				report.PeakUncommittedRecords, report.PeakUncommittedBytes = summary.PeakRecords, summary.PeakBytes
				// Then telemetry availability changes neither counts, caps nor archive record bytes.
				if summary.Confirmed != count || summary.Submitted != count || memory.samples == 0 || summary.PeakRecords > 20000 || summary.PeakBytes > 128<<20 {
					t.Fatal(summary, memory)
				}
				data, err := os.ReadFile(output)
				if err != nil {
					t.Fatal(err)
				}
				_, startRecords, err := codec.Header(bytes.NewReader(data), int64(len(data)))
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(data[startRecords:], bytes.Repeat(frame, count)) {
					t.Fatal("record bytes changed")
				}
				if (requests.Load() == 0) != (mode == "disabled") {
					t.Fatal("unexpected exporter activity", mode, requests.Load())
				}
				t.Logf("records/s=%.0f cpu=%.3fs peakRSS=%dKiB requests=%d", report.RecordsPerSecond, report.ChildCPUSeconds, memory.peak, requests.Load())
			})
		}
	}
}
