//go:build integration && load

package compat

import (
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
	"github.com/stretchr/testify/require"
)

func TestLoadObservability(t *testing.T) {
	// Given the same file workload for disabled, healthy and unavailable export.
	count := loadRecordCount(t, "KAFERRY_LOAD_FILE_RECORDS", 40000, 200000)
	dir := t.TempDir()
	source := filepath.Join(dir, "source.avro")
	writer := archiveFixtureWriter(t, source)
	record := core.Record{Key: []byte("key"), Value: make([]byte, 1024), TimestampType: "unknown"}
	for id := range count {
		require.NoError(t, writer.Append(fixtureRecord(record, int64(id))))
	}
	require.NoError(t, writer.Finish())
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
				output := filepath.Join(caseDir, mode+".avro")
				config := filepath.Join(caseDir, mode+".toml")
				settings := fmt.Sprintf("[worker]\ncheckpoint_dir=%q\nprogress_interval='0'\n[metrics]\nenabled=%t\n", caseDir, mode != "disabled")
				require.NoError(t, os.WriteFile(config, []byte(settings), 0600))
				statement := fmt.Sprintf("INSERT INTO FILE avro('%s') SELECT * FROM FILE avro('%s') WITH (GROUP='%s') ", output, source, mode)
				report := newLoadReport(t, "observability")
				report.ArchiveFormat = "avro"
				report.WorkerRecordCap = 20000
				report.WorkerByteCap = 128 << 20
				timeout := time.Minute
				if count > 40000 {
					timeout = 3 * time.Minute
				}
				ctx, cancel := context.WithTimeout(t.Context(), timeout)
				defer cancel()
				// When the CLI copies, with CPU and resident memory sampled independently.
				process := launchCLI(t, ctx, binary, "run", "-f", "-", "-c", config)
				summary := measureCLI(t, report, process, statement)
				// Then telemetry availability changes neither counts, caps nor archive record bytes.
				require.Equal(t, count, summary.Confirmed)
				require.Equal(t, count, summary.Submitted)
				require.LessOrEqual(t, summary.PeakRecords, 20000)
				require.LessOrEqual(t, summary.PeakBytes, int64(128<<20))
				verified := 0
				scanArchive(t, output, func(got core.Record) {
					require.Equal(t, record.Key, got.Key)
					require.Equal(t, record.Value, got.Value)
					require.Nil(t, got.TimestampMS)
					require.Equal(t, "unknown", got.TimestampType)
					require.Empty(t, got.Headers)
					require.EqualValues(t, verified, got.Source.Offset)
					verified++
				})
				require.Equal(t, count, verified, "telemetry does not change archive records")
				require.Equal(t, (mode == "disabled"), (requests.Load() == 0), "unexpected exporter activity")
				t.Logf("records/s=%.0f cpu=%.3fs peakRSS=%dKiB requests=%d", report.RecordsPerSecond, report.ChildCPUSeconds, report.SampledPeakRSSBytes/1024, requests.Load())
			})
		}
	}
}
