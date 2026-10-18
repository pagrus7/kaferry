//go:build integration && load

package compat

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestLoadBoundedKafkaCopy(t *testing.T) {
	// Given default caps and a small-cap slow-destination scenario.
	scenarios := []struct {
		name           string
		count, records int
		bytes          int64
		config         string
		limit          int
	}{
		{"default", 16000, 20000, 128 << 20, "", 0},
		{"slow destination small caps", 128, 32, 128 << 10, "[worker]\nrecord_cap=32\nbyte_cap=131072\npartition_record_cap=8\npartition_byte_cap=32768\n", 0},
		{"record limit", 16000, 20000, 128 << 20, "", 2000},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			count, recordCap, byteCap, configuration := scenario.count, scenario.records, scenario.bytes, scenario.config
			if scenario.name == "default" {
				count = loadRecordCount(t, "KAFERRY_LOAD_KAFKA_RECORDS", count, 1_000_000)
			}
			report := newLoadReport(t, "load")
			report.WorkerStopAfterRecords = scenario.limit
			report.SourcePartitions, report.DestinationPartitions = 4, 5
			report.WorkerRecordCap, report.WorkerByteCap = recordCap, byteCap
			// Given seeded topics, a compiled CLI and worker limits.
			a := admin(t)
			src, dst := topic(t, a, 4), topic(t, a, 5)
			p := producer(t)
			reports := make(chan kgo.ProduceResult, count)
			seedTimeout, copyTimeout := 15*time.Second, time.Minute
			if count > scenario.count {
				seedTimeout, copyTimeout = 2*time.Minute, 3*time.Minute
			}
			seedCtx, seedCancel := context.WithTimeout(t.Context(), seedTimeout)
			defer seedCancel()
			stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
			records := newStreamRecords(src, stamp)
			records.publish(t, seedCtx, p, reports, count)
			for range count {
				select {
				case e := <-reports:
					if e.Err != nil {
						t.Fatal("fixture publish failed", e)
					}
				case <-seedCtx.Done():
					t.Fatal("fixture delivery timed out")
				}
			}
			seedCancel()
			binaryPath := testBinary(t)
			statement := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s') INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' OPTIONS(GROUP='%s',CUTOFF=INTERVAL '0' SECOND)", broker(t), dst, src, stamp.Add(time.Second).Format(time.RFC3339Nano), src+"-load")
			args := []string{"run", "--file", "-"}
			if configuration != "" {
				path := filepath.Join(t.TempDir(), "worker.toml")
				require.NoError(t, os.WriteFile(path, []byte(configuration), 0600))
				args = append(args, "--config", path)
				statement = fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s'), CONNECTION slow AS ('bootstrap.servers'='%s','linger.ms'='100') INSERT INTO TOPIC '%s@slow' SELECT * FROM TOPIC '%s' WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' OPTIONS(GROUP='%s',CUTOFF=INTERVAL '0' SECOND)", broker(t), broker(t), dst, src, stamp.Add(time.Second).Format(time.RFC3339Nano), src+"-load")
			}
			want := count
			if scenario.limit > 0 {
				statement = strings.Replace(statement, "OPTIONS(", fmt.Sprintf("OPTIONS(WORKER_STOP_AFTER_RECORDS=%d,", scenario.limit), 1)
				want = min(count, scenario.limit)
			}
			// When executing the bounded copy and sampling its memory.
			ctx, cancel := context.WithTimeout(t.Context(), copyTimeout)
			defer cancel()
			process := launchCLI(t, ctx, binaryPath, args...)
			report.cli = process
			report.MeasurementMethod = "sql_release_to_exit"
			stopSampling := watchRSS(t, process)
			start := time.Now()
			require.NoError(t, process.send(statement), "release SQL input")
			err := process.wait()
			elapsed := time.Since(start)
			memory := stopSampling()
			report.ElapsedSeconds = elapsed.Seconds()
			report.ChildCPUSeconds = process.cpu().Seconds()
			report.SampledPeakRSSBytes = memory.peak * 1024
			report.RSSSamples = memory.samples
			require.NoError(t, err, "load copy: %v %s", err, process.logs())
			summary := summaryFrom(t, process.logs())
			report.Records = summary.Confirmed
			report.RecordsPerSecond = float64(report.Records) / elapsed.Seconds()
			report.PeakUncommittedRecords, report.PeakUncommittedBytes = summary.PeakRecords, summary.PeakBytes

			// Then the CLI resolves every record within its work caps and preserves output.
			require.Equal(t, want, summary.Submitted, "accepted records")
			require.Equal(t, want, summary.Confirmed, "confirmed records")
			require.LessOrEqual(t, summary.PeakRecords, recordCap, "uncommitted records")
			require.LessOrEqual(t, summary.PeakBytes, byteCap, "uncommitted bytes")
			c := consumer(t)
			assign(t, c, dst, 5)
			for range want {
				records.verify(t, read(t, c))
			}
			require.EqualValues(t, want, topicRecordCount(t, a, dst, 5), "destination must not exceed the cap")
			cpu := process.cpu()
			t.Logf("records=%d elapsed=%s records/s=%.0f child_cpu=%s sampled_peak_rss_MiB=%.1f peak_uncommitted_records=%d peak_uncommitted_bytes=%d", report.Records, elapsed, report.RecordsPerSecond, cpu, float64(memory.peak)/1024, summary.PeakRecords, summary.PeakBytes)
		})
	}
}
