//go:build integration && load

package compat

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/require"
)

func TestLoadBoundedKafkaCopy(t *testing.T) {
	// Given default caps and a small-cap slow-destination scenario.
	scenarios := []struct {
		name           string
		count, records int
		bytes          int64
		config         string
	}{
		{"default", 16000, 20000, 128 << 20, ""},
		{"slow destination small caps", 128, 32, 128 << 10, "[worker]\nrecord_cap=32\nbyte_cap=131072\npartition_record_cap=8\npartition_byte_cap=32768\n"},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			count, recordCap, byteCap, configuration := scenario.count, scenario.records, scenario.bytes, scenario.config
			report := newLoadReport(t, "load")
			report.SourcePartitions, report.DestinationPartitions = 4, 5
			report.WorkerRecordCap, report.WorkerByteCap = recordCap, byteCap
			// Given seeded topics, a compiled CLI and worker limits.
			a := admin(t)
			src, dst := topic(t, a, 4), topic(t, a, 5)
			p := producer(t, nil)
			reports := make(chan k.Event, count)
			stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
			records := newStreamRecords(src, stamp)
			records.publish(t, p, reports, count)
			for range count {
				select {
				case e := <-reports:
					if r, ok := e.(*k.Message); !ok || r.TopicPartition.Error != nil {
						t.Fatal("fixture publish failed", e)
					}
				case <-time.After(15 * time.Second):
					t.Fatal("fixture delivery timed out")
				}
			}
			binaryPath := testBinary(t)
			statement := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s') INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' OPTIONS(GROUP='%s',CUTOFF=INTERVAL '0' SECOND)", broker(t), dst, src, stamp.Add(time.Second).Format(time.RFC3339Nano), src+"-load")
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			args := []string{"run", "--file", "-"}
			if configuration != "" {
				path := filepath.Join(t.TempDir(), "worker.toml")
				require.NoError(t, os.WriteFile(path, []byte(configuration), 0600))
				args = append(args, "--config", path)
				statement = fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s'), CONNECTION slow AS ('bootstrap.servers'='%s','linger.ms'='100') INSERT INTO TOPIC '%s@slow' SELECT * FROM TOPIC '%s' WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' OPTIONS(GROUP='%s',CUTOFF=INTERVAL '0' SECOND)", broker(t), broker(t), dst, src, stamp.Add(time.Second).Format(time.RFC3339Nano), src+"-load")
			}
			// When executing the bounded copy and sampling its memory.
			start := time.Now()
			process := startCLI(t, ctx, binaryPath, statement, args...)
			report.cli = process
			stopSampling := watchRSS(process)
			err := process.wait()
			elapsed := time.Since(start)
			memory := stopSampling()
			report.ElapsedSeconds = elapsed.Seconds()
			report.ChildCPUSeconds = process.cpu().Seconds()
			report.SampledPeakRSSBytes = memory.peak * 1024
			require.NoError(t, err, "load copy: %v %s", err, process.logs())
			require.NotZero(t, memory.samples, "no RSS samples; failures=%d", memory.failures)
			summary := summaryFrom(t, process.logs())
			report.Records = summary.Confirmed
			report.RecordsPerSecond = float64(report.Records) / elapsed.Seconds()
			report.PeakUncommittedRecords, report.PeakUncommittedBytes = summary.PeakRecords, summary.PeakBytes

			// Then the CLI resolves every record within its work caps and preserves output.
			if summary.Submitted != count || summary.Confirmed != count || summary.PeakRecords > recordCap || summary.PeakBytes > byteCap {
				t.Fatalf("load counts/caps wrong: %+v", summary)
			}
			c := consumer(t, dst+"-reader")
			var parts []k.TopicPartition
			for i := range 5 {
				parts = append(parts, k.TopicPartition{Topic: &dst, Partition: int32(i), Offset: 0})
			}
			require.NoError(t, c.Assign(parts))
			for range count {
				records.verify(t, read(t, c))
			}
			cpu := process.cpu()
			t.Logf("records=%d elapsed=%s records/s=%.0f child_cpu=%s sampled_peak_rss_MiB=%.1f peak_uncommitted_records=%d peak_uncommitted_bytes=%d", count, elapsed, float64(count)/elapsed.Seconds(), cpu, float64(memory.peak)/1024, summary.PeakRecords, summary.PeakBytes)
		})
	}
}
