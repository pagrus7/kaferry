//go:build integration && load

package compat

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

type kafkaLoadScenario struct {
	name           string
	count, records int
	bytes          int64
	config         string
	limit          int
	profile        *workloadProfile
}

func TestLoadBoundedKafkaCopy(t *testing.T) {
	// Given default caps, small caps and the measured payload/key/compression profiles.
	scenarios := []kafkaLoadScenario{
		{name: "default", count: 16000, records: 20000, bytes: 128 << 20},
		{name: "preserved partitions", count: 16000, records: 20000, bytes: 128 << 20},
		{name: "slow destination small caps", count: 128, records: 32, bytes: 128 << 10,
			config: "[worker]\nrecord_cap=32\nbyte_cap=131072\npartition_record_cap=8\npartition_byte_cap=32768\n"},
		{name: "record limit", count: 16000, records: 20000, bytes: 128 << 20, limit: 2000},
	}
	for _, profile := range loadWorkloadProfiles() {
		count := 3200
		if profile.TopicMaxMessageBytes != 0 {
			count = 256
		}
		scenarios = append(scenarios, kafkaLoadScenario{name: profile.Name, count: count,
			records: 20000, bytes: 128 << 20, profile: &profile})
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			count, recordCap, byteCap, configuration := scenario.count, scenario.records, scenario.bytes, scenario.config
			if scenario.name == "default" || scenario.name == "preserved partitions" {
				count = loadRecordCount(t, "KAFERRY_LOAD_KAFKA_RECORDS", count, 160000)
			}
			report := newLoadReport(t, "load")
			report.WorkerStopAfterRecords = scenario.limit
			report.SourcePartitions, report.DestinationPartitions = 4, 5
			report.WorkerRecordCap, report.WorkerByteCap = recordCap, byteCap
			if scenario.profile != nil {
				report.Workload = *scenario.profile
			}
			// Given seeded topics, a compiled CLI and worker limits.
			a := admin(t)
			src, dst := topic(t, a, 4), topic(t, a, 5)
			var compression sourceCompressionCounts
			var options []kgo.Opt
			if profile := scenario.profile; profile != nil {
				options = append(options, kgo.MaxBufferedRecords(profile.SourceBufferRecords), kgo.ProducerBatchCompression(profile.compression()), kgo.WithHooks(&compression))
				alterTopicConfig(t, a, src, map[string]string{"compression.type": "producer"})
				for _, name := range []string{src, dst} {
					if profile.TopicMaxMessageBytes != 0 {
						alterTopicConfig(t, a, name, map[string]string{"max.message.bytes": fmt.Sprint(profile.TopicMaxMessageBytes)})
					}
					if profile.CompactionLagMS != 0 {
						// Preserve every version and tombstone through this bounded reconciliation.
						alterTopicConfig(t, a, name, map[string]string{"cleanup.policy": "compact",
							"min.compaction.lag.ms": fmt.Sprint(profile.CompactionLagMS),
							"delete.retention.ms":   fmt.Sprint(profile.CompactionLagMS)})
					}
				}
			}
			p := producer(t, options...)
			reports := make(chan kgo.ProduceResult, count)
			seedTimeout, copyTimeout := 15*time.Second, time.Minute
			if count > scenario.count {
				seedTimeout, copyTimeout = 2*time.Minute, 3*time.Minute
			}
			seedCtx, seedCancel := context.WithTimeout(t.Context(), seedTimeout)
			defer seedCancel()
			stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
			if scenario.profile != nil && scenario.profile.CompactionLagMS != 0 {
				// Cleaner age follows record timestamps; old fixture timestamps would bypass the lag.
				stamp = time.Now().Truncate(time.Millisecond)
			}
			records := newFiniteStreamRecords(src, stamp, count)
			records.profile = scenario.profile
			if scenario.name != "preserved partitions" {
				records.keyRouting = kgo.StickyKeyPartitioner(nil).ForTopic(dst)
				records.destinationPartitions = 5
			}
			records.publish(t, seedCtx, p, reports, count)
			for range count {
				select {
				case e := <-reports:
					if e.Err != nil {
						t.Fatal("fixture publish failed", e)
					}
					records.acknowledged++
				case <-seedCtx.Done():
					t.Fatal("fixture delivery timed out")
				}
			}
			require.NoError(t, p.Flush(seedCtx))
			if scenario.profile != nil {
				report.SourceCompressionRecords = compression.snapshot()
				require.Equal(t, map[string]int64{scenario.profile.SourceCompression: int64(count)}, report.SourceCompressionRecords,
					"source batches must use the reported compression codec")
			}
			seedCancel()
			binaryPath := testBinary(t)
			statement := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s') INSERT INTO TOPIC '%s' WITH (PARTITIONING = KEY) SELECT * FROM TOPIC '%s' WITH (GROUP='%s', MAX_STALENESS=INTERVAL '0' SECOND) WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' ", broker(t), dst, src, src+"-load", stamp.Add(time.Second).Format(time.RFC3339Nano))
			args := []string{"run", "--file", "-"}
			if configuration != "" {
				path := filepath.Join(t.TempDir(), "worker.toml")
				require.NoError(t, os.WriteFile(path, []byte(configuration), 0600))
				args = append(args, "--config", path)
				statement = fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s'), CONNECTION slow AS ('bootstrap.servers'='%s','linger.ms'='100') INSERT INTO TOPIC '%s@slow' WITH (PARTITIONING = KEY) SELECT * FROM TOPIC '%s' WITH (GROUP='%s', MAX_STALENESS=INTERVAL '0' SECOND) WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' ", broker(t), broker(t), dst, src, src+"-load", stamp.Add(time.Second).Format(time.RFC3339Nano))
			}
			if scenario.name == "preserved partitions" {
				statement = strings.Replace(statement, "WITH (PARTITIONING = KEY)", "", 1)
			}
			want := count
			if scenario.limit > 0 {
				statement = statement + fmt.Sprintf(" OPTIONS(WORKER_STOP_AFTER_RECORDS=%d)", scenario.limit)
				want = min(count, scenario.limit)
			}
			// When executing the bounded copy and sampling its memory.
			ctx, cancel := context.WithTimeout(t.Context(), copyTimeout)
			defer cancel()
			process := launchCLI(t, ctx, binaryPath, args...)
			summary := measureCLI(t, report, process, statement)

			// Then the CLI resolves every record within its work caps and preserves output.
			require.Equal(t, want, summary.Submitted, "accepted records")
			require.Equal(t, want, summary.Confirmed, "confirmed records")
			require.LessOrEqual(t, summary.PeakRecords, recordCap, "uncommitted records")
			require.LessOrEqual(t, summary.PeakBytes, byteCap, "uncommitted bytes")
			c := consumer(t)
			assign(t, c, dst, 5)
			for range want {
				record := read(t, c)
				records.verify(t, record)
				if scenario.name == "preserved partitions" {
					require.EqualValues(t, binary.BigEndian.Uint32(record.Value)%4, record.Partition, "preserved source partition")
				}
			}
			require.Equal(t, want, records.unique, "unique destination records")
			require.Equal(t, want, records.verified, "verified destination records")
			require.Zero(t, records.duplicates, "duplicate destination records")
			require.Zero(t, records.corrupted, "corrupted destination records")
			require.Zero(t, records.unexpected, "unexpected destination records")
			require.Zero(t, records.reordered, "reordered destination records")
			delivery := records.delivery(want == count)
			report.Delivery = &delivery
			if want == count {
				require.True(t, delivery.ReconciliationComplete)
				require.NotNil(t, delivery.Integrity.Matched)
				require.True(t, *delivery.Integrity.Matched, "submitted and received record fingerprints differ")
			}
			require.EqualValues(t, want, topicRecordCount(t, a, dst, 5), "destination must not exceed the cap")
			cpu := process.cpu()
			t.Logf("records=%d elapsed=%s records/s=%.0f child_cpu=%s sampled_peak_rss_MiB=%.1f peak_uncommitted_records=%d peak_uncommitted_bytes=%d", report.Records, time.Duration(report.ElapsedSeconds*float64(time.Second)), report.RecordsPerSecond, cpu, float64(report.SampledPeakRSSBytes)/(1<<20), summary.PeakRecords, summary.PeakBytes)
		})
	}
}
