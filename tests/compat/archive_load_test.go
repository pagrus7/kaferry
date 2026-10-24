//go:build integration && load

package compat

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestLoadArchiveCopy(t *testing.T) {
	count := loadRecordCount(t, "KAFERRY_LOAD_ARCHIVE_RECORDS", 16000, 200000)
	for _, direction := range []string{"capture", "replay", "file"} {
		t.Run(direction, func(t *testing.T) {
			// Given the same logical records and real CLI for each archive direction.
			root := t.TempDir()
			source, output := filepath.Join(root, "input"), filepath.Join(root, "output")
			cfg := filepath.Join(root, "worker.toml")
			require.NoError(t, os.WriteFile(cfg, []byte(fmt.Sprintf("[worker]\ncheckpoint_dir=%q\nprogress_interval='0'\n", root)), 0600))
			inputSQL, outputSQL := archiveQuery(source), archiveQuery(output)
			sourceOptions := "GROUP='archive-load'"
			copyOptions := ""
			prefix := ""
			var destination string
			var original *core.Provenance
			if direction != "file" {
				a := admin(t)
				name := topic(t, a, 4)
				prefix = fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s') ", broker(t))
				if direction == "capture" {
					original = brokerSourceIdentity(t, a, name)
					// A fresh disposable broker may still be creating its group coordinator.
					warmCtx, warmCancel := context.WithTimeout(t.Context(), 30*time.Second)
					for {
						_, err := a.FetchOffsets(warmCtx, "archive-load")
						if err == nil || errors.Is(err, kerr.GroupIDNotFound) {
							break
						}
						require.NoError(t, warmCtx.Err(), "fixture coordinator readiness: %v", err)
						time.Sleep(100 * time.Millisecond)
					}
					warmCancel()
					inputSQL = fmt.Sprintf("TOPIC '%s'", name)
					producer := producer(t)
					seedCtx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
					defer cancel()
					pending := make(chan kgo.ProduceResult, count)
					for id := range count {
						r := archiveLoadRecord(id)
						headers := make([]kgo.RecordHeader, len(r.Headers))
						for i, h := range r.Headers {
							headers[i] = kgo.RecordHeader{Key: h.Name, Value: h.Value}
						}
						producer.Produce(seedCtx, &kgo.Record{Topic: name, Partition: r.Source.Partition, Key: r.Key, Value: r.Value, Timestamp: time.UnixMilli(*r.TimestampMS), Headers: headers}, deliveryReporter(seedCtx, pending))
					}
					for range count {
						select {
						case result := <-pending:
							require.NoError(t, result.Err)
						case <-seedCtx.Done():
							t.Fatal(seedCtx.Err())
						}
					}
					sourceOptions += ",START=EARLIEST"
					copyOptions = fmt.Sprintf(" OPTIONS(WORKER_STOP_AFTER_RECORDS=%d)", count)
				} else {
					outputSQL = fmt.Sprintf("TOPIC '%s' WITH(PARTITIONING=KEY)", name)
					destination = name
				}
			}
			if direction != "capture" {
				seedArchive(t, source, count)
			}
			statement := fmt.Sprintf("%sINSERT INTO %s SELECT * FROM %s WITH (%s)%s", prefix, outputSQL, inputSQL, sourceOptions, copyOptions)
			report := measureArchiveCLI(t, statement, cfg, count)

			// Then verify every output after the timed region, so measurements cannot hide data loss.
			if destination != "" {
				consumer := consumer(t)
				assign(t, consumer, destination, 4)
				seen := make([]bool, count)
				for range count {
					record := read(t, consumer)
					require.GreaterOrEqual(t, len(record.Value), 4)
					id := int(binary.BigEndian.Uint32(record.Value))
					require.Less(t, id, count)
					require.False(t, seen[id])
					seen[id] = true
					expected := archiveLoadRecord(id)
					require.Equal(t, expected.Key, record.Key)
					require.Equal(t, expected.Value, record.Value)
					require.EqualValues(t, *expected.TimestampMS, record.Timestamp.UnixMilli())
				}
			} else {
				verifyArchive(t, output, count, original)
			}
			t.Logf("format=avro direction=%s records=%d elapsed=%s records/s=%.0f cpu=%s peak_RSS_MiB=%.1f", direction, count, time.Duration(report.ElapsedSeconds*float64(time.Second)), report.RecordsPerSecond, time.Duration(report.ChildCPUSeconds*float64(time.Second)), float64(report.SampledPeakRSSBytes)/(1<<20))
		})
	}
}

func measureArchiveCLI(t *testing.T, statement, cfg string, count int) *loadReport {
	t.Helper()
	report := newLoadReport(t, "archive")
	report.ArchiveFormat = "avro"
	report.ArchivePayload = os.Getenv("KAFERRY_ARCHIVE_PAYLOAD")
	if report.ArchivePayload == "" {
		report.ArchivePayload = "compressible"
	}
	report.WorkerRecordCap = 20000
	report.WorkerByteCap = 128 << 20
	// When executing SQL through durable completion, sample the actual CLI child.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	process := launchCLI(t, ctx, testBinary(t), "run", "-f", "-", "-c", cfg)
	measureCLI(t, report, process, statement)
	require.Equal(t, count, report.Records)
	require.LessOrEqual(t, report.PeakUncommittedRecords, 20000)
	return report
}
