//go:build integration && load

package compat

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestLoadArchiveCopy(t *testing.T) {
	format := os.Getenv("KAFERRY_ARCHIVE_FORMAT")
	if format == "" {
		format = "avro"
	}
	require.Contains(t, []string{"cbor", "avro"}, format)
	count := loadRecordCount(t, "KAFERRY_LOAD_ARCHIVE_RECORDS", 16000, 200000)
	for _, direction := range []string{"capture", "replay", "file"} {
		t.Run(direction, func(t *testing.T) {
			// Given the same logical records and real CLI for each archive direction.
			root := t.TempDir()
			source, output := filepath.Join(root, "input"), filepath.Join(root, "output")
			cfg := filepath.Join(root, "worker.toml")
			require.NoError(t, os.WriteFile(cfg, []byte(fmt.Sprintf("[worker]\ncheckpoint_dir=%q\nprogress_interval='0'\n", root)), 0600))
			inputSQL, outputSQL := archiveQuery(format, source), archiveQuery(format, output)
			options := "GROUP='archive-load'"
			prefix := ""
			var destination string
			if direction != "file" {
				a := admin(t)
				name := topic(t, a, 4)
				prefix = fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s') ", broker(t))
				if direction == "capture" {
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
					options += fmt.Sprintf(",WORKER_STOP_AFTER_RECORDS=%d,START=EARLIEST", count)
				} else {
					outputSQL = fmt.Sprintf("TOPIC '%s'", name)
					destination = name
				}
			}
			if direction != "capture" {
				seedArchive(t, source, format, count)
			}
			statement := fmt.Sprintf("%sINSERT INTO %s SELECT * FROM %s OPTIONS(%s)", prefix, outputSQL, inputSQL, options)
			report := measureArchiveCLI(t, statement, cfg, format, count)

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
				verifyArchive(t, output, format, count)
			}
			t.Logf("format=%s direction=%s records=%d elapsed=%s records/s=%.0f cpu=%s peak_RSS_MiB=%.1f", format, direction, count, time.Duration(report.ElapsedSeconds*float64(time.Second)), report.RecordsPerSecond, time.Duration(report.ChildCPUSeconds*float64(time.Second)), float64(report.SampledPeakRSSBytes)/(1<<20))
		})
	}
}

func measureArchiveCLI(t *testing.T, statement, cfg, format string, count int) *loadReport {
	t.Helper()
	report := newLoadReport(t, "archive")
	report.ArchiveFormat = format
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
	report.cli = process
	report.MeasurementMethod = "sql_release_to_exit"
	stopSampling := watchRSS(t, process)
	start := time.Now()
	require.NoError(t, process.send(statement))
	err := process.wait()
	elapsed := time.Since(start)
	memory := stopSampling()
	report.ElapsedSeconds = elapsed.Seconds()
	report.ChildCPUSeconds = process.cpu().Seconds()
	report.SampledPeakRSSBytes = memory.peak * 1024
	report.RSSSamples = memory.samples
	require.NoError(t, err, process.logs())
	for _, line := range strings.Split(process.logs(), "\n") {
		var event struct {
			Time    time.Time `json:"time"`
			Message string    `json:"msg"`
		}
		if json.Unmarshal([]byte(line), &event) == nil && event.Message == "partition assigned" {
			report.ReadySeconds = event.Time.Sub(start).Seconds()
			break
		}
	}
	summary := summaryFrom(t, process.logs())
	report.Records = summary.Confirmed
	report.RecordsPerSecond = float64(report.Records) / elapsed.Seconds()
	report.PeakUncommittedRecords = summary.PeakRecords
	report.PeakUncommittedBytes = summary.PeakBytes
	require.Equal(t, count, report.Records)
	require.LessOrEqual(t, summary.PeakRecords, 20000)
	return report
}
