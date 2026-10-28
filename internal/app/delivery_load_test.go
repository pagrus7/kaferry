package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/archive/avro"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestStreamingBoundedLoadAllEndpoints(t *testing.T) {
	if os.Getenv("KAFERRY_LAB_LOAD") != "1" {
		t.Skip("set KAFERRY_LAB_LOAD=1 for bounded subprocess load qualification")
	}
	require.NotEmpty(t, os.Getenv("KAFERRY_LAB_SOURCE"), "load qualification requires real disposable brokers")
	require.NotEmpty(t, os.Getenv("KAFERRY_LAB_DESTINATION"))
	binary := streamBinary(t)
	const partitions, perPartition, valueBytes = 8, 2048, 1024
	for _, src := range []string{"TOPIC", "ARCHIVE"} {
		for _, dst := range []string{"TOPIC", "ARCHIVE"} {
			t.Run(src+"_to_"+dst, func(t *testing.T) {
				// Given 16 MiB of authored payload across eight source partitions.
				source, destination := clusters(t, partitions)
				input, target := "TOPIC 'input@src'", "TOPIC 'output@dst'"
				inputRoot, outputRoot := t.TempDir(), ""
				codec, err := avro.New(64 << 20)
				require.NoError(t, err)
				producer := client(t, source)
				expected := map[string][32]byte{}
				stamp := time.Now().Add(-time.Minute).Truncate(time.Millisecond)
				for part := range partitions {
					identity := avro.Identity{Version: 1, WriterID: strings.Repeat("a", 32), FileID: fmt.Sprintf("%032x", part+1), Source: avro.Source{Cluster: "load-fixture", Topic: "input", TopicID: avro.TopicID{1}, Partition: int32(part)}}
					var file *os.File
					var writer *avro.Writer
					if src == "ARCHIVE" {
						file, err = os.Create(filepath.Join(inputRoot, identity.WriterID+"."+identity.FileID+".avro"))
						require.NoError(t, err)
						writer, err = codec.NewWriter(file, identity)
						require.NoError(t, err)
					}
					records := make([]*kgo.Record, 0, perPartition)
					for offset := range perPartition {
						key := fmt.Sprintf("%d:%d", part, offset)
						value := bytes.Repeat([]byte{byte(part), byte(offset), byte(offset >> 8), 255}, valueBytes/4)
						expected[key] = sha256.Sum256(value)
						if writer != nil {
							ms := stamp.UnixMilli()
							require.NoError(t, writer.Append(avro.Record{Source: identity.Source, Offset: int64(offset), Key: []byte(key), Value: value, TimestampMS: &ms, TimestampKind: "create_time"}))
						} else {
							records = append(records, &kgo.Record{Topic: "input", Partition: int32(part), Key: []byte(key), Value: value, Timestamp: stamp})
						}
					}
					if writer != nil {
						require.NoError(t, writer.Flush())
						require.NoError(t, file.Close())
					} else {
						require.NoError(t, producer.ProduceSync(testContext(t), records...).FirstErr())
					}
				}
				if src == "ARCHIVE" {
					input = "ARCHIVE '" + inputRoot + "'"
				}
				if dst == "ARCHIVE" {
					outputRoot = t.TempDir()
					target = "ARCHIVE '" + outputRoot + "' WITH(ROTATE_AFTER_SIZE=4194304)"
				}
				state := t.TempDir()
				configPath := filepath.Join(t.TempDir(), "load.toml")
				configuration := fmt.Sprintf("[connections.src]\n'bootstrap.servers'=%q\n[connections.dst]\n'bootstrap.servers'=%q\n[worker]\ncheckpoint_dir=%q\nrecord_cap=256\npartition_record_cap=32\nbyte_cap=8388608\npartition_byte_cap=1048576\narchive_capture_open_files=8\nshutdown_timeout='10s'\n", strings.Join(source, ","), strings.Join(destination, ","), state)
				require.NoError(t, os.WriteFile(configPath, []byte(configuration), 0600))
				statement := fmt.Sprintf("INSERT INTO %s SELECT * FROM %s WITH(GROUP='load-%d') WHERE RECORD_OFFSET < LATEST", target, input, time.Now().UnixNano())
				var peakRSS int64
				started := time.Now()
				// When separate CLI processes stop at a cap, complete, and rerun the saved job.
				for index := range 3 {
					text := statement
					if index == 0 {
						text += " OPTIONS(WORKER_STOP_AFTER_RECORDS=4096)"
					}
					ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
					cmd := exec.CommandContext(ctx, binary, "--progress-topic", "progress", "--", "run", "--config", configPath, "--file", "-")
					cmd.Stdin = strings.NewReader(text)
					logs, err := cmd.CombinedOutput()
					cancel()
					require.NoError(t, err, "%s", logs)
					usage, ok := cmd.ProcessState.SysUsage().(*syscall.Rusage)
					require.True(t, ok, "load measurements require Unix process resource usage")
					rss := int64(usage.Maxrss)
					if runtime.GOOS != "darwin" {
						rss *= 1024
					}
					peakRSS = max(peakRSS, rss)
					if index == 0 {
						if dst == "TOPIC" {
							require.Len(t, committedRecords(t, destination, partitions), 4096)
						} else {
							records, private, err := scanArchiveRecords(outputRoot)
							require.NoError(t, err)
							require.Zero(t, private)
							require.Len(t, records, 4096)
						}
					}
				}
				elapsed := time.Since(started)
				// Then every key has its full payload exactly once after both resumptions.
				seen := map[string]int{}
				check := func(key, value []byte, timestamp int64, part int32) {
					t.Helper()
					want, exists := expected[string(key)]
					require.True(t, exists, "unexpected key %q", key)
					require.Equal(t, want, sha256.Sum256(value), "payload for %q", key)
					require.Equal(t, stamp.UnixMilli(), timestamp)
					require.True(t, strings.HasPrefix(string(key), fmt.Sprintf("%d:", part)), "partition for %q", key)
					seen[string(key)]++
				}
				if dst == "TOPIC" {
					for _, record := range committedRecords(t, destination, partitions) {
						check(record.Key, record.Value, record.Timestamp.UnixMilli(), record.Partition)
					}
				} else {
					records, private, err := scanArchiveRecords(outputRoot)
					require.NoError(t, err)
					require.Zero(t, private)
					for _, record := range records {
						require.NotNil(t, record.TimestampMS)
						check(record.Key, record.Value, *record.TimestampMS, record.Source.Partition)
					}
				}
				require.Len(t, seen, len(expected))
				for key, count := range seen {
					require.Equal(t, 1, count, "occurrence %s", key)
				}
				t.Logf("load records=%d partitions=%d value_bytes=%d processes=3 elapsed_seconds=%.3f records_per_second=%.1f peak_process_rss_bytes=%d", len(expected), partitions, valueBytes, elapsed.Seconds(), float64(len(expected))/elapsed.Seconds(), peakRSS)
			})
		}
	}
}
