package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/inspect"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type filePreviewFixture struct{ query, config, directory, destination string }

func TestExplainShowsPerInvocationLimit(t *testing.T) {
	// Given a limited file copy with no output or checkpoint.
	fixture := filePreview(t)
	query := strings.Replace(fixture.query, "OPTIONS(", "OPTIONS(WORKER_STOP_AFTER_RECORDS=5,", 1)
	for _, asJSON := range []bool{false, true} {
		args := []string{"explain", "-e", query, "-c", fixture.config}
		if asJSON {
			args = append(args, "--json")
		}
		var stdout, stderr bytes.Buffer
		// When previewing the statement.
		code := Execute(t.Context(), args, nil, &stdout, &stderr)
		// Then both formats show a cap, without estimating the number of source records.
		require.Zero(t, code, stderr.String())
		if asJSON {
			var report inspect.Report
			require.NoError(t, json.Unmarshal(stdout.Bytes(), &report))
			require.EqualValues(t, 5, report.WorkerStopAfterRecords)
		} else {
			require.Contains(t, stdout.String(), "Worker stop: after 5 records accepted for writing. Resuming gets a fresh allowance.")
		}
		_, err := os.Stat(fixture.destination)
		require.ErrorIs(t, err, os.ErrNotExist, "preview must not create the output")
	}
}

func filePreview(t *testing.T) filePreviewFixture {
	t.Helper()
	dir := t.TempDir()
	source, destination, config := filepath.Join(dir, "source.avro"), filepath.Join(dir, "out.avro"), filepath.Join(dir, "config.toml")
	require.NoError(t, os.Mkdir(source, 0700))
	require.NoError(t, os.WriteFile(config, []byte(fmt.Sprintf("[worker]\ncheckpoint_dir = %q\n", dir)), 0600))
	query := fmt.Sprintf("INSERT INTO FILE avro('%s') SELECT * FROM FILE avro('%s') OPTIONS(GROUP='preview')", destination, source)
	return filePreviewFixture{query: query, config: config, directory: dir, destination: destination}
}

func TestExplainCLIInputFormsLeaveFilesUnchanged(t *testing.T) {
	for _, form := range []string{"inline", "file", "stdin"} {
		t.Run(form, func(t *testing.T) {
			// Given a valid file query with no output, checkpoint or locks.
			fixture := filePreview(t)
			args := []string{"explain", "-c", fixture.config, "--json"}
			var input io.Reader
			switch form {
			case "inline":
				args = append(args, "-e", fixture.query)
			case "stdin":
				args, input = append(args, "-f", "-"), strings.NewReader(fixture.query)
			case "file":
				path := filepath.Join(fixture.directory, "copy.sql")
				require.NoError(t, os.WriteFile(path, []byte(fixture.query), 0600))
				args = append(args, "-f", path)
			}
			before, err := os.ReadDir(fixture.directory)
			require.NoError(t, err)
			var stdout, stderr bytes.Buffer
			// When the real CLI prepares and inspects the selected input form.
			code := Execute(t.Context(), args, input, &stdout, &stderr)
			// Then JSON reports the record position and no filesystem objects were created.
			var report inspect.Report
			require.NoError(t, json.Unmarshal(stdout.Bytes(), &report))
			require.Zero(t, code)
			require.Zero(t, stderr.Len())
			require.EqualValues(t, 1, report.Version)
			require.NotNil(t, report.Source.File.PositionRecords)
			require.False(t, report.Destination.File.Exists)
			after, err := os.ReadDir(fixture.directory)
			require.NoError(t, err)
			require.Equal(t, after, before)
		})
	}
}

func TestExplainCLIReusesFileProgressAndReportsOutputErrors(t *testing.T) {
	// Given an already finished copy and its durable output/checkpoint.
	fixture := filePreview(t)
	var stderr bytes.Buffer
	require.Zero(t, Execute(t.Context(), []string{"run", "-e", fixture.query, "-c", fixture.config}, nil, io.Discard, &stderr))
	before := fileTree(t, fixture.directory)
	var stdout bytes.Buffer
	stderr.Reset()
	// When inspecting a rerun.
	code := Execute(t.Context(), []string{"explain", "-e", fixture.query, "-c", fixture.config, "--json"}, nil, &stdout, &stderr)
	// Then saved progress and acknowledged output are preserved.
	var report inspect.Report
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &report))
	require.Zero(t, code)
	require.True(t, *report.Source.File.CheckpointExists)
	require.True(t, report.Destination.File.Exists)
	require.Equal(t, before, fileTree(t, fixture.directory))
	stderr.Reset()
	require.EqualValues(t, 1, Execute(t.Context(), []string{"explain", "-e", fixture.query, "-c", fixture.config}, nil, closedOutput{}, &stderr))
	require.Contains(t, stderr.String(), "output closed")
}

func TestExplainCLIReportsInputAndIdentityErrorsWithoutPartialOutput(t *testing.T) {
	for _, problem := range []string{"SQL", "missing source", "invalid header", "self-copy", "empty resolved window"} {
		t.Run(problem, func(t *testing.T) {
			// Given invalid input or an archive that cannot be inspected.
			fixture := filePreview(t)
			source := filepath.Join(fixture.directory, "source.avro")
			want := 2
			switch problem {
			case "SQL":
				fixture.query = "SELECT 'secret_SENTINEL_415'"
			case "missing source":
				require.NoError(t, os.Remove(source))
				want = 1
			case "invalid header":
				require.NoError(t, os.WriteFile(filepath.Join(source, "invalid.avro"), []byte("invalid"), 0600))
				want = 1
			case "self-copy":
				fixture.query = strings.Replace(fixture.query, fixture.destination, source, 1)
			case "empty resolved window":
				fixture.query = strings.Replace(fixture.query, " OPTIONS(", " WHERE RECORD_TIMESTAMP >= NOW() AND RECORD_TIMESTAMP < TIMESTAMP '2000-01-01T00:00:00Z' OPTIONS(", 1)
			}
			var stdout, stderr bytes.Buffer
			// When explaining, then errors preserve their exit class and never print a partial report.
			code := Execute(t.Context(), []string{"explain", "-e", fixture.query, "-c", fixture.config, "--json"}, nil, &stdout, &stderr)
			require.Equal(t, want, code)
			require.Zero(t, stdout.Len())
			require.NotZero(t, stderr.Len())
			require.NotContains(t, stderr.String(), "secret_SENTINEL")
		})
	}
}

func TestExplainUsesOnlyReadOnlyKafkaRequests(t *testing.T) {
	for _, output := range []string{"TOPIC", "FILE", "SELF-COPY"} {
		t.Run(output, func(t *testing.T) {
			// Given real protocol clients and two source partitions.
			cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, "input"), kfake.SeedTopics(3, "output"))
			defer cluster.Close()
			producer, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
			require.NoError(t, err)
			defer producer.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
			for id := range int32(2) {
				require.NoError(t, producer.ProduceSync(ctx, &kgo.Record{Topic: "input", Partition: id, Timestamp: stamp, Value: []byte("record")}).FirstErr())
			}
			var writes atomic.Int64
			for _, key := range []kmsg.Key{kmsg.Produce, kmsg.Fetch, kmsg.JoinGroup, kmsg.OffsetCommit, kmsg.ConsumerGroupHeartbeat} {
				cluster.ControlKey(int16(key), func(kmsg.Request) (kmsg.Response, error, bool) {
					cluster.KeepControl()
					writes.Add(1)
					return nil, nil, false
				})
			}
			// A denied optional config request must not add a permission requirement.
			cluster.ControlKey(int16(kmsg.DescribeConfigs), func(request kmsg.Request) (kmsg.Response, error, bool) {
				cluster.KeepControl()
				response := request.ResponseKind().(*kmsg.DescribeConfigsResponse)
				response.Resources = []kmsg.DescribeConfigsResponseResource{{ErrorCode: 29, ResourceType: kmsg.ConfigResourceTypeTopic, ResourceName: "output"}}
				return response, nil, true
			})
			directory := t.TempDir()
			destination := "TOPIC 'output'"
			if output == "FILE" {
				destination = fmt.Sprintf("FILE avro('%s')", filepath.Join(directory, "archive.avro"))
			} else if output == "SELF-COPY" {
				destination = "TOPIC 'input'"
			}
			query := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s','client.id'='secret_SENTINEL_415') INSERT INTO %s SELECT * FROM TOPIC 'input' WHERE RECORD_TIMESTAMP >= TIMESTAMP '%s' AND RECORD_TIMESTAMP < TIMESTAMP '%s' OPTIONS(GROUP='preview',CUTOFF=INTERVAL '0' SECOND,ON_ERROR=SKIP)", strings.Join(cluster.ListenAddrs(), ","), destination, stamp.Format(time.RFC3339Nano), stamp.Add(time.Minute).Format(time.RFC3339Nano))
			var stdout, stderr bytes.Buffer
			// When the full Fx-composed command explains the copy.
			code := Execute(ctx, []string{"explain", "-e", query, "--json"}, nil, &stdout, &stderr)
			// Then self-copy is rejected before any consume, produce, join or commit;
			// successful previews report positions without leaking properties.
			if output == "SELF-COPY" {
				require.EqualValues(t, 2, code)
				require.Zero(t, stdout.Len())
				require.Zero(t, writes.Load())
				require.Contains(t, stderr.String(), "source and destination are the same Kafka topic")
				require.NotContains(t, stderr.String(), "secret_SENTINEL")
				return
			}
			var report inspect.Report
			require.NoError(t, json.Unmarshal(stdout.Bytes(), &report))
			require.Zero(t, code)
			require.Zero(t, writes.Load())
			require.Len(t, report.Partitions, 2)
			require.Equal(t, "WINDOW", report.StartRule)
			require.Equal(t, (inspect.ErrorPolicies{MissingTimestamp: "SKIP", RejectedTimestamp: "SKIP", OtherRecord: "SKIP"}), report.RecordErrors)
			require.Zero(t, report.Partitions[0].Start)
			require.EqualValues(t, 1, report.Partitions[0].LogEnd)
			require.NotContains(t, stdout.String()+stderr.String(), "secret_SENTINEL")
			if output == "TOPIC" {
				require.Empty(t, report.Destination.TimestampType)
				require.Contains(t, strings.Join(report.Warnings, " "), "unreadable")
			}
			entries, err := os.ReadDir(directory)
			require.NoError(t, err)
			require.Len(t, entries, 0)
		})
	}
}

func TestShutdownDeadlineCoversExplanationOutput(t *testing.T) {
	if os.Getenv("KAFERRY_SHUTDOWN_HELPER") == "explain" {
		fixture := filePreview(t)
		config := fmt.Sprintf("[worker]\ncheckpoint_dir = %q\nshutdown_timeout = '40ms'\n", fixture.directory)
		require.NoError(t, os.WriteFile(fixture.config, []byte(config), 0600))
		budget := newShutdownBudget(context.Background(), time.Second, forceShutdown)
		_ = executeWithShutdown(context.Background(), []string{"explain", "-e", fixture.query, "-c", fixture.config}, nil, blockingErrorOutput{}, os.Stderr, budget)
		t.Fatal("blocked explanation returned")
	}
	// Given blocked stdout after a successful file-only preview, when no signal arrives,
	// then printing still uses the process shutdown deadline and reports its stage.
	t.Setenv("TMPDIR", t.TempDir())
	runShutdownHelper(t, "explain", "printing explanation")
}
