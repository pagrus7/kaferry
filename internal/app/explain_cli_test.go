package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/file"
	"github.com/pagrus7/kaferry/internal/inspect"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type filePreviewFixture struct{ query, config, directory, destination string }

func filePreview(t *testing.T) filePreviewFixture {
	t.Helper()
	dir := t.TempDir()
	source, destination, config := filepath.Join(dir, "source.cbor"), filepath.Join(dir, "out.cbor"), filepath.Join(dir, "config.toml")
	codec, err := file.NewCodec(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	header, err := codec.EncodeHeader(file.Header{Version: 1, ID: "source", Group: "fixture", Fingerprint: "fixture"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(source, header, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(config, []byte(fmt.Sprintf("[worker]\ncheckpoint_dir = %q\n", dir)), 0600); err != nil {
		t.Fatal(err)
	}
	query := fmt.Sprintf("INSERT INTO FILE cbor('%s') SELECT * FROM FILE cbor('%s') OPTIONS(GROUP='preview')", destination, source)
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
				if err := os.WriteFile(path, []byte(fixture.query), 0600); err != nil {
					t.Fatal(err)
				}
				args = append(args, "-f", path)
			}
			before, err := os.ReadDir(fixture.directory)
			if err != nil {
				t.Fatal(err)
			}
			var stdout, stderr bytes.Buffer
			// When the real CLI prepares and inspects the selected input form.
			code := Execute(t.Context(), args, input, &stdout, &stderr)
			// Then JSON reports the byte start and no filesystem objects were created.
			var report inspect.Report
			if err := json.Unmarshal(stdout.Bytes(), &report); err != nil || code != 0 || stderr.Len() != 0 || report.Version != 1 || report.Source.File.StartBytes == nil || report.Destination.File.Exists {
				t.Fatalf("exit=%d report=%+v err=%v stderr=%s", code, report, err, stderr.String())
			}
			after, err := os.ReadDir(fixture.directory)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("inspection created files", before, after, err)
			}
		})
	}
}

func TestExplainCLIReusesFileProgressAndReportsOutputErrors(t *testing.T) {
	// Given an already finished copy and its durable output/checkpoint.
	fixture := filePreview(t)
	var stderr bytes.Buffer
	if code := Execute(t.Context(), []string{"run", "-e", fixture.query, "-c", fixture.config}, nil, io.Discard, &stderr); code != 0 {
		t.Fatal(code, stderr.String())
	}
	before, err := os.ReadFile(fixture.destination)
	if err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	stderr.Reset()
	// When inspecting a rerun.
	code := Execute(t.Context(), []string{"explain", "-e", fixture.query, "-c", fixture.config, "--json"}, nil, &stdout, &stderr)
	// Then saved progress and acknowledged output are preserved.
	var report inspect.Report
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil || code != 0 || !*report.Source.File.CheckpointExists || report.Destination.File.AcknowledgedExtent == nil || *report.Destination.File.AcknowledgedExtent != int64(len(before)) {
		t.Fatal(code, report, err, stderr.String())
	}
	after, err := os.ReadFile(fixture.destination)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("preview changed output", err)
	}
	stderr.Reset()
	if code := Execute(t.Context(), []string{"explain", "-e", fixture.query, "-c", fixture.config}, nil, closedOutput{}, &stderr); code != 1 || !strings.Contains(stderr.String(), "output closed") {
		t.Fatal("lost output error", code, stderr.String())
	}
}

func TestExplainCLIReportsInputAndIdentityErrorsWithoutPartialOutput(t *testing.T) {
	for _, problem := range []string{"SQL", "missing source", "invalid header", "self-copy", "empty resolved window"} {
		t.Run(problem, func(t *testing.T) {
			// Given invalid input or an archive that cannot be inspected.
			fixture := filePreview(t)
			source := filepath.Join(fixture.directory, "source.cbor")
			want := 2
			switch problem {
			case "SQL":
				fixture.query = "SELECT 'secret_SENTINEL_415'"
			case "missing source":
				if err := os.Remove(source); err != nil {
					t.Fatal(err)
				}
				want = 1
			case "invalid header":
				if err := os.WriteFile(source, []byte("invalid"), 0600); err != nil {
					t.Fatal(err)
				}
			case "self-copy":
				fixture.query = strings.Replace(fixture.query, fixture.destination, source, 1)
			case "empty resolved window":
				fixture.query = strings.Replace(fixture.query, " OPTIONS(", " WHERE RECORD_TIMESTAMP >= NOW() AND RECORD_TIMESTAMP < TIMESTAMP '2000-01-01T00:00:00Z' OPTIONS(", 1)
			}
			var stdout, stderr bytes.Buffer
			// When explaining, then errors preserve their exit class and never print a partial report.
			code := Execute(t.Context(), []string{"explain", "-e", fixture.query, "-c", fixture.config, "--json"}, nil, &stdout, &stderr)
			if code != want || stdout.Len() != 0 || stderr.Len() == 0 || strings.Contains(stderr.String(), "secret_SENTINEL") {
				t.Fatal(code, stdout.String(), stderr.String())
			}
		})
	}
}

func TestExplainUsesOnlyReadOnlyKafkaRequests(t *testing.T) {
	for _, output := range []string{"TOPIC", "FILE"} {
		t.Run(output, func(t *testing.T) {
			// Given real protocol clients and two source partitions.
			cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, "input"), kfake.SeedTopics(3, "output"))
			defer cluster.Close()
			producer, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
			if err != nil {
				t.Fatal(err)
			}
			defer producer.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
			for id := range int32(2) {
				if err := producer.ProduceSync(ctx, &kgo.Record{Topic: "input", Partition: id, Timestamp: stamp, Value: []byte("record")}).FirstErr(); err != nil {
					t.Fatal(err)
				}
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
				destination = fmt.Sprintf("FILE cbor('%s')", filepath.Join(directory, "archive.cbor"))
			}
			query := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s','client.id'='secret_SENTINEL_415') INSERT INTO %s SELECT * FROM TOPIC 'input' WHERE RECORD_TIMESTAMP >= TIMESTAMP '%s' AND RECORD_TIMESTAMP < TIMESTAMP '%s' OPTIONS(GROUP='preview',CUTOFF=INTERVAL '0' SECOND,ON_ERROR=SKIP)", strings.Join(cluster.ListenAddrs(), ","), destination, stamp.Format(time.RFC3339Nano), stamp.Add(time.Minute).Format(time.RFC3339Nano))
			var stdout, stderr bytes.Buffer
			// When the full Fx-composed command explains the copy.
			code := Execute(ctx, []string{"explain", "-e", query, "--json"}, nil, &stdout, &stderr)
			// Then no consume, produce, join or commit occurs and no properties leak.
			var report inspect.Report
			if err := json.Unmarshal(stdout.Bytes(), &report); err != nil || code != 0 || writes.Load() != 0 || len(report.Partitions) != 2 || report.StartRule != "WINDOW" || report.RecordErrors != (inspect.ErrorPolicies{MissingTimestamp: "SKIP", RejectedTimestamp: "SKIP", OtherRecord: "SKIP"}) || report.Partitions[0].Start != 0 || report.Partitions[0].LogEnd != 1 || strings.Contains(stdout.String()+stderr.String(), "secret_SENTINEL") {
				t.Fatalf("exit=%d writes=%d report=%+v err=%v stderr=%s", code, writes.Load(), report, err, stderr.String())
			}
			if output == "TOPIC" && (report.Destination.TimestampType != "" || !strings.Contains(strings.Join(report.Warnings, " "), "unreadable")) {
				t.Fatal("optional config denial was not reported", report)
			}
			entries, err := os.ReadDir(directory)
			if err != nil || len(entries) != 0 {
				t.Fatal("inspection created an archive or lock", entries, err)
			}
		})
	}
}

func TestShutdownDeadlineCoversExplanationOutput(t *testing.T) {
	if os.Getenv("KAFERRY_SHUTDOWN_HELPER") == "explain" {
		fixture := filePreview(t)
		config := fmt.Sprintf("[worker]\ncheckpoint_dir = %q\nshutdown_timeout = '40ms'\n", fixture.directory)
		if err := os.WriteFile(fixture.config, []byte(config), 0600); err != nil {
			t.Fatal(err)
		}
		budget := newShutdownBudget(context.Background(), time.Second, forceShutdown)
		_ = executeWithShutdown(context.Background(), []string{"explain", "-e", fixture.query, "-c", fixture.config}, nil, blockingErrorOutput{}, os.Stderr, budget)
		t.Fatal("blocked explanation returned")
	}
	// Given blocked stdout after a successful file-only preview, when no signal arrives,
	// then printing still uses the process shutdown deadline and reports its stage.
	t.Setenv("TMPDIR", t.TempDir())
	runShutdownHelper(t, "explain", "printing explanation")
}
