package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"
	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/protobuf/proto"
)

func streamBinary(t *testing.T) string {
	t.Helper()
	path := os.Getenv("KAFERRY_TEST_BINARY")
	if path == "" {
		t.Skip("set KAFERRY_TEST_BINARY to the built kaferry CLI")
	}
	return path
}
func TestCLIRejectsUnsupportedLocalRecovery(t *testing.T) {
	for _, route := range []struct{ name, source, destination, group string }{
		{"one-off capture", "TOPIC 'input'", "ARCHIVE '%s'", ""},
		{"Kafka destination", "TOPIC 'input'", "TOPIC 'output'", " WITH (GROUP='recovery')"},
		{"archive source", "ARCHIVE '%s'", "ARCHIVE '%s'", " WITH (GROUP='recovery')"},
	} {
		t.Run(route.name, func(t *testing.T) {
			// Given a valid copy query whose route cannot publish local capture recovery.
			directory := t.TempDir()
			configPath := filepath.Join(directory, "config.toml")
			require.NoError(t, os.WriteFile(configPath, []byte("[connections.default]\n'bootstrap.servers'='127.0.0.1:1'\n"), 0600))
			input, target := filepath.Join(directory, "input"), filepath.Join(directory, "output")
			statement := "INSERT INTO " + strings.ReplaceAll(route.destination, "%s", target) + " SELECT * FROM " + strings.ReplaceAll(route.source, "%s", input) + route.group
			var stdout, stderr bytes.Buffer
			// When dispatching --recover-local, then it rejects the route before opening endpoints.
			code := Execute(testContext(t), []string{"run", "--recover-local", "--config", configPath, "--sql", statement}, nil, &stdout, &stderr)
			require.Equal(t, 2, code, "%s", stderr.String())
			require.Contains(t, stderr.String(), "local recovery requires Kafka capture and the original named group")
			require.Empty(t, stdout.String())
			require.NoDirExists(t, target)
		})
	}
}

func TestStreamingCLIAllEndpoints(t *testing.T) {
	binary := streamBinary(t)
	for _, src := range []string{"TOPIC", "ARCHIVE"} {
		for _, dst := range []string{"TOPIC", "ARCHIVE"} {
			t.Run(src+"_to_"+dst, func(t *testing.T) {
				// Given the real CLI with TOML profiles and a production shutdown budget.
				source, destination := clusters(t, 2)
				input, target := "TOPIC 'input@src'", "TOPIC 'output@dst'"
				expected := map[string]int{}
				if src == "TOPIC" {
					seed(t, source, 2, 4)
					for p := range 2 {
						for o := range 4 {
							expected[fmt.Sprintf("%d:%d", p, o)] = 1
						}
					}
				} else {
					root := t.TempDir()
					expected = archiveInput(t, root)
					input = "ARCHIVE '" + root + "'"
				}
				outputRoot := ""
				if dst == "ARCHIVE" {
					outputRoot = t.TempDir()
					target = "ARCHIVE '" + outputRoot + "' WITH (ROTATE_AFTER_SIZE=1000)"
				}
				directory := t.TempDir()
				configPath := filepath.Join(t.TempDir(), "config.toml")
				configuration := fmt.Sprintf("[connections.src]\n'bootstrap.servers'=%q\n[connections.dst]\n'bootstrap.servers'=%q\n[worker]\nprogress_topic='progress'\ncheckpoint_dir=%q\nshutdown_timeout='3s'\nrecord_cap=3\npartition_record_cap=2\n", strings.Join(source, ","), strings.Join(destination, ","), directory)
				require.NoError(t, os.WriteFile(configPath, []byte(configuration), 0600))
				statement := fmt.Sprintf("INSERT INTO %s SELECT * FROM %s WITH (GROUP='cli-matrix-%d') WHERE RECORD_OFFSET < LATEST", target, input, time.Now().UnixNano())
				invoke := func(command, sql string) []byte {
					t.Helper()
					ctx, cancel := context.WithTimeout(testContext(t), 20*time.Second)
					defer cancel()
					cmd := exec.CommandContext(ctx, binary, command, "--config", configPath, "--file", "-")
					cmd.Stdin = strings.NewReader(sql)
					logs, err := cmd.CombinedOutput()
					require.NoError(t, err, "%s", logs)
					return logs
				}
				// When separate processes stop at a worker cap and resume, then output stays exact.
				invoke("run", statement+" OPTIONS(WORKER_STOP_AFTER_RECORDS=2)")
				invoke("run", statement)
				var got map[string]int
				if dst == "TOPIC" {
					got = output(t, destination, 2)
				} else {
					got = archiveValues(t, outputRoot)
				}
				require.Equal(t, expected, got)
				status := invoke("status", statement)
				require.Contains(t, string(status), "stopped")
				explanation := invoke("explain", statement)
				require.Contains(t, string(explanation), "cli-matrix")
			})
		}
	}
}
func TestCLIOneOffLogsGroupAndRemovesReplayBinding(t *testing.T) {
	// Given a file-only ONE_OFF query and an existing checkpoint directory.
	binary := streamBinary(t)
	input, target, checkpoints := t.TempDir(), t.TempDir(), t.TempDir()
	expected := archiveInput(t, input)
	config := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, os.WriteFile(config, []byte(fmt.Sprintf("[worker]\nprogress_topic='progress'\ncheckpoint_dir=%q\n", checkpoints)), 0600))
	statement := fmt.Sprintf("INSERT INTO ARCHIVE '%s' SELECT * FROM ARCHIVE '%s'", target, input)
	// When it finishes, then the logged group is usable for diagnostics and its replay state is removed.
	cmd := exec.CommandContext(testContext(t), binary, "run", "--config", config, "--file", "-")
	cmd.Stdin = strings.NewReader(statement)
	logs, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", logs)
	found := false
	for _, line := range strings.Split(string(logs), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) == nil && record["msg"] == "one-off group generated" {
			require.NotEmpty(t, record["group"])
			found = true
		}
	}
	require.True(t, found, "%s", logs)
	entries, err := os.ReadDir(checkpoints)
	require.NoError(t, err)
	for _, entry := range entries {
		require.True(t, strings.HasSuffix(entry.Name(), ".lock"), entry.Name())
	}
	require.Equal(t, expected, archiveValues(t, target))
}

func TestCLIDeadlineAfterLostCommitReplyResumesExactly(t *testing.T) {
	// Given a broker that commits output but withholds the EndTxn response.
	binary := streamBinary(t)
	source, destination := kfake.MustCluster(kfake.NumBrokers(1)), (*kfake.Cluster)(nil)
	gate := &commitReplyGate{apiKey: uint16(kmsg.EndTxn)}
	destination = kfake.MustCluster(kfake.NumBrokers(1), kfake.ListenFn(gate.listen))
	defer source.Close()
	defer destination.Close()
	topic(t, source.ListenAddrs(), "input", 1, false)
	topic(t, destination.ListenAddrs(), "output", 1, false)
	topic(t, destination.ListenAddrs(), "progress", 1, true)
	seed(t, source.ListenAddrs(), 1, 6)
	p := query(t, source.ListenAddrs(), destination.ListenAddrs(), "WHERE RECORD_OFFSET < LATEST", "")
	cfg := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, os.WriteFile(cfg, []byte(fmt.Sprintf("[connections.src]\n'bootstrap.servers'=%q\n[connections.dst]\n'bootstrap.servers'=%q\n[worker]\nprogress_topic='progress'\nrecord_cap=1\npartition_record_cap=1\nshutdown_timeout='500ms'\n", strings.Join(source.ListenAddrs(), ","), strings.Join(destination.ListenAddrs(), ","))), 0600))
	statement := fmt.Sprintf("INSERT INTO TOPIC 'output@dst' SELECT * FROM TOPIC 'input@src' WITH(GROUP='%s') WHERE RECORD_OFFSET < LATEST", p.Group)
	command := exec.CommandContext(testContext(t), binary, "run", "--config", cfg, "--file", "-")
	command.Stdin = strings.NewReader(statement)
	metadata, err := kadm.NewClient(client(t, destination.ListenAddrs())).Metadata(testContext(t), "output")
	require.NoError(t, err)
	outputID := metadata.Topics["output"].ID
	var fault atomic.Bool
	fault.Store(true)
	destination.ControlKey(int16(kmsg.Produce), func(request kmsg.Request) (kmsg.Response, error, bool) {
		destination.KeepControl()
		for _, topic := range request.(*kmsg.ProduceRequest).Topics {
			if fault.Load() && (topic.Topic == "output" || topic.TopicID == outputID) {
				gate.after.Store(1)
			}
		}
		return nil, nil, false
	})
	var logs bytes.Buffer
	command.Stdout = &logs
	command.Stderr = &logs
	require.NoError(t, command.Start())
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	waited := false
	t.Cleanup(func() {
		if !waited {
			_ = command.Process.Kill()
			<-done
		}
		if t.Failed() {
			t.Logf("process output: %s", logs.String())
		}
	})

	require.Eventually(t, func() bool { return gate.dropped.Load() > 0 }, 10*time.Second, 10*time.Millisecond)
	require.Equal(t, map[string]int{"0:0": 1}, output(t, destination.ListenAddrs(), 1))
	// When SIGTERM starts the whole-process budget, then the blocked process exits on deadline.
	started := time.Now()
	require.NoError(t, command.Process.Signal(syscall.SIGTERM))
	select {
	case err := <-done:
		waited = true
		require.Error(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown budget did not terminate blocked commit")
	}
	require.Less(t, time.Since(started), 3*time.Second)
	require.NotEmpty(t, logs.String())
	// When a replacement resumes after the lost reply, it reads the committed target cursor.
	fault.Store(false)
	gate.after.Store(0)
	removeDeadMember(t, source.ListenAddrs(), p.Group)
	require.NoError(t, Run(testContext(t), p, config.Defaults(""), Options{ProgressTopic: "progress"}))
	got := output(t, destination.ListenAddrs(), 1)
	require.Len(t, got, 6)
	for offset := range 6 {
		require.Equal(t, 1, got[fmt.Sprintf("0:%d", offset)])
	}
}

func TestCLIExportsFinalMetricsForTransactionalProgress(t *testing.T) {
	// Given a local OTLP receiver and metrics enabled for a file-only copy.
	binary := streamBinary(t)
	input, target, state := t.TempDir(), t.TempDir(), t.TempDir()
	expected := archiveInput(t, input)
	requests := make(chan *collectormetrics.ExportMetricsServiceRequest, 10)
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(400)
			return
		}
		request := new(collectormetrics.ExportMetricsServiceRequest)
		if proto.Unmarshal(body, request) != nil {
			w.WriteHeader(400)
			return
		}
		select {
		case requests <- request:
		default:
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(200)
	}))
	defer collector.Close()
	cfg := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, os.WriteFile(cfg, []byte(fmt.Sprintf("[worker]\nprogress_topic='progress'\ncheckpoint_dir=%q\n[metrics]\nenabled=true\n", state)), 0600))
	statement := fmt.Sprintf("INSERT INTO ARCHIVE '%s' SELECT * FROM ARCHIVE '%s' WITH(GROUP='metrics')", target, input)
	cmd := exec.CommandContext(testContext(t), binary, "run", "--config", cfg, "--file", "-")
	cmd.Stdin = strings.NewReader(statement)
	cmd.Env = append(os.Environ(), "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT="+collector.URL+"/v1/metrics", "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL=http/protobuf")
	// When the real process completes, then its final confirmed and successful-save counters reach OTLP.
	logs, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", logs)
	var confirmed, saves int64
	for len(requests) > 0 {
		request := <-requests
		for _, resource := range request.ResourceMetrics {
			for _, scope := range resource.ScopeMetrics {
				for _, metric := range scope.Metrics {
					for _, point := range metric.GetSum().GetDataPoints() {
						if metric.Name == "kaferry.records.confirmed" {
							confirmed = max(confirmed, point.GetAsInt())
						}
						if metric.Name == "kaferry.progress.saves" {
							for _, attribute := range point.Attributes {
								if attribute.Key == "outcome" && attribute.Value.GetStringValue() == "success" {
									saves += point.GetAsInt()
								}
							}
						}
					}
				}
			}
		}
	}
	var total int
	for _, count := range expected {
		total += count
	}
	require.EqualValues(t, total, confirmed)
	require.Positive(t, saves)
	require.Equal(t, expected, archiveValues(t, target))
}
