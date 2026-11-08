//go:build integration && fault

package compat

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// faultOracle is a separate librdkafka process, never an adapter/client callback.
// Its output stays under the suite receipt directory when one is requested.
type faultOracle struct {
	process  *exec.Cmd
	input    io.WriteCloser
	output   *bufio.Scanner
	log      string
	budget   time.Duration
	finished bool
	writers  []*cliProcess
	recovery time.Duration
	timeouts map[string]time.Duration
}

func faultTimeouts(t *testing.T, id string) map[string]time.Duration {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "qualification", "fault-timeouts-ms.json"))
	require.NoError(t, err)
	var catalog map[string]map[string]int64
	require.NoError(t, json.Unmarshal(data, &catalog))
	if _, present := catalog[id]; !present {
		return nil
	}
	result := make(map[string]time.Duration)
	for name, milliseconds := range catalog[id] {
		require.Positive(t, milliseconds)
		result[name] = time.Duration(milliseconds) * time.Millisecond
	}
	return result
}

func startFaultOracle(t *testing.T, id, source, destination, src, dst, group string, partitions int, allowEmpty bool) *faultOracle {
	t.Helper()
	root, err := filepath.Abs("../../..")
	require.NoError(t, err)
	python := os.Getenv("KAFERRY_ORACLE_PYTHON")
	require.NotEmpty(t, python, "run qualification setup and use make fault or qualify run fault/recovery")
	directory := os.Getenv("KAFERRY_REPORT_DIR")
	if directory == "" {
		directory = t.TempDir()
	}
	parent := filepath.Join(directory, "fault-oracle", os.Getenv("KAFERRY_ORACLE_RUN"))
	require.NoError(t, os.MkdirAll(parent, 0700))
	unique, err := os.MkdirTemp(parent, strings.ReplaceAll(id, "/", "-")+"-")
	require.NoError(t, err)
	// The observer creates its own directory; retain stderr beside it on failure.
	output := filepath.Join(unique, "observations")
	log := filepath.Join(unique, "observer.log")
	stderr, err := os.Create(log)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stderr.Close() })
	binary := testBinary(t)
	budget := 60*time.Second + time.Duration(3*partitions+1)*time.Second
	args := []string{"-m", "tools.qualification.fault_oracle", "--case", id, "--source", source, "--destination", destination,
		"--source-topic", src, "--destination-topic", dst, "--group", group, "--partitions", fmt.Sprint(partitions), "--output", output,
		"--binary", binary, "--ready-deadline", fmt.Sprintf("%.6f", float64(time.Now().Add(budget).UnixMicro())/1e6)}
	if allowEmpty {
		args = append(args, "--allow-empty")
	}
	process := exec.CommandContext(t.Context(), python, args...)
	process.Dir, process.Stderr = root, stderr
	input, err := process.StdinPipe()
	require.NoError(t, err)
	stdout, err := process.StdoutPipe()
	require.NoError(t, err)
	observer := &faultOracle{process: process, input: input, output: bufio.NewScanner(stdout), log: log,
		budget: budget, timeouts: faultTimeouts(t, id)}
	require.NoError(t, process.Start())
	t.Cleanup(func() {
		_ = input.Close()
		_ = process.Process.Kill()
		if !observer.finished {
			_ = process.Wait()
		}
	})
	require.Equal(t, true, observer.response(t)["ready"])
	return observer
}

func (o *faultOracle) response(t *testing.T) map[string]any {
	t.Helper()
	type response struct {
		data []byte
		err  error
	}
	result := make(chan response, 1)
	go func() {
		if !o.output.Scan() {
			err := o.output.Err()
			if err == nil {
				err = io.EOF
			}
			result <- response{err: fmt.Errorf("oracle ended: %w", err)}
			return
		}
		result <- response{data: append([]byte(nil), o.output.Bytes()...)}
	}()
	select {
	case read := <-result:
		log, _ := os.ReadFile(o.log)
		require.NoError(t, read.err, "%s", log)
		var value map[string]any
		require.NoError(t, json.Unmarshal(read.data, &value), "%s", log)
		require.Empty(t, value["error"], "%s", log)
		return value
	case <-time.After(o.budget): // Two 30s captured-end drains and per-partition 1s metadata queries.
		t.Fatal("independent oracle exceeded its observation budget; inspect ", o.log)
	case <-t.Context().Done():
		t.Fatal(context.Cause(t.Context()))
	}
	return nil
}

func (o *faultOracle) command(t *testing.T, value map[string]any) map[string]any {
	t.Helper()
	data, err := json.Marshal(value)
	require.NoError(t, err)
	_, err = fmt.Fprintln(o.input, string(data))
	require.NoError(t, err)
	return o.response(t)
}

func (o *faultOracle) writer(process *cliProcess) {
	o.writers = append(o.writers, process)
}

func (o *faultOracle) begin(t *testing.T, kind string, target any, unavailable bool, timeouts map[string]time.Duration, observation string) int {
	t.Helper()
	require.NotEmpty(t, timeouts)
	recovery := time.Duration(0)
	basis := make(map[string]float64, len(timeouts))
	for name, timeout := range timeouts {
		require.Positive(t, timeout)
		basis[name] = timeout.Seconds()
		// A replacement may first wait out the old session, then retry delivery
		// and save on shutdown. The sum covers those serial configured budgets.
		recovery += timeout
	}
	o.recovery = max(o.recovery, recovery)
	result := o.command(t, map[string]any{"command": "fault-start", "kind": kind, "target": target,
		"unavailable": unavailable, "recovery_seconds": recovery.Seconds(), "timeout_basis": basis, "observation": observation})
	return int(result["window"].(float64))
}

func (o *faultOracle) injected(t *testing.T, window int, kind string, target any, observation string) {
	t.Helper()
	o.command(t, map[string]any{"command": "fault-injected", "window": window, "kind": kind, "target": target, "observation": observation})
}

func (o *faultOracle) healed(t *testing.T, window int, observation string) {
	t.Helper()
	o.command(t, map[string]any{"command": "fault-healed", "window": window, "observation": observation})
}

func (o *faultOracle) finish(t *testing.T) {
	t.Helper()
	o.budget += o.recovery
	writers := make([]map[string]any, 0, len(o.writers))
	for _, process := range o.writers {
		require.True(t, process.finished(), "stop every registered fixture writer before the final snapshot")
		_ = process.wait()
		writers = append(writers, map[string]any{"pid": process.pid(), "exit_code": process.command.ProcessState.ExitCode(),
			"observation": "OS process wait completed before final snapshot"})
	}
	require.Equal(t, true, o.command(t, map[string]any{"command": "finish", "writers_stopped": true, "writer_observations": writers})["finished"])
	err := o.process.Wait()
	o.finished = true
	require.NoError(t, err)
}
