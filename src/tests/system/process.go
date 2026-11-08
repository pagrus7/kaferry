//go:build integration

package compat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// cliProcess owns one Wait call; completion publishes its output and exit status.
type cliProcess struct {
	command *exec.Cmd
	stdin   io.WriteCloser
	done    chan struct{}
	output  string
	err     error
}

func startCLI(t *testing.T, ctx context.Context, binary, input string, args ...string) *cliProcess {
	t.Helper()
	for i := 0; i < len(args)-1; i++ {
		if args[i] == "-e" || args[i] == "--sql" {
			var err error
			args[i+1], err = binarySQL(binary, args[i+1])
			require.NoError(t, err)
		}
	}
	if input != "" {
		var err error
		input, err = binarySQL(binary, input)
		require.NoError(t, err)
	}
	p := &cliProcess{command: exec.CommandContext(ctx, binary, args...), done: make(chan struct{})}
	p.command.Stdin = strings.NewReader(input)
	p.start(t)
	return p
}

func binaryWorker(t *testing.T, ctx context.Context, statement, config string) workerResult {
	t.Helper()
	process := startCLI(t, ctx, testBinary(t), statement, "run", "--file", "-", "--config", config)
	err := process.wait()
	require.NoError(t, ctx.Err(), "worker must exit before its deadline: %s", process.logs())
	code := 0
	if err != nil {
		var exit *exec.ExitError
		require.ErrorAs(t, err, &exit, "worker must start and return an exit status")
		code = exit.ExitCode()
	}
	return workerResult{code, process.logs()}
}

func launchCLI(t *testing.T, ctx context.Context, binary string, args ...string) *cliProcess {
	t.Helper()
	p := &cliProcess{command: exec.CommandContext(ctx, binary, args...), done: make(chan struct{})}
	var err error
	p.stdin, err = p.command.StdinPipe()
	require.NoError(t, err)
	p.start(t)
	return p
}

func (p *cliProcess) start(t *testing.T) {
	t.Helper()
	p.output = filepath.Join(t.TempDir(), "cli.log")
	log, err := os.Create(p.output)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, log.Close()) })
	p.command.Stderr = log
	p.command.Stdout = log
	require.NoError(t, p.command.Start())
	go func() { p.err = p.command.Wait(); close(p.done) }()
	t.Cleanup(func() {
		if !p.finished() {
			_ = p.command.Process.Kill()
		}
		_ = p.wait()
		if t.Failed() {
			t.Logf("copy process output: %s", p.logs())
		}
	})
}
func (p *cliProcess) send(input string) error {
	input, err := binarySQL(p.command.Path, input)
	if err != nil {
		return err
	}
	_, err = io.WriteString(p.stdin, input)
	return errors.Join(err, p.stdin.Close())
}

func (p *cliProcess) finished() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}
func (p *cliProcess) wait() error                   { <-p.done; return p.err }
func (p *cliProcess) signal(signal os.Signal) error { return p.command.Process.Signal(signal) }
func (p *cliProcess) pid() int                      { return p.command.Process.Pid }
func (p *cliProcess) logs() string {
	_ = p.wait()
	data, err := os.ReadFile(p.output)
	if err != nil {
		return err.Error()
	}
	return string(data)
}

// currentLogs reads a fixed file-end snapshot without waiting for the process.
// The bound is for short deterministic cases; long-running observers need a
// streaming reader. An unfinished live suffix remains available to the caller,
// while an exited process must have completed its final newline-terminated row.
func (p *cliProcess) currentLogs() (string, error) {
	finished := p.finished()
	log, err := os.Open(p.output)
	if err != nil {
		return "", fmt.Errorf("open current CLI log: %w", err)
	}
	defer log.Close()
	info, err := log.Stat()
	if err != nil {
		return "", fmt.Errorf("inspect current CLI log: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() > 8<<20 {
		return "", fmt.Errorf("current CLI log must be a regular file no larger than 8 MiB")
	}
	data := make([]byte, int(info.Size()))
	if _, err := io.ReadFull(log, data); err != nil {
		return "", fmt.Errorf("read current CLI log snapshot: %w", err)
	}
	if finished && len(data) > 0 && data[len(data)-1] != '\n' {
		return "", fmt.Errorf("final CLI log has an unfinished row: %w", io.ErrUnexpectedEOF)
	}
	return string(data), nil
}
func (p *cliProcess) cpu() time.Duration {
	_ = p.wait()
	return p.command.ProcessState.UserTime() + p.command.ProcessState.SystemTime()
}

func tutorialShell(ctx context.Context, script string) *exec.Cmd {
	command := exec.CommandContext(ctx, "sh", "-eu", "-c", script)
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error {
		err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	command.WaitDelay = time.Second
	return command
}

type copySummary struct {
	Message     string `json:"msg"`
	Submitted   int    `json:"submitted"`
	Confirmed   int    `json:"confirmed"`
	PeakRecords int    `json:"peak_uncommitted_records"`
	PeakBytes   int64  `json:"peak_uncommitted_bytes"`
}

func summaryFrom(t *testing.T, output string) copySummary {
	t.Helper()
	for _, line := range strings.Split(output, "\n") {
		var summary copySummary
		if json.Unmarshal([]byte(line), &summary) == nil && summary.Message == "copy summary" {
			return summary
		}
	}
	t.Fatal("CLI omitted copy summary", output)
	return copySummary{}
}
