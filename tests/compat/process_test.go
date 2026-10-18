//go:build integration

package compat

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// cliProcess owns one Wait call; completion publishes its output and exit status.
type cliProcess struct {
	command *exec.Cmd
	stdin   io.WriteCloser
	done    chan struct{}
	output  bytes.Buffer
	err     error
}

func startCLI(t *testing.T, ctx context.Context, binary, input string, args ...string) *cliProcess {
	t.Helper()
	p := &cliProcess{command: exec.CommandContext(ctx, binary, args...), done: make(chan struct{})}
	p.command.Stdin = strings.NewReader(input)
	p.start(t)
	return p
}

func binaryWorker(t *testing.T, ctx context.Context, statement, config string) workerResult {
	t.Helper()
	process := startCLI(t, ctx, testBinary(t), statement, "run", "--file", "-", "--config", config)
	err := process.wait()
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
	p.command.Stderr = &p.output
	p.command.Stdout = &p.output
	require.NoError(t, p.command.Start())
	go func() { p.err = p.command.Wait(); close(p.done) }()
	t.Cleanup(func() {
		if !p.finished() {
			_ = p.command.Process.Kill()
		}
		_ = p.wait()
		if t.Failed() {
			t.Logf("copy process output: %s", p.output.String())
		}
	})
}
func (p *cliProcess) send(input string) error {
	_, err := io.WriteString(p.stdin, input)
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
func (p *cliProcess) logs() string                  { _ = p.wait(); return p.output.String() }
func (p *cliProcess) cpu() time.Duration {
	_ = p.wait()
	return p.command.ProcessState.UserTime() + p.command.ProcessState.SystemTime()
}
