//go:build integration

package compat

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// cliProcess owns one Wait call; completion publishes its output and exit status.
type cliProcess struct {
	command *exec.Cmd
	done    chan struct{}
	output  bytes.Buffer
	err     error
}

func startCLI(t *testing.T, ctx context.Context, binary, input string, args ...string) *cliProcess {
	t.Helper()
	p := &cliProcess{command: exec.CommandContext(ctx, binary, args...), done: make(chan struct{})}
	p.command.Stdin = strings.NewReader(input)
	p.command.Stderr = &p.output
	if err := p.command.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { p.err = p.command.Wait(); close(p.done) }()
	t.Cleanup(func() {
		if !p.finished() {
			_ = p.command.Process.Kill()
		}
		_ = p.wait()
		if t.Failed() {
			t.Logf("copy process stderr: %s", p.output.String())
		}
	})
	return p
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
