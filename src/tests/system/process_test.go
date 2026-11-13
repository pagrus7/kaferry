//go:build integration

package compat

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const brokerProgressRow = `{"msg":"progress","submitted":9,"confirmed":7,"pending_entries":2,"pending_estimated_bytes":4096,"peak_uncommitted_records":2,"peak_uncommitted_bytes":4096}` + "\n"

func brokerLogChild(t *testing.T, terminal string, setup ...func(*cliProcess)) *cliProcess {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	process := &cliProcess{command: exec.CommandContext(ctx, os.Args[0], "-test.run=^TestBrokerCurrentLogChild$"), done: make(chan struct{})}
	for _, configure := range setup {
		configure(process)
	}
	process.command.Env = append(os.Environ(), "KAFERRY_BROKER_LOG_CHILD=1", "KAFERRY_BROKER_LOG_CHILD_ERROR="+terminal)
	var err error
	process.stdin, err = process.command.StdinPipe()
	require.NoError(t, err)
	process.start(t)
	require.Eventually(t, func() bool {
		data, readErr := os.ReadFile(process.output)
		return readErr == nil && strings.Contains(string(data), brokerProgressRow)
	}, 5*time.Second, 10*time.Millisecond)
	return process
}
