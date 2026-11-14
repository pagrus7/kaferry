//go:build integration && load

package compat

import (
	"context"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoadSamplesMemoryBeforeReleasingInput(t *testing.T) {
	// Given a CLI blocked on SQL input, which may finish before the next sample.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	process := launchCLI(t, ctx, testBinary(t), "run", "-f", "-")

	// When sampling finishes before SQL is released.
	finish := watchRSS(t, process)
	memory := finish()

	// Then memory was measured while the CLI was alive and waiting for input.
	require.Positive(t, memory.samples, "initial memory sample")
	require.Positive(t, memory.peak, "resident memory")
	require.False(t, process.finished(), "CLI must wait for SQL")
	require.Equal(t, memory, finish(), "sampling cleanup is safe to repeat")
	require.NoError(t, process.send("SELECT"))
	var exit *exec.ExitError
	require.ErrorAs(t, process.wait(), &exit, "invalid SQL must fail")
	require.Equal(t, 2, exit.ExitCode(), "invalid SQL exit code")
	require.NoError(t, ctx.Err(), "CLI must exit before the timeout")
}
