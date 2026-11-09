package compat

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRSSSampleOwnedChild(t *testing.T) {
	if os.Getenv("KAFERRY_TEST_RSS_CHILD") == "1" {
		rssSampleChild(t)
		return
	}

	// Given an owned child that waits for input before allocating memory.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRSSSampleOwnedChild$")
	command.Env = append(os.Environ(), "KAFERRY_TEST_RSS_CHILD=1")
	var diagnostics bytes.Buffer
	command.Stderr = &diagnostics
	input, err := command.StdinPipe()
	require.NoError(t, err)
	output, err := command.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, command.Start())
	waited := false
	t.Cleanup(func() {
		_ = input.Close()
		if !waited {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	})
	reader := bufio.NewReader(output)
	line, err := reader.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "ready\n", line)
	initial, err := rssSample(command.Process.Pid)
	require.NoError(t, err, "sample before releasing child input")
	require.Positive(t, initial)

	// When the child touches 64 MiB and waits with those pages still live.
	_, err = input.Write([]byte("g"))
	require.NoError(t, err)
	line, err = reader.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "grown\n", line)
	loaded, err := rssSample(command.Process.Pid)
	require.NoError(t, err)

	// Then live RSS grows, and its KiB units keep it below 256 MiB of growth.
	require.Greater(t, loaded-initial, int64(32<<10))
	require.Less(t, loaded-initial, int64(256<<10))
	t.Logf("owned child RSS: initial=%d KiB loaded=%d KiB", initial, loaded)
	_, err = input.Write([]byte("x"))
	require.NoError(t, err)
	err = command.Wait()
	waited = true
	require.NoError(t, err, diagnostics.String())
	_, err = rssSample(command.Process.Pid)
	require.ErrorIs(t, err, syscall.ESRCH, "an exited child has no live RSS")
	require.NoError(t, ctx.Err())
}

func rssSampleChild(t *testing.T) {
	t.Helper()
	_, err := fmt.Fprintln(os.Stdout, "ready")
	require.NoError(t, err)
	var command [1]byte
	_, err = io.ReadFull(os.Stdin, command[:])
	require.NoError(t, err)
	memory := make([]byte, 64<<20)
	for i := 0; i < len(memory); i += os.Getpagesize() {
		memory[i] = byte(i/os.Getpagesize()) + 1
	}
	_, err = fmt.Fprintln(os.Stdout, "grown")
	require.NoError(t, err)
	_, err = io.ReadFull(os.Stdin, command[:])
	require.NoError(t, err)
	runtime.KeepAlive(memory)
}
