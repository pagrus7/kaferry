//go:build integration && fault && load

package compat

import (
	"context"
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

const brokerProgressRow = `{"msg":"progress","submitted":9,"confirmed":7,"peak_uncommitted_records":2,"peak_uncommitted_bytes":4096}` + "\n"

func TestBrokerCurrentProgressDoesNotWaitForChildExit(t *testing.T) {
	// Given a real child that reports progress and remains alive awaiting input.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	process := &cliProcess{command: exec.CommandContext(ctx, os.Args[0], "-test.run=^TestBrokerCurrentLogChild$"), done: make(chan struct{})}
	process.command.Env = append(os.Environ(), "KAFERRY_BROKER_LOG_CHILD=1")
	var err error
	process.stdin, err = process.command.StdinPipe()
	require.NoError(t, err)
	process.start(t)
	require.Eventually(t, func() bool {
		data, readErr := os.ReadFile(process.output)
		return readErr == nil && strings.Contains(string(data), brokerProgressRow)
	}, 5*time.Second, 10*time.Millisecond)
	type observation struct {
		progress copySummary
		found    bool
		err      error
	}
	result := make(chan observation, 1)
	// When the broker orchestration reads the current progress, completion is
	// bounded independently of the child's exit and the child remains alive.
	go func() {
		progress, found, readErr := currentCopyProgress(process)
		result <- observation{progress: progress, found: found, err: readErr}
	}()
	select {
	case observed := <-result:
		require.NoError(t, observed.err)
		require.True(t, observed.found)
		require.Equal(t, 9, observed.progress.Submitted)
		require.Equal(t, 7, observed.progress.Confirmed)
		require.Equal(t, 2, observed.progress.PeakRecords)
		require.EqualValues(t, 4096, observed.progress.PeakBytes)
	case <-time.After(time.Second):
		t.Fatal("current progress waited for the live child to exit")
	}
	// Then the same child can still be released and joined successfully.
	require.False(t, process.finished())
	require.NoError(t, process.stdin.Close())
	require.NoError(t, process.wait())
}

func TestBrokerCurrentLogChild(t *testing.T) {
	if os.Getenv("KAFERRY_BROKER_LOG_CHILD") != "1" {
		return
	}
	_, err := fmt.Fprint(os.Stdout, brokerProgressRow)
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, os.Stdin)
	require.NoError(t, err)
}

func TestBrokerCurrentProgressRequiresReadableCompleteEvidence(t *testing.T) {
	// Given a live process log with one complete row and an unfinished append.
	path := filepath.Join(t.TempDir(), "cli.log")
	process := &cliProcess{output: path, done: make(chan struct{})}
	partial := `{"msg":"progress","submitted":99`
	require.NoError(t, os.WriteFile(path, []byte(brokerProgressRow+partial), 0600))
	// When read while live, then only the complete row supplies progress. The
	// pending suffix cannot fabricate the later submitted count.
	progress, found, err := currentCopyProgress(process)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 9, progress.Submitted)
	require.NoError(t, os.WriteFile(path, []byte(partial), 0600))
	_, found, err = currentCopyProgress(process)
	require.NoError(t, err)
	require.False(t, found, "an unfinished first row is pending, not zero-valued progress")
	// Once the process ends, that same unfinished final row is truncated evidence.
	close(process.done)
	_, _, err = currentCopyProgress(process)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	process.output = path + "-missing"
	_, _, err = currentCopyProgress(process)
	require.ErrorIs(t, err, os.ErrNotExist)
	process.output = path
	require.NoError(t, os.Truncate(path, (8<<20)+1))
	_, _, err = currentCopyProgress(process)
	require.ErrorContains(t, err, "8 MiB")
}

func TestBrokerCurrentProgressRejectsMalformedCompleteRows(t *testing.T) {
	// Given otherwise valid earlier progress, when a complete progress row is
	// malformed, missing counts or inconsistent, then the old row cannot hide it.
	for _, row := range []string{
		`{"msg":"progress","submitted":9`,
		`{"msg":"progress","submitted":"9","confirmed":7}`,
		`{"msg":"progress","submitted":9}`,
		`{"msg":"progress","submitted":null,"confirmed":0}`,
		`{"msg":"progress","submitted":9,"confirmed":10}`,
		`{"msg":"progress","submitted":-1,"confirmed":0}`,
	} {
		_, found, err := latestCopyProgress(brokerProgressRow + row + "\n")
		require.Error(t, err, row)
		require.False(t, found)
	}
}
