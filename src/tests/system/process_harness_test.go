//go:build integration && harness

package compat

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCLIDelayedInputPreservesBytesAndClosesStdin(t *testing.T) {
	// Given a delayed SQL input pipe and a reader that waits for its end.
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close(); _ = writer.Close() })
	process := &cliProcess{stdin: writer}
	type received struct {
		data []byte
		err  error
	}
	result := make(chan received, 1)
	go func() {
		data, readErr := io.ReadAll(reader)
		result <- received{data: data, err: readErr}
	}()
	input := "INSERT INTO ARCHIVE 'outı''put'\r\nSELECT * FROM ARCHIVE '源/in' WITH(GROUP='g') OPTIONS(WORKER_STOP_AFTER_RECORDS=1);\n"

	// When the launcher releases input, then it preserves every byte and closes
	// stdin so the reader can finish without waiting for any more input.
	require.NoError(t, process.send(input))
	select {
	case observed := <-result:
		require.NoError(t, observed.err)
		require.Equal(t, []byte(input), observed.data)
	case <-time.After(time.Second):
		t.Fatal("delayed CLI input did not reach EOF after send")
	}
}

func TestBrokerCurrentProgressDoesNotWaitForChildExit(t *testing.T) {
	// Given a real child that reports progress and remains alive awaiting input.
	process := brokerLogChild(t, "")
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
	if terminal := os.Getenv("KAFERRY_BROKER_LOG_CHILD_ERROR"); terminal != "" {
		_, err = fmt.Fprintln(os.Stderr, terminal)
		require.NoError(t, err)
		os.Exit(1)
	}
}

func TestBrokerCurrentProgressRequiresReadableCompleteEvidence(t *testing.T) {
	// Given a live process log with one complete row and an unfinished append.
	path := filepath.Join(t.TempDir(), "cli.log")
	process := &cliProcess{output: path, done: make(chan struct{})}
	partial := `{"msg":"progress","submitted":99`
	require.NoError(t, os.WriteFile(path, []byte("{\"msg\":\"partition assigned\",\"partition\":0}\n"+brokerProgressRow+partial), 0600))
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
		`{"msg":"progress","submitted":9,"confirmed":7,"peak_uncommitted_records":"bad"}`,
	} {
		_, found, err := latestCopyProgress(brokerProgressRow + row + "\n")
		require.Error(t, err, row)
		require.False(t, found)
	}
}
