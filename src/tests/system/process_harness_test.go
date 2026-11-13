//go:build integration && harness

package compat

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

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
