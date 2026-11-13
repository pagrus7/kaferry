//go:build integration && fault && harness

package compat

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRebalanceChurnUsesSelectedBinary(t *testing.T) {
	// Given a selected executable with a checked digest that needs no brokers.
	selected := filepath.Join(t.TempDir(), "selected")
	data := []byte("#!/bin/sh\nprintf 'selected executable: %s\\n' \"$*\"\n")
	require.NoError(t, os.WriteFile(selected, data, 0700))
	t.Setenv("KAFERRY_TEST_BINARY", selected)
	t.Setenv("KAFERRY_EXPECT_BINARY_SHA256", fmt.Sprintf("%x", sha256.Sum256(data)))
	results := make(chan workerResult, 1)
	// When the churn fixture starts a worker, then it executes that binary and
	// passes the same SQL/config arguments used by the broker scenario.
	process, _ := startChurnWorker(t, t.Context(), "SELECT fixture", "fixture.toml", results)
	require.NoError(t, process.wait())
	result := <-results
	require.Zero(t, result.code)
	require.Contains(t, result.detail, "selected executable: run -e SELECT fixture -c fixture.toml")
}
