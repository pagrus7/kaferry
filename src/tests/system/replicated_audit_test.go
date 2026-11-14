//go:build integration && fault

package compat

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pagrus7/kaferry/src/tests/system/faults"
	"github.com/stretchr/testify/require"
)

func assertFailedAuditArtifact(t *testing.T, directory, mode string, artifact failedAuditArtifact) {
	t.Helper()
	if mode == "empty" && artifact.Broker == 3 && strings.Contains(artifact.Path, "requests") {
		require.False(t, artifact.Saved)
		require.Contains(t, strings.Join(artifact.Errors, " "), "absence of requests is not established")
		_, err := os.Stat(filepath.Join(directory, artifact.Path))
		require.ErrorIs(t, err, os.ErrNotExist)
		return
	}
	require.True(t, artifact.Saved)
	data, err := os.ReadFile(filepath.Join(directory, artifact.Path))
	require.NoError(t, err)
	require.NotEmpty(t, data)
	require.Equal(t, fmt.Sprintf("%x", sha256.Sum256(data)), artifact.SHA256)
}

func runReplicatedAuditFailure(t *testing.T, directory string) {
	t.Helper()
	// Given an actual failing test after injection, with available response
	// completions and, optionally, a failed or empty diagnostic command.
	mode := os.Getenv("KAFERRY_AUDIT_FAILURE_MODE")
	calls := 0
	audit := &replicatedAudit{output: directory, faultAttempted: true,
		targets: []faults.Target{{Broker: 1, Container: "one"}, {Broker: 2, Container: "two"}, {Broker: 3, Container: "three"}},
		failureCommand: func(ctx context.Context, args ...string) ([]byte, error) {
			calls++
			if mode == "partial" && args[0] == "logs" && args[1] == "two" {
				return []byte("available console prefix\n"), errors.New("container log read failed")
			}
			if mode == "empty" && args[0] == "exec" && args[1] == "three" {
				return nil, nil
			}
			return []byte("Completed request: retained response\n"), ctx.Err()
		}}
	t.Cleanup(func() {
		audit.retainFailure(t)
		// The following restoration/teardown step can inspect the receipt,
		// even when capture adds an error to the original failed test.
		_, err := os.Stat(filepath.Join(directory, "replicated-audit.failure.json"))
		if err == nil {
			err = os.WriteFile(filepath.Join(directory, "restoration-reached"), []byte(fmt.Sprint(calls)), 0600)
		}
		if err != nil {
			t.Error(err)
		}
	})
	t.Fatal("original fixture failure")
}
