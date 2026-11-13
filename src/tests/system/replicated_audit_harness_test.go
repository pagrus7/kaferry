//go:build integration && fault && harness

package compat

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/src/tests/system/faults"
	"github.com/stretchr/testify/require"
)

func TestReplicatedAuditFailureCleanupRetainsDiagnostics(t *testing.T) {
	const helper = "KAFERRY_AUDIT_FAILURE_HELPER"
	if directory := os.Getenv(helper); directory != "" {
		runReplicatedAuditFailure(t, directory)
	}
	for _, mode := range []string{"captured", "partial", "empty"} {
		t.Run(mode, func(t *testing.T) {
			directory := t.TempDir()
			// When an early failure unwinds real testing.T cleanup callbacks.
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestReplicatedAuditFailureCleanupRetainsDiagnostics$")
			command.Env = append(os.Environ(), helper+"="+directory, "KAFERRY_AUDIT_FAILURE_MODE="+mode)
			output, commandErr := command.CombinedOutput()
			// Then capture cannot clear the original failure or publish proof.
			require.Error(t, commandErr)
			require.Contains(t, string(output), "original fixture failure")
			require.Contains(t, string(output), "FAIL")
			var receipt failedAuditCapture
			receiptData, readErr := os.ReadFile(filepath.Join(directory, "replicated-audit.failure.json"))
			require.NoError(t, readErr)
			require.NoError(t, json.Unmarshal(receiptData, &receipt))
			require.True(t, receipt.CaseFailed)
			require.True(t, receipt.DiagnosticOnly)
			require.Equal(t, mode != "captured", receipt.Partial)
			require.Len(t, receipt.Artifacts, 6)
			for _, artifact := range receipt.Artifacts {
				assertFailedAuditArtifact(t, directory, mode, artifact)
			}
			if mode == "partial" {
				require.Contains(t, string(output), "retain failed broker audit (diagnostics only)")
				require.Contains(t, string(output), "container log read failed")
			}
			restored, restoreErr := os.ReadFile(filepath.Join(directory, "restoration-reached"))
			require.NoError(t, restoreErr)
			require.Equal(t, "6", string(restored), "one attempt for each artifact, followed by restoration")
			for _, name := range []string{"source-1.final.requests.log", "source-1.console.log", "replicated-ownership.json", "ownership-proof.json"} {
				_, err := os.Stat(filepath.Join(directory, name))
				require.ErrorIs(t, err, os.ErrNotExist)
			}
		})
	}
}

func TestReplicatedAuditFailureCleanupSkipsPassingCases(t *testing.T) {
	// Given a passing case after injection, when cleanup runs, then only the
	// existing strict final capture path may produce qualification evidence.
	directory := t.TempDir()
	audit := &replicatedAudit{output: directory, faultAttempted: true,
		failureCommand: func(context.Context, ...string) ([]byte, error) {
			t.Fatal("passing case must not invoke failure capture")
			return nil, nil
		}}
	audit.retainFailure(t)
	entries, err := os.ReadDir(directory)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestReplicatedAuditFailureCaptureSharesDeadline(t *testing.T) {
	// Given a blocked command and a single bounded cleanup deadline.
	directory := t.TempDir()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	calls := 0
	audit := &replicatedAudit{output: directory, targets: []faults.Target{{Broker: 1}, {Broker: 2}, {Broker: 3}},
		failureCommand: func(ctx context.Context, _ ...string) ([]byte, error) {
			calls++
			<-ctx.Done()
			return []byte("partial response\n"), ctx.Err()
		}}
	// When that deadline expires, then every artifact records its bounded
	// attempt and available bytes without retrying or becoming success proof.
	require.ErrorContains(t, audit.captureFailure(ctx), "context deadline exceeded")
	require.Equal(t, 6, calls)
	var receipt failedAuditCapture
	data, err := os.ReadFile(filepath.Join(directory, "replicated-audit.failure.json"))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &receipt))
	require.True(t, receipt.Partial)
	for _, artifact := range receipt.Artifacts {
		require.True(t, artifact.Saved)
		require.NotEmpty(t, artifact.Errors)
	}
}

func TestReplicatedAuditFailureOutputIsBounded(t *testing.T) {
	// Given an oversized broker output stream, when retained, then its prefix
	// is bounded and truncation is explicit instead of a complete audit claim.
	var output boundedAuditOutput
	data := bytes.Repeat([]byte("x"), auditCaptureLimit+1)
	n, err := output.Write(data)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
	require.Len(t, output.data, auditCaptureLimit)
	require.True(t, output.exceeded)
}

func TestReplicatedAuditArmsOnlyAfterCurrentGenerationAcceptedProgress(t *testing.T) {
	t.Run("post-transfer receipt", replicatedAuditRequiresPostTransferReceipt)
	// Given a worker whose earlier generation committed, then rejoined.
	rows := []string{
		`{"requestHeader":{"clientId":"one","requestApiKeyName":"JOIN_GROUP"},"request":{"groupId":"group"},"response":{"errorCode":0,"memberId":"member","generationId":1}}`,
		`{"requestHeader":{"clientId":"one","requestApiKeyName":"OFFSET_COMMIT"},"request":{"groupId":"group","memberId":"member","generationIdOrMemberEpoch":1,"topics":[{"partitions":[{"partitionIndex":0,"committedOffset":"5"},{"partitionIndex":1,"committedOffset":5}]}]},"response":{"topics":[{"partitions":[{"partitionIndex":0,"errorCode":0},{"partitionIndex":1,"errorCode":0}]}]}}`,
		`{"requestHeader":{"clientId":"one","requestApiKeyName":"JOIN_GROUP"},"request":{"groupId":"group"},"response":{"errorCode":0,"memberId":"member","generationId":2}}`,
	}
	encode := func() []byte {
		var data []byte
		for _, row := range rows {
			data = append(data, []byte("Completed request:"+row+" (kafka.request.logger)\n")...)
		}
		return data
	}
	generation, ready, err := currentGenerationSaved(encode(), "one", "group", "member", []int32{0, 1}, nil, time.Time{})
	require.NoError(t, err)
	require.EqualValues(t, 2, generation)
	require.False(t, ready, "earlier generation commits cannot arm a stale-ownership fault")

	// When current-generation progress is rejected, it still cannot arm the case.
	rows = append(rows, `{"requestHeader":{"clientId":"one","requestApiKeyName":"OFFSET_COMMIT"},"request":{"groupId":"group","memberId":"member","generationIdOrMemberEpoch":2,"topics":[{"partitions":[{"partitionIndex":0,"committedOffset":"6"},{"partitionIndex":1,"committedOffset":6}]}]},"response":{"topics":[{"partitions":[{"partitionIndex":0,"errorCode":22},{"partitionIndex":1,"errorCode":22}]}]}}`)
	_, ready, err = currentGenerationSaved(encode(), "one", "group", "member", []int32{0, 1}, nil, time.Time{})
	require.NoError(t, err)
	require.False(t, ready)

	// Then only actual accepted positive progress for every owned partition in
	// the latest joined generation permits injection.
	rows = append(rows, `{"requestHeader":{"clientId":"one","requestApiKeyName":"OFFSET_COMMIT"},"request":{"groupId":"group","memberId":"member","generationIdOrMemberEpoch":2,"topics":[{"partitions":[{"partitionIndex":0,"committedOffset":"6"},{"partitionIndex":1,"committedOffset":6}]}]},"response":{"topics":[{"partitions":[{"partitionIndex":0,"errorCode":0},{"partitionIndex":1,"errorCode":0}]}]}}`)
	_, ready, err = currentGenerationSaved(encode(), "one", "group", "member", []int32{0, 1}, nil, time.Time{})
	require.NoError(t, err)
	require.True(t, ready)
	_, ready, err = currentGenerationSaved(encode(), "one", "group", "member", []int32{0, 1}, map[int32]int64{0: 6, 1: 6}, time.Time{})
	require.NoError(t, err)
	require.False(t, ready, "peer progress must advance beyond the observed pre-fault prefix")
	_, ready, err = currentGenerationSaved(encode(), "one", "group", "member", []int32{0, 1, 2}, nil, time.Time{})
	require.NoError(t, err)
	require.False(t, ready, "an unobserved partition cannot be inferred from its peers")
}

func TestReplicatedAuditPreservesDecodedConfigurationAndOriginalDigest(t *testing.T) {
	// Given YAML whose formatting and scalar types differ from JSON.
	data := []byte("Configuration:\n  Loggers:\n    Logger:\n      - name: kafka.request.logger\n        additivity: false\n")
	// When captured, then the semantic representation remains bound to its bytes.
	encoded, err := requestLogConfiguration(data)
	require.NoError(t, err)
	var snapshot struct {
		Digest        string `json:"raw_sha256"`
		Configuration map[string]map[string]map[string][]map[string]any
	}
	require.NoError(t, json.Unmarshal(encoded, &snapshot))
	require.Equal(t, fmt.Sprintf("%x", sha256.Sum256(data)), snapshot.Digest)
	require.Equal(t, false, snapshot.Configuration["Configuration"]["Loggers"]["Logger"][0]["additivity"])
	_, err = requestLogConfiguration([]byte("Configuration: {}\nConfiguration: {}\n"))
	require.Error(t, err, "duplicate configuration keys must not disguise the active logger")
}
