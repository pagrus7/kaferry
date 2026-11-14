//go:build integration && fault && harness

package compat

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/src/tests/system/faults"
	"github.com/stretchr/testify/require"
)

func TestReplicatedAuditGrowthAccountsForEveryCapturedByte(t *testing.T) {
	// Given complete request lines from two clients, one escaped client ID,
	// and two request types, when summarized, then bytes retain their owners.
	header := []byte("KAFERRY_AUDIT_FILE kafka-request.log\n")
	first := []byte("Completed request:{\"requestHeader\":{\"clientId\":\"one\",\"requestApiKeyName\":\"FETCH\"}} (kafka.request.logger)\n")
	second := []byte("Completed request:{\"requestHeader\":{\"clientId\":\"two\\\"\",\"requestApiKeyName\":\"PRODUCE\"}} (kafka.request.logger)\n")
	data := bytes.Join([][]byte{header, first, second, first}, nil)
	row := summarizeAuditGrowth(2, "interval", data, nil)
	require.True(t, row.Complete)
	require.Equal(t, int64(2*len(first)), row.ClientBytes["one"])
	require.Equal(t, int64(len(second)), row.ClientBytes["two\""])
	require.Equal(t, row.ClientBytes["one"], row.RequestBytes["FETCH"])
	require.Equal(t, row.ClientBytes["two\""], row.RequestBytes["PRODUCE"])
	require.Equal(t, int64(len(data)), row.HeaderBytes+row.RequestBytes["FETCH"]+row.RequestBytes["PRODUCE"]+row.UnclassifiedBytes)
	// Malformed and truncated input must remain visible, not become a
	// complete zero-traffic summary; capture errors also prohibit completeness.
	for _, bad := range [][]byte{[]byte("Completed request:{\n"), first[:len(first)-1], nil} {
		got := summarizeAuditGrowth(2, "interval", bad, nil)
		require.False(t, got.Complete)
		require.NotEmpty(t, got.Error)
		require.Equal(t, int64(len(bad)), got.UnclassifiedBytes)
	}
	row = summarizeAuditGrowth(2, "failure", data, errors.New("capture command failed"))
	require.False(t, row.Complete)
	require.Contains(t, row.Error, "capture command failed")
}

func TestReplicatedAuditGrowthSurvivesEarlyFailureAndCap(t *testing.T) {
	const helper = "KAFERRY_AUDIT_GROWTH_HELPER"
	if directory := os.Getenv(helper); directory != "" {
		// Given the real sampler registered before a setup failure, retain
		// interval and oversized-capture summaries without producing proof.
		audit := &replicatedAudit{output: directory}
		audit.startGrowth()
		t.Cleanup(audit.stopGrowth)
		audit.recordGrowth(1, "interval", []byte("KAFERRY_AUDIT_FILE kafka-request.log\n"), nil)
		audit.recordGrowth(1, "final", bytes.Repeat([]byte{'x'}, auditCaptureLimit+1), nil)
		t.Fatal("original fixture setup failure")
	}
	directory := t.TempDir()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestReplicatedAuditGrowthSurvivesEarlyFailureAndCap$")
	command.Env = append(os.Environ(), helper+"="+directory)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "original fixture setup failure")
	data, err := os.ReadFile(filepath.Join(directory, "replicated-audit.growth.jsonl"))
	require.NoError(t, err)
	var rows []auditGrowthRecord
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte{'\n'}) {
		var row auditGrowthRecord
		require.NoError(t, json.Unmarshal(line, &row))
		rows = append(rows, row)
	}
	require.Len(t, rows, 3)
	require.Equal(t, "start", rows[0].Reason)
	require.Equal(t, "interval", rows[1].Reason)
	require.True(t, rows[1].Complete)
	require.Equal(t, "final", rows[2].Reason)
	require.True(t, rows[2].CapHit)
	require.False(t, rows[2].Complete)
	require.Equal(t, auditCaptureLimit+1, rows[2].Bytes)
	require.False(t, rows[2].Observed.Before(rows[1].Observed))
	require.GreaterOrEqual(t, rows[2].Elapsed, rows[1].Elapsed)
	_, err = os.Stat(filepath.Join(directory, "ownership-proof.json"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestReplicatedAuditGrowthFailureCaptureRetainsAvailableCounts(t *testing.T) {
	// Given a failed command with a complete available request, when cleanup
	// retains diagnostics, then its summary survives and cannot claim completeness.
	directory := t.TempDir()
	audit := &replicatedAudit{output: directory, growth: &auditGrowth{started: time.Now(), path: filepath.Join(directory, "replicated-audit.growth.jsonl")},
		targets: []faults.Target{{Broker: 1}}, failureCommand: func(context.Context, ...string) ([]byte, error) {
			return []byte("Completed request:{\"requestHeader\":{\"clientId\":\"worker\",\"requestApiKeyName\":\"FETCH\"}}\n"), errors.New("fixture disappeared")
		}}
	require.ErrorContains(t, audit.captureFailure(t.Context()), "fixture disappeared")
	data, err := os.ReadFile(audit.growth.path)
	require.NoError(t, err)
	var row auditGrowthRecord
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(data), &row))
	require.Equal(t, "failure", row.Reason)
	require.Positive(t, row.ClientBytes["worker"])
	require.Equal(t, row.ClientBytes["worker"], row.RequestBytes["FETCH"])
	require.Contains(t, row.Error, "fixture disappeared")
	require.False(t, row.Complete)
}
