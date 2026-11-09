//go:build integration && load

package compat

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEnduranceRecoveryRetainsFailedGateBeforeFatalAssertion(t *testing.T) {
	if os.Getenv("KAFERRY_TEST_RECOVERY_FAILURE") != "" {
		enduranceRecoveryFailureChild(t)
		return
	}
	// Given an owned child executing the same recovery gate as streaming endurance.
	directory := t.TempDir()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestEnduranceRecoveryRetainsFailedGateBeforeFatalAssertion$")
	command.Env = append(os.Environ(), "KAFERRY_TEST_RECOVERY_FAILURE=1", "KAFERRY_REPORT_DIR="+directory)
	// When its gate fails, then cleanup must close a retained failed observation.
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.NoError(t, ctx.Err())
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit)
	require.Equal(t, 1, exit.ExitCode())
	require.Contains(t, string(output), "backlog failed to recover after destination stall")
	require.FileExists(t, filepath.Join(directory, "cleanup-finished"))
	events := readRecoveryEvents(t, directory)
	require.Len(t, events, 2)
	require.Equal(t, "started", events[0].Result)
	failure := events[1]
	require.Equal(t, "failed", failure.Result)
	require.Equal(t, time.Unix(1700000000, 0).UTC(), failure.Started)
	require.Equal(t, failure.Started.Add(15*time.Second), failure.Deadline)
	require.Equal(t, failure.Started.Add(failure.Elapsed), failure.Checked)
	require.Equal(t, 15*time.Second+5*time.Millisecond, failure.Elapsed)
	require.Equal(t, 9637, failure.Submitted)
	require.Equal(t, 9592, failure.Acknowledged)
	require.Equal(t, 9216, failure.Unique)
	require.Equal(t, 376, failure.Backlog)
	require.Equal(t, 1, failure.Observed.Backlog)
	require.Equal(t, 1, failure.Observed.Unsettled)
	require.Equal(t, int64(2), failure.Operations.Poll.Count)
	require.Equal(t, 5*time.Millisecond, failure.Operations.Poll.Last)
	require.Equal(t, 8*time.Millisecond, failure.Operations.Poll.Maximum)
	require.Equal(t, 13*time.Millisecond, failure.Operations.Poll.Total)
	require.Equal(t, 200*time.Millisecond, failure.Operations.SavedOffsets.Total)
	metrics, err := filepath.Glob(filepath.Join(directory, "recovery-fixture-*.metrics.json"))
	require.NoError(t, err)
	require.Len(t, metrics, 1)
	data, err := os.ReadFile(metrics[0])
	require.NoError(t, err)
	var report loadReport
	require.NoError(t, json.Unmarshal(data, &report))
	require.False(t, report.Passed)
	require.Zero(t, report.Endurance.Recoveries)
	require.Equal(t, "failed", report.Endurance.Recovery.Result)
	require.Equal(t, failure.Backlog, report.Endurance.Recovery.Backlog)

}

func enduranceRecoveryFailureChild(t *testing.T) {
	t.Helper()
	directory := os.Getenv("KAFERRY_REPORT_DIR")
	t.Cleanup(func() { require.NoError(t, os.WriteFile(filepath.Join(directory, "cleanup-finished"), nil, 0600)) })
	report := newLoadReport(t, "recovery-fixture")
	trace := recoveryTrace(t)
	e := newEnduranceEvidence(time.Minute)
	report.Endurance = e
	records := &streamRecords{published: 9637, acknowledged: 9592, unique: 9216, observer: newDeliveryObserver(time.Now)}
	records.observer.submit(0)
	records.observer.publication(0, nil)
	var operations soakOperationTimes
	e.beginRecovery(time.Unix(1700000000, 0).UTC(), records, &operations, trace)
	operations.Poll.record(8 * time.Millisecond)
	operations.Poll.record(5 * time.Millisecond)
	operations.SavedOffsets.record(200 * time.Millisecond)
	e.checkRecovery(t, 15*time.Second+5*time.Millisecond, records, operations, 200, trace)
	t.Fatal("expired recovery gate unexpectedly returned")
}

func readRecoveryEvents(t *testing.T, directory string) []enduranceRecoveryObservation {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(directory, "soak.recovery.jsonl"))
	require.NoError(t, err)
	var result []enduranceRecoveryObservation
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte{'\n'}) {
		var event enduranceRecoveryObservation
		require.NoError(t, json.Unmarshal(line, &event))
		result = append(result, event)
	}
	return result
}

func TestEnduranceRecoveryRecordsHealthyBoundaryAndResets(t *testing.T) {
	// Given a recovery whose backlog remains above the unchanged20-record gate.
	e := newEnduranceEvidence(time.Minute)
	records := &streamRecords{published: 1000, acknowledged: 1000, unique: 979}
	var trace bytes.Buffer
	encoder := json.NewEncoder(&trace)
	record := func(observation enduranceRecoveryObservation) { require.NoError(t, encoder.Encode(observation)) }
	operations := soakOperationTimes{}
	e.beginRecovery(time.Now(), records, &operations, record)
	// When many checks run, then diagnostic history is streamed only at boundaries.
	for range 10000 {
		e.checkRecovery(t, time.Second, records, soakOperationTimes{}, 200, record)
	}
	require.Zero(t, e.Recoveries)
	require.Equal(t, 1, strings.Count(trace.String(), "\n"))
	records.unique = 980
	e.checkRecovery(t, 15*time.Second, records, soakOperationTimes{}, 200, record)
	// Then equality at the original deadline and backlog limit still recovers once.
	require.Equal(t, 1, e.Recoveries)
	require.Equal(t, 15.0, e.MaxRecoverySeconds)
	require.True(t, e.recoveryAt.IsZero())
	require.Equal(t, "recovered", e.Recovery.Result)
	require.Equal(t, 20, e.Recovery.Backlog)
	require.Equal(t, 2, strings.Count(trace.String(), "\n"))
	e.checkRecovery(t, 16*time.Second, records, soakOperationTimes{}, 200, record)
	require.Equal(t, 1, e.Recoveries)
	require.Equal(t, 2, strings.Count(trace.String(), "\n"))
	// A subsequent stall gets a new observation without retaining old per-check state.
	operations.Poll.record(time.Second)
	e.beginRecovery(time.Now(), records, &operations, record)
	require.Equal(t, "started", e.Recovery.Result)
	require.Equal(t, soakOperationTimes{}, e.Recovery.Operations)
	require.Equal(t, soakOperationTimes{}, operations)
}
