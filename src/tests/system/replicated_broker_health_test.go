//go:build integration && fault && load

package compat

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/src/tests/system/faults"
	"github.com/stretchr/testify/require"
)

const brokerHealthCaptureLimit = 64 << 10
const brokerHealthFormat = `{"Running":{{json .State.Running}},"StartedAt":{{json .State.StartedAt}},"Healthcheck":{{json .Config.Healthcheck}},"Health":{{json .State.Health}}}`

var errBrokerHealthTruncated = errors.New("docker health output exceeded bounded capture")

type brokerHealthSnapshot struct {
	Running       *bool               `json:"Running"`
	StartedAt     time.Time           `json:"StartedAt"`
	Healthcheck   json.RawMessage     `json:"Healthcheck"`
	Health        *brokerHealthStatus `json:"Health"`
	OmittedProbes int                 `json:"omitted_probe_records,omitempty"`
	ProbeHistory  string              `json:"probe_history_state"`
}

type brokerHealthStatus struct {
	Status        string              `json:"Status"`
	FailingStreak *int                `json:"FailingStreak"`
	Log           []brokerHealthProbe `json:"Log"`
}

type brokerHealthProbe struct {
	Start     time.Time `json:"Start"`
	End       time.Time `json:"End"`
	ExitCode  *int      `json:"ExitCode"`
	Output    string    `json:"Output"`
	Truncated bool      `json:"output_truncated,omitempty"`
}

// Retain the same inspect call and context; cap both streams without adding
// any shutdown allowance or a second query after a failed observation.
func brokerHealthDocker(ctx context.Context, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, "docker", args...)
	output, diagnostic := boundedAuditOutput{limit: brokerHealthCaptureLimit}, boundedAuditOutput{limit: 4096}
	command.Stdout, command.Stderr = &output, &diagnostic
	err := command.Run()
	if output.exceeded || diagnostic.exceeded {
		return output.data, errors.Join(err, errBrokerHealthTruncated)
	}
	if err != nil {
		return output.data, fmt.Errorf("inspect broker health: %w; stderr: %s", err, diagnostic.data)
	}
	return output.data, nil
}

func brokerHealthSnapshots(data []byte, expected int) ([]brokerHealthSnapshot, string, error) {
	if len(data) > brokerHealthCaptureLimit {
		return nil, "truncated", errBrokerHealthTruncated
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != expected || expected == 0 || len(data) == 0 {
		return nil, "unavailable", errors.New("docker health snapshot count differs from resolved targets")
	}
	snapshots := make([]brokerHealthSnapshot, 0, expected)
	for _, line := range lines {
		var snapshot brokerHealthSnapshot
		if err := json.Unmarshal([]byte(line), &snapshot); err != nil {
			return nil, "malformed", fmt.Errorf("decode complete Docker health snapshot: %w", err)
		}
		if len(snapshot.Healthcheck) > 4096 {
			return nil, "truncated", errors.New("rendered Docker healthcheck exceeded 4 KiB")
		}
		if err := snapshot.validate(); err != nil {
			return nil, "unavailable", err
		}
		snapshot.bound()
		snapshots = append(snapshots, snapshot)
	}
	return snapshots, "complete", nil
}

func (s *brokerHealthSnapshot) validate() error {
	var configured struct{ Test []string }
	if err := json.Unmarshal(s.Healthcheck, &configured); err != nil || len(configured.Test) == 0 || configured.Test[0] == "NONE" {
		return errors.New("docker health snapshot has no available configured probe")
	}
	if s.Running == nil || s.StartedAt.IsZero() || s.Health == nil || s.Health.FailingStreak == nil ||
		*s.Health.FailingStreak < 0 || !json.Valid(s.Healthcheck) || string(s.Healthcheck) == "null" ||
		(s.Health.Status != "starting" && s.Health.Status != "healthy" && s.Health.Status != "unhealthy") {
		return errors.New("docker health snapshot omitted required running, start, configured probe or health state")
	}
	for _, probe := range s.Health.Log {
		if probe.Start.IsZero() || probe.End.Before(probe.Start) || probe.ExitCode == nil {
			return errors.New("docker health snapshot omitted a complete retained probe outcome")
		}
	}
	return nil
}

func (s *brokerHealthSnapshot) bound() {
	s.ProbeHistory = "unavailable"
	if len(s.Health.Log) > 0 {
		s.ProbeHistory = "retained"
	}
	if len(s.Health.Log) > 5 {
		s.OmittedProbes = len(s.Health.Log) - 5
		s.Health.Log = s.Health.Log[s.OmittedProbes:]
	}
	for index := range s.Health.Log {
		probe := &s.Health.Log[index]
		probe.Truncated = len(probe.Output) > 2048
		probe.Output = brokerDiagnosticText(probe.Output)
	}
}

func brokerHealthFixture(status string) map[string]any {
	row := map[string]any{"Running": true, "StartedAt": "2026-11-08T22:55:50Z",
		"Healthcheck": map[string]any{"Test": []string{"CMD", "kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--list"},
			"Interval": int64(5 * time.Second), "Timeout": int64(5 * time.Second), "StartPeriod": int64(30 * time.Second), "StartInterval": int64(time.Second), "Retries": 18},
		"Health": map[string]any{"Status": status, "FailingStreak": 2, "Log": []map[string]any{
			{"Start": "2026-11-08T22:56:30Z", "End": "2026-11-08T22:56:31Z", "ExitCode": 1, "Output": "AdminClient connection failed"},
			{"Start": "2026-11-08T22:56:36Z", "End": "2026-11-08T22:56:41Z", "ExitCode": -1, "Output": "Health check exceeded timeout (5s)"}}}}
	if status == "healthy" {
		health := row["Health"].(map[string]any)
		health["FailingStreak"] = 0
		health["Log"] = append(health["Log"].([]map[string]any), map[string]any{"Start": "2026-11-08T22:56:46Z", "End": "2026-11-08T22:56:47Z", "ExitCode": 0, "Output": "source"})
	}
	return row
}

func TestBrokerHealthInspectionRequiresHealthyAndRetainsBoundedProbeEvidence(t *testing.T) {
	// Given Docker's health state and retained probe outcomes, when inspecting
	// once with the original context, then only Running && healthy can pass.
	for _, scenario := range []struct {
		name, status, diagnostic string
		edit                     func(map[string]any)
		raw                      []byte
		err                      error
		ready                    bool
	}{
		{name: "healthy", status: "healthy", diagnostic: "complete", ready: true},
		{name: "starting failed probes", status: "starting", diagnostic: "complete"},
		{name: "stopped", status: "healthy", diagnostic: "complete", edit: func(row map[string]any) { row["Running"] = false }},
		{name: "missing health", status: "healthy", diagnostic: "unavailable", edit: func(row map[string]any) { row["Health"] = nil }},
		{name: "missing running", status: "healthy", diagnostic: "unavailable", edit: func(row map[string]any) { delete(row, "Running") }},
		{name: "malformed", raw: []byte("{"), diagnostic: "malformed"},
		{name: "truncated capture", raw: bytes.Repeat([]byte("x"), brokerHealthCaptureLimit+1), diagnostic: "truncated"},
		{name: "inspect unavailable", status: "healthy", err: context.DeadlineExceeded, diagnostic: "unavailable"},
		{name: "capture overflow", status: "healthy", err: errBrokerHealthTruncated, diagnostic: "truncated"},
		{name: "oversized command", status: "healthy", diagnostic: "truncated", edit: func(row map[string]any) {
			row["Healthcheck"] = map[string]any{"Test": []string{"CMD", strings.Repeat("x", 4096)}}
		}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			row := brokerHealthFixture(scenario.status)
			if scenario.edit != nil {
				scenario.edit(row)
			}
			data, err := json.Marshal(row)
			require.NoError(t, err)
			if scenario.raw != nil {
				data = scenario.raw
			}
			check := brokerHealthObservation(t, data, scenario.err)
			require.Equal(t, scenario.ready, check.Passed)
			require.Equal(t, scenario.diagnostic, check.Diagnostic)
			if scenario.diagnostic != "complete" {
				require.NotEmpty(t, check.Error)
				return
			}
			detail := check.Details[0]
			wantStreak := 2
			if scenario.status == "healthy" {
				wantStreak = 0
				require.Zero(t, *detail.Health.Log[len(detail.Health.Log)-1].ExitCode)
			}
			require.Equal(t, wantStreak, *detail.Health.FailingStreak)
			require.Equal(t, 1, *detail.Health.Log[0].ExitCode)
			require.Equal(t, -1, *detail.Health.Log[1].ExitCode)
			require.Equal(t, "Health check exceeded timeout (5s)", detail.Health.Log[1].Output)
			require.Equal(t, 5*time.Second, detail.Health.Log[1].End.Sub(detail.Health.Log[1].Start))
			require.Contains(t, string(detail.Healthcheck), `"Timeout":5000000000`)
			require.Equal(t, "2026-11-08T22:55:50Z", detail.StartedAt.Format(time.RFC3339))
		})
	}
	// Then metadata retains only the latest five probes and bounds every output,
	// making omissions explicit without changing starting into a healthy result.
	row := brokerHealthFixture("starting")
	var probes []map[string]any
	for index := range 7 {
		probes = append(probes, map[string]any{"Start": "2026-11-08T22:56:36Z", "End": "2026-11-08T22:56:41Z", "ExitCode": -1, "Output": fmt.Sprint("probe ", index, " ", strings.Repeat("x", 3000))})
	}
	row["Health"].(map[string]any)["Log"] = probes
	data, err := json.Marshal(row)
	require.NoError(t, err)
	check := brokerHealthObservation(t, data, nil)
	require.False(t, check.Passed)
	require.Equal(t, "complete", check.Diagnostic)
	detail := check.Details[0]
	require.Equal(t, 2, detail.OmittedProbes)
	require.Len(t, detail.Health.Log, 5)
	require.Contains(t, detail.Health.Log[0].Output, "probe 2 ")
	for _, probe := range detail.Health.Log {
		require.True(t, probe.Truncated)
		require.LessOrEqual(t, len(probe.Output), 2060)
	}
	healthy, err := json.Marshal(brokerHealthFixture("healthy"))
	require.NoError(t, err)
	starting, err := json.Marshal(brokerHealthFixture("starting"))
	require.NoError(t, err)
	targets := []faults.Target{{Container: "owned-1"}, {Container: "owned-2"}, {Container: "owned-3"}}
	mixed := []byte(string(healthy) + "\n" + string(starting) + "\n" + string(healthy))
	check = brokerHealthObservation(t, mixed, nil, targets...)
	require.False(t, check.Passed, "every resolved broker must be healthy")
	require.Equal(t, "complete", check.Diagnostic)
	require.Len(t, check.Details, 3)
	check = brokerHealthObservation(t, healthy, nil, targets...)
	require.False(t, check.Passed)
	require.Equal(t, "unavailable", check.Diagnostic)
	row = brokerHealthFixture("healthy")
	recent := row["Health"].(map[string]any)
	recent["FailingStreak"] = 2
	recent["Log"] = recent["Log"].([]map[string]any)[:2]
	data, err = json.Marshal(row)
	require.NoError(t, err)
	check = brokerHealthObservation(t, data, nil)
	require.True(t, check.Passed, "retained probe metadata must not replace Docker's current healthy status")
	row = brokerHealthFixture("starting")
	row["Health"].(map[string]any)["FailingStreak"] = 0
	row["Health"].(map[string]any)["Log"] = nil
	data, err = json.Marshal(row)
	require.NoError(t, err)
	check = brokerHealthObservation(t, data, nil)
	require.False(t, check.Passed)
	require.Equal(t, "unavailable", check.Details[0].ProbeHistory)
	var output = boundedAuditOutput{limit: brokerHealthCaptureLimit}
	_, err = output.Write(bytes.Repeat([]byte("x"), brokerHealthCaptureLimit+1))
	require.NoError(t, err)
	require.Len(t, output.data, brokerHealthCaptureLimit)
	require.True(t, output.exceeded)
}

func brokerHealthObservation(t *testing.T, data []byte, inspectError error, targets ...faults.Target) brokerHealthCheck {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if len(targets) == 0 {
		targets = []faults.Target{{Container: "owned-container"}}
	}
	expectedArgs := []string{"inspect", "--format", brokerHealthFormat}
	for _, target := range targets {
		expectedArgs = append(expectedArgs, target.Container)
	}
	calls := 0
	s := &brokerCopy{lab: &faults.Lab{Docker: func(actual context.Context, args ...string) ([]byte, error) {
		calls++
		require.Equal(t, ctx, actual, "health diagnostics must use the same query context")
		require.Equal(t, expectedArgs, args)
		require.NotContains(t, brokerHealthFormat, ".Config.Env")
		require.NotContains(t, brokerHealthFormat, ".HostConfig")
		return data, inspectError
	}}}
	ready := s.healthy(ctx, targets...)
	require.Equal(t, 1, calls)
	require.Len(t, s.healthChecks, 1)
	require.Equal(t, ready, s.healthChecks[0].Passed)
	return s.healthChecks[0]
}
