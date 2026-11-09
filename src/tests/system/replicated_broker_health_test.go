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

	"github.com/stretchr/testify/require"
)

const brokerHealthCaptureLimit = 64 << 10
const brokerHealthFormat = `{"ID":{{json .Id}},"Project":{{json (index .Config.Labels "com.docker.compose.project")}},"Service":{{json (index .Config.Labels "com.docker.compose.service")}},"Running":{{json .State.Running}},"StartedAt":{{json .State.StartedAt}},"Healthcheck":{{json .Config.Healthcheck}},"Health":{{json .State.Health}}}`

var errBrokerHealthTruncated = errors.New("docker health output exceeded bounded capture")

type brokerHealthSnapshot struct {
	ID            string              `json:"ID"`
	Project       string              `json:"Project"`
	Service       string              `json:"Service"`
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

func TestBrokerHealthInspectionRetainsBoundedProbeEvidence(t *testing.T) {
	// Given Docker's selected state, when decoding bounded diagnostics, then
	// malformed evidence fails and current health/probe outcomes remain distinct.
	for _, scenario := range []struct {
		name, status, diagnostic string
		edit                     func(map[string]any)
		raw                      []byte
	}{
		{name: "healthy", status: "healthy", diagnostic: "complete"},
		{name: "starting failed probes", status: "starting", diagnostic: "complete"},
		{name: "stopped", status: "healthy", diagnostic: "complete", edit: func(row map[string]any) { row["Running"] = false }},
		{name: "missing health", status: "healthy", diagnostic: "unavailable", edit: func(row map[string]any) { row["Health"] = nil }},
		{name: "missing running", status: "healthy", diagnostic: "unavailable", edit: func(row map[string]any) { delete(row, "Running") }},
		{name: "malformed", raw: []byte("{"), diagnostic: "malformed"},
		{name: "truncated capture", raw: bytes.Repeat([]byte("x"), brokerHealthCaptureLimit+1), diagnostic: "truncated"},
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
			details, diagnostic, err := brokerHealthSnapshots(data, 1)
			require.Equal(t, scenario.diagnostic, diagnostic)
			if diagnostic != "complete" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, scenario.status, details[0].Health.Status)
			require.Equal(t, -1, *details[0].Health.Log[1].ExitCode)
			require.Equal(t, "Health check exceeded timeout (5s)", details[0].Health.Log[1].Output)
			require.Contains(t, string(details[0].Healthcheck), `"Timeout":5000000000`)
		})
	}
	row := brokerHealthFixture("starting")
	var probes []map[string]any
	for index := range 7 {
		probes = append(probes, map[string]any{"Start": "2026-11-08T22:56:36Z", "End": "2026-11-08T22:56:41Z", "ExitCode": -1, "Output": fmt.Sprint("probe ", index, " ", strings.Repeat("x", 3000))})
	}
	row["Health"].(map[string]any)["Log"] = probes
	data, err := json.Marshal(row)
	require.NoError(t, err)
	details, _, err := brokerHealthSnapshots(data, 1)
	require.NoError(t, err)
	require.Equal(t, 2, details[0].OmittedProbes)
	require.Len(t, details[0].Health.Log, 5)
	for _, probe := range details[0].Health.Log {
		require.True(t, probe.Truncated)
		require.LessOrEqual(t, len(probe.Output), 2060)
	}
	_, _, err = brokerHealthSnapshots(data, 3)
	require.Error(t, err)
	output := boundedAuditOutput{limit: brokerHealthCaptureLimit}
	_, err = output.Write(bytes.Repeat([]byte("x"), brokerHealthCaptureLimit+1))
	require.NoError(t, err)
	require.True(t, output.exceeded)
}
