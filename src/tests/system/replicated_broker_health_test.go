//go:build integration && fault && load

package compat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

const brokerHealthCaptureLimit = 64 << 10
const brokerHealthFormat = `{"ID":{{json .Id}},"Project":{{json (index .Config.Labels "com.docker.compose.project")}},"Service":{{json (index .Config.Labels "com.docker.compose.service")}},"Running":{{json .State.Running}},"StartedAt":{{json .State.StartedAt}}}`

var errBrokerHealthTruncated = errors.New("docker health output exceeded bounded capture")

type brokerHealthSnapshot struct {
	ID        string    `json:"ID"`
	Project   string    `json:"Project"`
	Service   string    `json:"Service"`
	Running   *bool     `json:"Running"`
	StartedAt time.Time `json:"StartedAt"`
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

func brokerHealthSnapshots(data []byte, expected int) ([]brokerHealthSnapshot, error) {
	if len(data) > brokerHealthCaptureLimit {
		return nil, errBrokerHealthTruncated
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != expected || expected == 0 || len(data) == 0 {
		return nil, errors.New("docker state count differs from resolved targets")
	}
	snapshots := make([]brokerHealthSnapshot, 0, expected)
	for _, line := range lines {
		var snapshot brokerHealthSnapshot
		if err := json.Unmarshal([]byte(line), &snapshot); err != nil {
			return nil, fmt.Errorf("decode Docker broker state: %w", err)
		}
		if snapshot.Running == nil || snapshot.StartedAt.IsZero() {
			return nil, errors.New("docker broker state omitted running or start time")
		}
		snapshots = append(snapshots, snapshot)
	}
	return snapshots, nil
}
