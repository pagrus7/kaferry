//go:build integration && fault && load

package compat

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/src/tests/system/faults"
	"github.com/stretchr/testify/require"
)

type brokerLifetime struct {
	Number  int       `json:"number"`
	PID     int       `json:"pid"`
	Started time.Time `json:"started_at"`
	Binary  string    `json:"binary_sha256"`
}

type brokerProgressWitness struct {
	Lifetime brokerLifetime `json:"lifetime"`
	Bytes    int            `json:"log_prefix_bytes"`
	SHA256   string         `json:"log_prefix_sha256"`
}

type brokerFile struct {
	Path   string `json:"observations"`
	SHA256 string `json:"sha256"`
}

// This snapshot is written once before classification or resource assertions,
// including for an unsupported exit. It records observations, not RPC cause.
type brokerExitWitness struct {
	Lifetime     brokerLifetime      `json:"lifetime"`
	Stopped      time.Time           `json:"stopped_at"`
	Recorded     time.Time           `json:"recorded_at"`
	ExitCode     int                 `json:"exit_code"`
	WaitError    string              `json:"wait_error"`
	Signal       string              `json:"termination_signal"`
	DriverIntent []cliSignalIntent   `json:"driver_intent_before_wait"`
	Terminal     []string            `json:"terminal_lines"`
	Log          *brokerFile         `json:"raw_log"`
	LogBytes     int                 `json:"raw_log_bytes"`
	LogError     string              `json:"raw_log_error"`
	Window       *brokerFailureProof `json:"active_fault"`
}

type brokerRetainedExit struct {
	Witness   *brokerExitWitness
	Reference brokerFile
}

func brokerDigest(data string) string { return fmt.Sprintf("%x", sha256.Sum256([]byte(data))) }

func brokerTerminals(logs string) []string {
	lines := []string{}
	for _, line := range strings.Split(logs, "\n") {
		if line = strings.TrimSpace(line); line != "" && !json.Valid([]byte(line)) {
			lines = append(lines, line)
		}
	}
	return lines
}

func (s *brokerCopy) lifetime(process *cliProcess) brokerLifetime {
	for i, worker := range s.workers {
		if worker == process {
			return s.identities[i]
		}
	}
	return brokerLifetime{}
}

func (s *brokerCopy) currentProgress(process *cliProcess) (copySummary, *brokerProgressWitness, bool, error) {
	logs, err := process.currentLogs()
	if err != nil {
		return copySummary{}, nil, false, err
	}
	logs = logs[:strings.LastIndexByte(logs, '\n')+1]
	progress, found, err := latestCopyProgress(logs)
	return progress, &brokerProgressWitness{Lifetime: s.lifetime(process), Bytes: len(logs), SHA256: brokerDigest(logs)}, found, err
}

func writeBrokerImmutable(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(data)
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

func (s *brokerCopy) retainExit(process *cliProcess) (*brokerRetainedExit, error) {
	if old := s.exits[process]; old != nil {
		return old, nil
	}
	if !process.finished() {
		return nil, fmt.Errorf("cannot retain live broker worker exit")
	}
	w := &brokerExitWitness{Lifetime: s.lifetime(process), Stopped: process.stopped, Recorded: time.Now().UTC(),
		ExitCode: process.command.ProcessState.ExitCode(), DriverIntent: process.exitIntent,
		Window: s.activeProof(process)}
	if process.err != nil {
		w.WaitError = process.err.Error()
	}
	if status, ok := process.command.ProcessState.Sys().(syscall.WaitStatus); ok && status.Signaled() {
		w.Signal = status.Signal().String()
	}
	logs, logErr := process.currentLogs()
	if logErr != nil {
		w.LogError = logErr.Error()
	} else {
		name := fmt.Sprintf("worker-%d.exited.cli.log", w.Lifetime.Number)
		if err := writeBrokerImmutable(filepath.Join(s.directory, name), []byte(logs)); err != nil {
			return nil, err
		}
		w.Log, w.LogBytes, w.Terminal = &brokerFile{Path: name, SHA256: brokerDigest(logs)}, len(logs), brokerTerminals(logs)
	}
	data, err := json.MarshalIndent(w, "", "  ")
	if err != nil {
		return nil, err
	}
	name := fmt.Sprintf("worker-%d.lifecycle.json", w.Lifetime.Number)
	if err := writeBrokerImmutable(filepath.Join(s.directory, name), append(data, '\n')); err != nil {
		return nil, err
	}
	retained := &brokerRetainedExit{Witness: w, Reference: brokerFile{Path: name, SHA256: brokerDigest(string(append(data, '\n')))}}
	if s.exits == nil {
		s.exits = make(map[*cliProcess]*brokerRetainedExit)
	}
	s.exits[process] = retained
	return retained, nil
}

func (w *brokerExitWitness) binds(logs string, before brokerObservation) bool {
	if w == nil || w.Log == nil || w.LogError != "" || w.ExitCode != 1 || w.WaitError != "exit status 1" || w.Signal != "" || len(w.DriverIntent) != 0 ||
		w.LogBytes != len(logs) || w.Log.SHA256 != brokerDigest(logs) || before.Worker == nil {
		return false
	}
	prefix := before.Worker
	if prefix.Lifetime != w.Lifetime || w.Lifetime.Number <= 0 || w.Lifetime.PID <= 0 || len(w.Lifetime.Binary) != 64 ||
		!w.Lifetime.Started.Before(before.At) || !before.At.Before(w.Stopped) || w.Recorded.Before(w.Stopped) ||
		prefix.Bytes <= 0 || prefix.Bytes > len(logs) || brokerDigest(logs[:prefix.Bytes]) != prefix.SHA256 || logs[prefix.Bytes-1] != '\n' {
		return false
	}
	progress, found, err := latestCopyProgress(logs[:prefix.Bytes])
	return err == nil && found && progress == before.Pending && strings.Join(w.Terminal, "\n") == strings.Join(brokerTerminals(logs), "\n")
}

func TestBrokerExitWitnessRetainsUnsupportedAndIntentionalExitsBeforeAssertions(t *testing.T) {
	for _, terminal := range []string{brokerForcedSaveExit, brokerForcedWritesExit} {
		t.Run(terminal, func(t *testing.T) {
			// Given complete progress from this actual live lifetime, when its
			// documented forced exit follows injection, then restart uses the
			// immutable witness rather than a prior worker's healthy counters.
			process := brokerLogChild(t, terminal)
			binary, err := os.ReadFile(process.command.Path)
			require.NoError(t, err)
			s := &brokerCopy{t: t, caseID: "broker/destination-leader", directory: t.TempDir(), workers: []*cliProcess{process}, identities: []brokerLifetime{{Number: 1, PID: process.pid(), Started: time.Now().UTC().Add(-time.Second), Binary: brokerDigest(string(binary))}}}
			s.timeouts = faultTimeouts(t, s.caseID)
			s.beforeFault = brokerEvidenceProof("destination").Before
			progress, prefix, found, err := s.currentProgress(process)
			require.NoError(t, err)
			require.True(t, found)
			s.beforeFault.At, s.beforeFault.Pending, s.beforeFault.Worker = time.Now().UTC(), progress, prefix
			s.active = &faults.Injection{Event: faults.Event{Role: faults.Role{Kind: "leader", Cluster: "destination"}, Fault: faults.Fault{Kind: "kill"}, Start: s.beforeFault.At.Add(time.Nanosecond)}}
			require.NoError(t, process.stdin.Close())
			require.Error(t, process.wait())
			proof := s.failureProof(process)
			logs, err := process.currentLogs()
			require.NoError(t, err)
			require.True(t, proof.Exit.binds(logs, proof.Before), "raw lifecycle must bind the observed current progress: %+v", proof.Exit)
			require.True(t, proof.forcedCopy(logs), "registered active fault must bind: %+v", proof)
			failure, err := completedBrokerFailure(process, true, proof)
			require.NoError(t, err)
			require.Equal(t, terminal, failure)
			_, err = completedBrokerFailure(process, false, proof)
			require.Error(t, err, "ISR keeps its no-restart gate")
			require.NotNil(t, s.exits[process], "exit witness is retained before summary checks")
		})
	}
	for _, intentional := range []bool{false, true} {
		t.Run(fmt.Sprint(intentional), func(t *testing.T) {
			// Given a real live worker, when it exits with an unsupported terminal
			// or a driver signal, then forensic evidence precedes classification.
			process := brokerLogChild(t, "unexpected invariant failure")
			s := &brokerCopy{t: t, directory: t.TempDir(), workers: []*cliProcess{process}, identities: []brokerLifetime{{Number: 1, PID: process.pid(), Started: time.Now().UTC()}}}
			if intentional {
				require.NoError(t, process.signal(os.Interrupt, "regression stop"))
			} else {
				require.NoError(t, process.stdin.Close())
			}
			require.Error(t, process.wait())
			exit, err := s.retainExit(process)
			require.NoError(t, err)
			require.Equal(t, process.stopped, exit.Witness.Stopped)
			if intentional {
				require.Len(t, exit.Witness.DriverIntent, 1)
				require.NotEmpty(t, exit.Witness.Signal)
			} else {
				require.Equal(t, []string{"unexpected invariant failure"}, exit.Witness.Terminal)
			}
			_, err = completedBrokerFailure(process, true)
			require.Error(t, err, "unsupported failure cannot reach replacement")
			original, err := os.ReadFile(filepath.Join(s.directory, exit.Reference.Path))
			require.NoError(t, err)
			require.Equal(t, exit.Reference.SHA256, brokerDigest(string(original)))
			raw, err := os.ReadFile(filepath.Join(s.directory, exit.Witness.Log.Path))
			require.NoError(t, err)
			require.Equal(t, exit.Witness.Log.SHA256, brokerDigest(string(raw)))
			// Later cleanup/read failures cannot rewrite the original witness.
			process.output += "-missing"
			again, err := s.retainExit(process)
			require.NoError(t, err)
			require.Same(t, exit, again)
			require.ErrorIs(t, writeBrokerImmutable(filepath.Join(s.directory, exit.Reference.Path), []byte("replacement")), os.ErrExist)
			retained, err := os.ReadFile(filepath.Join(s.directory, exit.Reference.Path))
			require.NoError(t, err)
			require.Equal(t, original, retained)
		})
	}
	// An unreadable final log still leaves an explicit immutable failure record.
	process := brokerLogChild(t, "unexpected invariant failure")
	require.NoError(t, process.stdin.Close())
	require.Error(t, process.wait())
	process.output += "-missing"
	s := &brokerCopy{directory: t.TempDir(), workers: []*cliProcess{process}, identities: []brokerLifetime{{Number: 1, PID: process.pid(), Started: process.stopped.Add(-time.Second)}}}
	exit, err := s.retainExit(process)
	require.NoError(t, err)
	require.NotEmpty(t, exit.Witness.LogError)
	require.Nil(t, exit.Witness.Log)
}
