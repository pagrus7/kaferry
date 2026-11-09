//go:build integration && fault && load

package compat

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/src/tests/system/faults"
	"github.com/stretchr/testify/require"
)

const brokerForcedSaveExit = "shutdown timed out while saving progress; exiting without further cleanup"
const brokerForcedWritesExit = "shutdown timed out while finishing writes; exiting without further cleanup"

func brokerForcedCopyPhase(terminal string) string {
	switch terminal {
	case brokerForcedSaveExit:
		return "saving progress"
	case brokerForcedWritesExit:
		return "finishing writes"
	default:
		return ""
	}
}

const brokerNegotiationEOF = "broker closed the connection immediately during api versions negotiation, which often happens when the broker requires TLS but the client is using plaintext: is TLS missing?"
const brokerSampledWorkScope = "sampled progress only; forced copy shutdown omits final lifetime highwater"
const brokerExactWorkScope = "exact lifetime highwater from final copy summary"

type brokerFailureProof struct {
	Case         string             `json:"case"`
	RecoveryMS   int64              `json:"recovery_milliseconds"`
	Exit         *brokerExitWitness `json:"exit_witness,omitempty"`
	Window       int                `json:"window"`
	Role         faults.Role        `json:"role"`
	Kind         string             `json:"kind"`
	Start        time.Time          `json:"start"`
	Heal         time.Time          `json:"heal"`
	ObservedExit time.Time          `json:"observed_exit_at"`
	Before       brokerObservation  `json:"before"`
	Transport    string             `json:"transport"`
	Targets      []faults.Target    `json:"targets,omitempty"`
	Timeouts     map[string]int64   `json:"timeout_milliseconds,omitempty"`
}

func (p *brokerFailureProof) affected(side string) bool {
	return p != nil && p.Window >= 0 && p.Role.Cluster == side &&
		(p.Kind == "kill" || p.Kind == "restart") && p.Transport == "plaintext" &&
		p.Before.At.Before(p.Start) && p.Start.Before(p.ObservedExit) &&
		(p.Heal.IsZero() || !p.ObservedExit.After(p.Heal)) &&
		brokerHealthyCopy(p.Before)
}

func brokerHealthyCopy(state brokerObservation) bool {
	return fullyReplicated(state, "source") && fullyReplicated(state, "destination") &&
		allAbove(state.Saved, make([]int64, replicatedPartitions), 0) &&
		allAbove(state.Publishing.Acknowledged, make([]int64, replicatedPartitions), 0) &&
		state.Pending.Submitted > 0 && state.Pending.Confirmed > 0
}

func (s *brokerCopy) activeProof(process *cliProcess) *brokerFailureProof {
	if s.active == nil || !process.finished() {
		return nil
	}
	event := s.active.Event
	var recovery time.Duration
	for _, timeout := range s.timeouts {
		recovery += timeout
	}
	proof := &brokerFailureProof{Case: s.caseID, RecoveryMS: recovery.Milliseconds(), Window: s.activeWindow, Role: event.Role, Kind: event.Fault.Kind,
		Start: event.Start, Heal: event.Heal, ObservedExit: process.stopped, Before: s.beforeFault, Transport: "plaintext"}
	if s.caseID == "broker/source-leader" || s.caseID == "broker/rolling-source" || s.caseID == "network/destination-outage" {
		proof.Targets = append([]faults.Target(nil), event.Targets...)
		proof.Timeouts = durationMilliseconds(s.timeouts)
	}
	return proof
}

func (s *brokerCopy) failureProof(process *cliProcess) *brokerFailureProof {
	proof := s.activeProof(process)
	if proof != nil && s.directory != "" {
		exit, err := s.retainExit(process)
		require.NoError(s.t, err)
		proof.Exit = exit.Witness
	}
	return proof
}

func (p *brokerFailureProof) forcedCopy(logs string) bool {
	if p == nil || p.Transport != "plaintext" || p.RecoveryMS <= 0 || !brokerHealthyCopy(p.Before) ||
		!p.Before.At.Before(p.Start) || !p.Start.Before(p.ObservedExit) || !p.Exit.binds(logs, p.Before) ||
		!p.ObservedExit.Equal(p.Exit.Stopped) || (!p.Heal.IsZero() && p.ObservedExit.After(p.Heal.Add(time.Duration(p.RecoveryMS)*time.Millisecond))) {
		return false
	}
	registered := map[string]struct{ side, role, kind string }{
		"broker/source-leader": {"source", "leader", "kill"}, "broker/destination-leader": {"destination", "leader", "kill"},
		"broker/rolling-source": {"source", "broker", "restart"}, "broker/rolling-destination": {"destination", "broker", "restart"},
	}
	rule, ok := registered[p.Case]
	active := p.Exit.Window
	return ok && p.Window >= 0 && p.Role.Cluster == rule.side && p.Role.Kind == rule.role && p.Kind == rule.kind &&
		active != nil && active.Case == p.Case && active.Window == p.Window && active.Role == p.Role && active.Kind == p.Kind &&
		active.RecoveryMS == p.RecoveryMS && active.Start.Equal(p.Start) && active.ObservedExit.Equal(p.ObservedExit) && active.Before.Worker != nil &&
		reflect.DeepEqual(active.Before, p.Before) && (active.Heal.IsZero() || active.Heal.Equal(p.Heal))
}

type brokerWorkEvidence struct {
	Summary             *copySummary
	ProgressSamples     int
	SampledPending      int
	SampledPendingBytes int64
}

// Progress reports current work, whereas only a final summary reports exact
// lifetime highwater. Missing fields and malformed complete rows are not zero.
func brokerWork(logs string) (brokerWorkEvidence, error) {
	var work brokerWorkEvidence
	for _, line := range strings.Split(logs, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "{") {
			continue
		}
		var row brokerWorkRow
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return work, fmt.Errorf("decode broker work evidence: %w", err)
		}
		if err := work.add(row); err != nil {
			return work, err
		}
	}
	if work.ProgressSamples == 0 || work.SampledPending > 1024 || work.SampledPendingBytes > 128<<20 {
		return work, fmt.Errorf("broker worker lacks valid observed current-work caps")
	}
	if work.Summary != nil && (work.Summary.PeakRecords < work.SampledPending || work.Summary.PeakBytes < work.SampledPendingBytes || work.Summary.PeakRecords > 1024 || work.Summary.PeakBytes > 128<<20) {
		return work, fmt.Errorf("broker lifetime highwater contradicts configured or observed work caps")
	}
	return work, nil
}

type brokerWorkRow struct {
	Message   string `json:"msg"`
	Submitted *int   `json:"submitted"`
	Confirmed *int   `json:"confirmed"`
	Pending   *int   `json:"pending_entries"`
	Bytes     *int64 `json:"pending_estimated_bytes"`
	Peak      *int   `json:"peak_uncommitted_records"`
	PeakBytes *int64 `json:"peak_uncommitted_bytes"`
}

func (w *brokerWorkEvidence) add(row brokerWorkRow) error {
	if row.Message != "progress" && row.Message != "copy summary" {
		return nil
	}
	if row.Submitted == nil || row.Confirmed == nil || *row.Submitted < 0 || *row.Confirmed < 0 || *row.Confirmed > *row.Submitted {
		return fmt.Errorf("broker work evidence requires complete valid counts")
	}
	if row.Message == "progress" {
		if row.Pending == nil || row.Bytes == nil || *row.Pending < 0 || *row.Bytes < 0 {
			return fmt.Errorf("broker progress requires current pending work")
		}
		w.ProgressSamples++
		w.SampledPending = max(w.SampledPending, *row.Pending)
		w.SampledPendingBytes = max(w.SampledPendingBytes, *row.Bytes)
		return nil
	}
	if w.Summary != nil || row.Peak == nil || row.PeakBytes == nil || *row.Peak < 0 || *row.PeakBytes < 0 {
		return fmt.Errorf("broker summary requires one complete lifetime highwater")
	}
	w.Summary = &copySummary{Message: row.Message, Submitted: *row.Submitted, Confirmed: *row.Confirmed, PeakRecords: *row.Peak, PeakBytes: *row.PeakBytes}
	return nil
}

func brokerContextualFailure(logs string, proof *brokerFailureProof) (string, bool) {
	var terminal []string
	for _, line := range strings.Split(logs, "\n") {
		if line = strings.TrimSpace(line); line != "" && !json.Valid([]byte(line)) {
			terminal = append(terminal, line)
		}
	}
	work, err := brokerWork(logs)
	if err != nil || len(terminal) == 0 {
		return "", false
	}
	if len(terminal) == 1 && brokerForcedCopyPhase(terminal[0]) != "" {
		return terminal[0], work.Summary == nil && strings.Contains("\n"+logs, "\n"+terminal[0]+"\n") &&
			(proof.forcedCopy(logs) || proof.networkOutageExit(logs))
	}
	if proof.sourceOperationExit(logs, strings.Join(terminal, "\n"), work.Summary) {
		return terminal[0], true
	}
	if work.Summary == nil || !proof.negotiationEOFContext(logs, len(terminal)) || len(terminal) != work.Summary.Submitted-work.Summary.Confirmed {
		return "", false
	}
	for _, line := range terminal {
		if line != brokerNegotiationEOF {
			return "", false
		}
	}
	return strings.Join(terminal, "\n"), true
}

func brokerEvidenceProof(side string) *brokerFailureProof {
	before := brokerObservation{At: time.Unix(10, 0).UTC(), Saved: make([]int64, replicatedPartitions), Topology: make(map[string][]faults.Partition), Pending: copySummary{Message: "progress", Submitted: 9, Confirmed: 7}}
	before.Publishing.Acknowledged = make([]int64, replicatedPartitions)
	for partition := range before.Saved {
		before.Saved[partition], before.Publishing.Acknowledged[partition] = 1, 1
		for _, cluster := range []string{"source", "destination"} {
			before.Topology[cluster] = append(before.Topology[cluster], faults.Partition{Leader: 1, ISR: []int32{1, 2, 3}})
		}
	}
	return &brokerFailureProof{Window: 0, Role: faults.Role{Kind: "leader", Cluster: side}, Kind: "kill", Start: time.Unix(20, 0).UTC(), Heal: time.Unix(30, 0).UTC(), ObservedExit: time.Unix(25, 0).UTC(), Before: before, Transport: "plaintext"}
}

const brokerWorkProgress = `{"msg":"progress","submitted":9,"confirmed":7,"pending_entries":2,"pending_estimated_bytes":4096}` + "\n"
const brokerWorkSummary = `{"msg":"copy summary","submitted":9,"confirmed":7,"peak_uncommitted_records":3,"peak_uncommitted_bytes":8192}` + "\n"

func TestBrokerNegotiationEOFRequiresAffectedPlainCopyAndEveryUnconfirmedError(t *testing.T) {
	// Given a verified healthy plaintext destination copy and exact final counts,
	// when every unconfirmed write returns the pinned SDK negotiation diagnostic,
	// then only that affected active destination fault can authorize restart.
	logs := brokerWorkProgress + brokerWorkSummary + strings.Repeat(brokerNegotiationEOF+"\n", 2)
	terminal, known := brokerContextualFailure(logs, brokerEvidenceProof("destination"))
	require.True(t, known)
	require.Equal(t, strings.Repeat(brokerNegotiationEOF+"\n", 2), terminal+"\n")
	for _, scenario := range []struct {
		name string
		logs string
		edit func(*brokerFailureProof)
	}{
		{name: "unknown terminal", logs: logs + "unknown failure\n"},
		{name: "different TLS diagnostic", logs: strings.ReplaceAll(logs, brokerNegotiationEOF, "TLS handshake failed")},
		{name: "missing one failure", logs: brokerWorkProgress + brokerWorkSummary + brokerNegotiationEOF + "\n"},
		{name: "missing summary", logs: brokerWorkProgress + brokerNegotiationEOF + "\n"},
		{name: "malformed summary", logs: strings.ReplaceAll(logs, `"confirmed":7`, `"confirmed":null`)},
		{name: "TLS configuration", edit: func(p *brokerFailureProof) { p.Transport = "TLS" }},
		{name: "wrong cluster", edit: func(p *brokerFailureProof) { p.Role.Cluster = "source" }},
		{name: "ISR cut", edit: func(p *brokerFailureProof) { p.Kind = "cut" }},
		{name: "startup", edit: func(p *brokerFailureProof) { p.Before.Pending.Confirmed = 0 }},
		{name: "unhealthy before", edit: func(p *brokerFailureProof) { p.Before.Topology["destination"][0].ISR = []int32{1, 2} }},
		{name: "before injection", edit: func(p *brokerFailureProof) { p.ObservedExit = p.Start.Add(-time.Second) }},
		{name: "after healing", edit: func(p *brokerFailureProof) { p.ObservedExit = p.Heal.Add(time.Second) }},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			proof := brokerEvidenceProof("destination")
			if scenario.edit != nil {
				scenario.edit(proof)
			}
			input := scenario.logs
			if input == "" {
				input = logs
			}
			_, accepted := brokerContextualFailure(input, proof)
			require.False(t, accepted)
		})
	}
}

func brokerForcedProof(side, terminal string) (*brokerFailureProof, string) {
	proof := brokerEvidenceProof(side)
	proof.Case, proof.RecoveryMS = "broker/"+side+"-leader", 119000
	identity := brokerLifetime{Number: 1, PID: 123, Started: time.Unix(1, 0).UTC(), Binary: strings.Repeat("a", 64)}
	proof.Before.Worker = &brokerProgressWitness{Lifetime: identity, Bytes: len(brokerWorkProgress), SHA256: brokerDigest(brokerWorkProgress)}
	logs := brokerWorkProgress + terminal + "\n"
	active := *proof
	proof.Exit = &brokerExitWitness{Lifetime: identity, Stopped: proof.ObservedExit, Recorded: proof.ObservedExit.Add(time.Millisecond), ExitCode: 1,
		WaitError: "exit status 1", DriverIntent: []cliSignalIntent{}, Terminal: []string{terminal}, Log: &brokerFile{Path: "worker-1.exited.cli.log", SHA256: brokerDigest(logs)}, LogBytes: len(logs), Window: &active}
	return proof, logs
}

func TestBrokerForcedCopyExitLeavesLifetimeHighwaterUnavailable(t *testing.T) {
	// Given current-lifetime healthy copying, when either documented pre-summary
	// phase forces exit 1 during a registered source or destination fault and its
	// existing recovery window, then only sampled work remains measurable.
	for _, fault := range []struct{ side, caseID, role, kind string }{
		{"source", "broker/source-leader", "leader", "kill"}, {"destination", "broker/destination-leader", "leader", "kill"},
		{"source", "broker/rolling-source", "broker", "restart"}, {"destination", "broker/rolling-destination", "broker", "restart"},
	} {
		for _, terminal := range []string{brokerForcedSaveExit, brokerForcedWritesExit} {
			proof, logs := brokerForcedProof(fault.side, terminal)
			proof.Case, proof.Kind, proof.Role.Kind = fault.caseID, fault.kind, fault.role
			active := *proof
			active.Exit = nil
			proof.Exit.Window = &active
			failure, known := brokerContextualFailure(logs, proof)
			require.True(t, known, fault.caseID+terminal)
			require.Equal(t, terminal, failure)
			_, generic := brokerTerminalFailure(logs)
			require.False(t, generic, "forced exits always require full contextual proof")
			work, err := brokerWork(logs)
			require.NoError(t, err)
			require.Nil(t, work.Summary)
			require.Equal(t, 2, work.SampledPending)
			// Physical healing does not change the declared recovery budget.
			proof.Heal = proof.ObservedExit.Add(-time.Second)
			proof.Exit.Window.Heal = proof.Heal
			_, known = brokerContextualFailure(logs, proof)
			require.True(t, known)
		}
	}
	for _, scenario := range []struct {
		name string
		edit func(*brokerFailureProof)
		logs func(string) string
	}{
		{name: "ISR", edit: func(p *brokerFailureProof) { p.Case = "broker/destination-isr-loss"; p.Kind = "cut" }},
		{name: "unknown case", edit: func(p *brokerFailureProof) { p.Case = "broker/unknown" }},
		{name: "unknown phase", logs: func(s string) string { return strings.ReplaceAll(s, "saving progress", "printing copy summary") }},
		{name: "nonliteral whitespace", logs: func(s string) string { return strings.ReplaceAll(s, brokerForcedSaveExit, " "+brokerForcedSaveExit) }},
		{name: "TLS configuration", edit: func(p *brokerFailureProof) { p.Transport = "TLS" }},
		{name: "unhealthy before", edit: func(p *brokerFailureProof) { p.Before.Topology["source"][0].ISR = []int32{1, 2} }},
		{name: "changed recovery budget", edit: func(p *brokerFailureProof) { p.RecoveryMS++ }},
		{name: "missing Wait witness", edit: func(p *brokerFailureProof) { p.Exit = nil }},
		{name: "compound terminal", logs: func(s string) string { return s + "context deadline exceeded\n" }},
		{name: "auth", logs: func(s string) string { return s + "SASL authentication failed\n" }},
		{name: "summary present", logs: func(s string) string { return brokerWorkSummary + s }},
		{name: "malformed", logs: func(s string) string { return s + "{malformed}\n" }},
		{name: "missing progress", logs: func(s string) string { return brokerForcedSaveExit + "\n" }},
		{name: "bad counts", logs: func(s string) string { return strings.ReplaceAll(s, `"pending_entries":2`, `"pending_entries":null`) }},
		{name: "work cap", logs: func(s string) string { return strings.ReplaceAll(s, `"pending_entries":2`, `"pending_entries":1025`) }},
		{name: "byte cap", logs: func(s string) string {
			return strings.ReplaceAll(s, `"pending_estimated_bytes":4096`, `"pending_estimated_bytes":134217729`)
		}},
		{name: "startup", edit: func(p *brokerFailureProof) { p.Before.Pending.Confirmed = 0 }},
		{name: "before fault", edit: func(p *brokerFailureProof) { p.ObservedExit = p.Start.Add(-time.Second) }},
		{name: "after recovery", edit: func(p *brokerFailureProof) {
			p.ObservedExit = p.Heal.Add(120 * time.Second)
			p.Exit.Stopped = p.ObservedExit
			p.Exit.Recorded = p.ObservedExit
		}},
		{name: "intentional stop", edit: func(p *brokerFailureProof) {
			p.Exit.DriverIntent = []cliSignalIntent{{At: p.Start, Signal: "interrupt", Reason: "driver"}}
		}},
		{name: "signal", edit: func(p *brokerFailureProof) { p.Exit.Signal = "killed" }},
		{name: "status", edit: func(p *brokerFailureProof) { p.Exit.ExitCode = 0 }},
		{name: "no Wait error", edit: func(p *brokerFailureProof) { p.Exit.WaitError = "" }},
		{name: "unreadable", edit: func(p *brokerFailureProof) { p.Exit.LogError = "permission denied" }},
		{name: "stale lifetime", edit: func(p *brokerFailureProof) { p.Exit.Lifetime.Number++ }},
		{name: "stale pid", edit: func(p *brokerFailureProof) { p.Exit.Lifetime.PID++ }},
		{name: "stale start", edit: func(p *brokerFailureProof) { p.Exit.Lifetime.Started = p.Start }},
		{name: "different binary", edit: func(p *brokerFailureProof) { p.Exit.Lifetime.Binary = strings.Repeat("b", 64) }},
		{name: "prefix", edit: func(p *brokerFailureProof) { p.Before.Worker.SHA256 = strings.Repeat("b", 64) }},
		{name: "missing baseline", edit: func(p *brokerFailureProof) { p.Before.Worker = nil }},
		{name: "not observed", edit: func(p *brokerFailureProof) { p.Exit.Recorded = p.Start }},
		{name: "missing active window", edit: func(p *brokerFailureProof) { p.Exit.Window = nil }},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			proof, logs := brokerForcedProof("source", brokerForcedSaveExit)
			if scenario.edit != nil {
				scenario.edit(proof)
			}
			if scenario.logs != nil {
				logs = scenario.logs(logs)
			}
			_, accepted := brokerContextualFailure(logs, proof)
			require.False(t, accepted)
		})
	}
	for _, invalid := range []string{strings.ReplaceAll(brokerWorkProgress+brokerWorkSummary, `"peak_uncommitted_records":3`, `"peak_uncommitted_records":1025`), strings.ReplaceAll(brokerWorkProgress+brokerWorkSummary, `"peak_uncommitted_bytes":8192`, `"peak_uncommitted_bytes":null`)} {
		_, err := brokerWork(invalid)
		require.Error(t, err)
	}
}
