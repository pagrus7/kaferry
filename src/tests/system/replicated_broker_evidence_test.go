//go:build integration && fault && load

package compat

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/src/tests/system/faults"
	"github.com/stretchr/testify/require"
)

const brokerForcedSaveExit = "shutdown timed out while saving progress; exiting without further cleanup"
const brokerNegotiationEOF = "broker closed the connection immediately during api versions negotiation, which often happens when the broker requires TLS but the client is using plaintext: is TLS missing?"
const brokerSampledWorkScope = "sampled progress only; forced save exit omits final lifetime highwater"
const brokerExactWorkScope = "exact lifetime highwater from final copy summary"

type brokerFailureProof struct {
	Window       int               `json:"window"`
	Role         faults.Role       `json:"role"`
	Kind         string            `json:"kind"`
	Start        time.Time         `json:"start"`
	Heal         time.Time         `json:"heal"`
	ObservedExit time.Time         `json:"observed_exit_at"`
	Before       brokerObservation `json:"before"`
	Transport    string            `json:"transport"`
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

func (s *brokerCopy) failureProof(process *cliProcess) *brokerFailureProof {
	if s.active == nil || !process.finished() {
		return nil
	}
	event := s.active.Event
	return &brokerFailureProof{Window: s.activeWindow, Role: event.Role, Kind: event.Fault.Kind,
		Start: event.Start, Heal: event.Heal, ObservedExit: process.stopped, Before: s.beforeFault, Transport: "plaintext"}
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
	if len(terminal) == 1 && terminal[0] == brokerForcedSaveExit {
		return terminal[0], work.Summary == nil && proof.affected("source")
	}
	if work.Summary == nil || !proof.affected("destination") || len(terminal) != work.Summary.Submitted-work.Summary.Confirmed {
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
	before := brokerObservation{At: time.Unix(10, 0).UTC(), Saved: make([]int64, replicatedPartitions), Topology: make(map[string][]faults.Partition), Pending: copySummary{Submitted: 9, Confirmed: 7}}
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

func TestBrokerForcedSaveExitLeavesLifetimeHighwaterUnavailable(t *testing.T) {
	// Given the documented os.Exit path after healthy source copying, when the
	// literal forced-save diagnostic occurs during source loss, then progress
	// remains sampled evidence and no exact lifetime highwater is fabricated.
	logs := brokerWorkProgress + brokerForcedSaveExit + "\n"
	terminal, known := brokerContextualFailure(logs, brokerEvidenceProof("source"))
	require.True(t, known)
	require.Equal(t, brokerForcedSaveExit, terminal)
	_, generic := brokerTerminalFailure(logs)
	require.False(t, generic, "the diagnostic requires its source fault proof")
	work, err := brokerWork(logs)
	require.NoError(t, err)
	require.Nil(t, work.Summary)
	require.Equal(t, 1, work.ProgressSamples)
	require.Equal(t, 2, work.SampledPending)
	require.EqualValues(t, 4096, work.SampledPendingBytes)
	process := brokerLogChild(t, brokerForcedSaveExit)
	require.NoError(t, process.stdin.Close())
	require.Error(t, process.wait())
	liveProof := brokerEvidenceProof("source")
	liveProof.Start, liveProof.Heal, liveProof.ObservedExit = process.stopped.Add(-time.Second), process.stopped.Add(time.Second), process.stopped
	_, err = completedBrokerFailure(process, true, liveProof)
	require.NoError(t, err, "the real exit 1 has no deferred summary")
	_, err = completedBrokerFailure(process, false, liveProof)
	require.Error(t, err, "ISR still forbids restart")
	for _, invalid := range []string{
		brokerForcedSaveExit + "\n", brokerWorkProgress + "unknown forced exit\n",
		brokerWorkProgress + brokerWorkSummary + brokerForcedSaveExit + "\n",
		strings.ReplaceAll(logs, `"pending_entries":2`, `"pending_entries":null`),
		strings.ReplaceAll(logs, `"pending_entries":2`, `"pending_entries":1025`),
		strings.ReplaceAll(logs, `"pending_estimated_bytes":4096`, `"pending_estimated_bytes":134217729`),
		logs + "{malformed}\n",
	} {
		_, accepted := brokerContextualFailure(invalid, brokerEvidenceProof("source"))
		require.False(t, accepted, invalid)
	}
	for _, proof := range []*brokerFailureProof{nil, brokerEvidenceProof("destination")} {
		_, accepted := brokerContextualFailure(logs, proof)
		require.False(t, accepted)
	}
	proof := brokerEvidenceProof("source")
	proof.ObservedExit = proof.Heal.Add(time.Second)
	_, accepted := brokerContextualFailure(logs, proof)
	require.False(t, accepted)
	// Normal exits still require the exact summary and its full cap checks.
	for _, invalid := range []string{strings.ReplaceAll(brokerWorkProgress+brokerWorkSummary, `"peak_uncommitted_records":3`, `"peak_uncommitted_records":1025`), strings.ReplaceAll(brokerWorkProgress+brokerWorkSummary, `"peak_uncommitted_bytes":8192`, `"peak_uncommitted_bytes":null`)} {
		_, err = brokerWork(invalid)
		require.Error(t, err)
	}
}
