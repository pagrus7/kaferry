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

const brokerSampledWorkScope = "sampled progress only; final lifetime highwater unavailable"
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
		Start: event.Start, Heal: event.Heal, ObservedExit: process.stopped, Before: s.beforeFault}
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

func (p *brokerFailureProof) restartScope(logs string) bool {
	if p == nil || p.RecoveryMS <= 0 || !brokerHealthyCopy(p.Before) ||
		!p.Before.At.Before(p.Start) || !p.Start.Before(p.ObservedExit) || !p.Exit.binds(logs, p.Before) ||
		!p.ObservedExit.Equal(p.Exit.Stopped) || (!p.Heal.IsZero() && p.ObservedExit.After(p.Heal.Add(time.Duration(p.RecoveryMS)*time.Millisecond))) {
		return false
	}
	registered := map[string]struct{ side, role, kind string }{
		"broker/source-leader": {"source", "leader", "kill"}, "broker/destination-leader": {"destination", "leader", "kill"},
		"broker/rolling-source": {"source", "broker", "restart"}, "broker/rolling-destination": {"destination", "broker", "restart"},
	}
	rule, ok := registered[p.Case]
	if p.Case == "network/destination-outage" {
		rule.side, rule.role, rule.kind, ok = "destination", "worker-destination", "cut", true
		rss := p.Exit.RSS
		if p.Window != 0 || p.Role.Worker != 1 || p.Exit.Lifetime.Number != 1 ||
			(!p.Heal.IsZero() && !p.ObservedExit.Before(p.Heal)) ||
			rss == nil || rss.Samples <= 0 || rss.Failures != 0 || rss.Peak <= 0 || rss.Peak > 512<<20 {
			return false
		}
	}
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

// Exit codes and Kaferry operation labels identify a failed copy; dependency
// diagnostics are retained verbatim without interpreting their wording or order.
func brokerContextualFailure(logs string, proof *brokerFailureProof) (string, bool) {
	lines := brokerTerminals(logs)
	terminal := strings.Join(lines, "\n")
	work, err := brokerWork(logs)
	if err != nil || terminal == "" || !proof.restartScope(logs) {
		return terminal, false
	}
	labelled := false
	for _, line := range lines {
		labelled = labelled || strings.HasPrefix(line, "poll source: ") || strings.HasPrefix(line, "save progress: ") ||
			strings.HasPrefix(line, "shutdown timed out while ")
	}
	unconfirmed := work.Summary != nil && work.Summary.Submitted > work.Summary.Confirmed
	return terminal, labelled || unconfirmed
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
	return &brokerFailureProof{Window: 0, Role: faults.Role{Kind: "leader", Cluster: side}, Kind: "kill", Start: time.Unix(20, 0).UTC(), Heal: time.Unix(30, 0).UTC(), ObservedExit: time.Unix(25, 0).UTC(), Before: before}
}

const brokerWorkProgress = `{"msg":"progress","submitted":9,"confirmed":7,"pending_entries":2,"pending_estimated_bytes":4096}` + "\n"
const brokerWorkSummary = `{"msg":"copy summary","submitted":9,"confirmed":7,"peak_uncommitted_records":3,"peak_uncommitted_bytes":8192}` + "\n"

func TestBrokerWorkCapsRequireActualSamplesAndKeepMissingPeaksUnavailable(t *testing.T) {
	// Given a real progress sample, when no final summary exists, then observed
	// work remains bounded and the missing lifetime highwater stays unavailable.
	work, err := brokerWork(brokerWorkProgress)
	require.NoError(t, err)
	require.Nil(t, work.Summary)
	require.Equal(t, 2, work.SampledPending)
	// When the final summary exists, then its exact peaks must cover the samples
	// and remain within the configured record and byte caps.
	work, err = brokerWork(brokerWorkProgress + brokerWorkSummary)
	require.NoError(t, err)
	require.Equal(t, 3, work.Summary.PeakRecords)
	for _, invalid := range []string{
		brokerWorkSummary,
		strings.ReplaceAll(brokerWorkProgress, `"pending_entries":2`, `"pending_entries":1025`),
		strings.ReplaceAll(brokerWorkProgress, `"pending_estimated_bytes":4096`, `"pending_estimated_bytes":134217729`),
		brokerWorkProgress + strings.ReplaceAll(brokerWorkSummary, `"peak_uncommitted_records":3`, `"peak_uncommitted_records":1`),
		brokerWorkProgress + strings.ReplaceAll(brokerWorkSummary, `"peak_uncommitted_bytes":8192`, `"peak_uncommitted_bytes":null`),
	} {
		_, err := brokerWork(invalid)
		require.Error(t, err)
	}
}
