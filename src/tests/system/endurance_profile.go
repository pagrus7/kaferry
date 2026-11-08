//go:build integration && load

package compat

import (
	"fmt"
	"time"
)

// This fixed workload exercises a single process over many active partitions.
// It is deliberately separate from the short CI soak and saturation tests.
type enduranceEvidence struct {
	Profile                        string  `json:"profile"`
	Processes                      int     `json:"processes"`
	Routing                        string  `json:"routing"`
	Topology                       string  `json:"topology"`
	ReplicationFactor              int     `json:"replication_factor"`
	MinInsyncReplicas              int     `json:"min_insync_replicas,omitempty"`
	PayloadBytes                   [2]int  `json:"payload_bytes_min_max"`
	PayloadEntropy                 string  `json:"payload_entropy"`
	KeyDistribution                string  `json:"key_distribution"`
	TargetRate                     int     `json:"target_records_per_second"`
	TargetRecords                  int     `json:"target_records"`
	SubmittedAtEnd                 int     `json:"submitted_at_publish_end"`
	AcknowledgedAtEnd              int     `json:"acknowledged_at_publish_end"`
	AcknowledgedAfterGrace         int     `json:"acknowledged_after_grace"`
	Windows                        int     `json:"rate_windows"`
	MinWindowRatio                 float64 `json:"min_acknowledged_window_ratio"`
	ProducerGates                  int     `json:"producer_gate_iterations"`
	VerifierGates                  int     `json:"verifier_gate_iterations"`
	MaxDebt                        int     `json:"max_scheduling_debt_records"`
	Recoveries                     int     `json:"recoveries"`
	MaxRecoverySeconds             float64 `json:"max_recovery_seconds"`
	SamplingSeconds                float64 `json:"sampling_seconds"`
	VerifierSeconds                float64 `json:"verifier_seconds"`
	ProducerSeconds                float64 `json:"producer_seconds"`
	LoadValidated                  bool    `json:"load_validated"`
	LoadedFinalMedianRSSBytes      int64   `json:"loaded_final_median_rss_bytes"`
	lastAt                         time.Duration
	lastExpected, lastAcknowledged int
	endRecorded, graceChecked      bool
	recoveryAt                     time.Time
}

func newEnduranceEvidence(duration time.Duration) *enduranceEvidence {
	return &enduranceEvidence{Profile: "application-24x32-v1", Processes: 1, Routing: "KEY", Topology: "single", ReplicationFactor: 1,
		PayloadBytes: [2]int{1024, 5119}, PayloadEntropy: "deterministic PCG bytes; first four bytes encode ID",
		KeyDistribution: "ID modulo 257; every seventh key is null",
		TargetRate:      200, TargetRecords: enduranceScheduled(duration), MinWindowRatio: 1}
}

// Schedule from elapsed time, never from verifier or producer completion.
func enduranceScheduled(elapsed time.Duration) int {
	bursts := 0
	if elapsed >= 10*time.Second {
		bursts = 1 + int((elapsed-10*time.Second)/time.Minute)
	}
	return int(elapsed*200/time.Second) + bursts*400
}

func (e *enduranceEvidence) check(elapsed, duration time.Duration, submitted, acknowledged int) error {
	at := min(elapsed, duration)
	expected := enduranceScheduled(at)
	e.MaxDebt = max(e.MaxDebt, expected-submitted)
	if expected-submitted > 1400 {
		return fmt.Errorf("endurance load invalid: scheduling debt exceeds five seconds plus one burst")
	}
	if !e.endRecorded && elapsed >= duration {
		e.SubmittedAtEnd, e.AcknowledgedAtEnd, e.endRecorded = submitted, acknowledged, true
	}
	// The final window includes records due exactly at the publishing boundary.
	// Let those records settle during grace before judging that window.
	if (at < duration && at-e.lastAt >= time.Minute) || (elapsed >= duration+5*time.Second && e.lastAt < duration) {
		planned := expected - e.lastExpected
		ratio := float64(acknowledged-e.lastAcknowledged) / float64(planned)
		e.Windows++
		e.MinWindowRatio = min(e.MinWindowRatio, ratio)
		e.lastAt, e.lastExpected, e.lastAcknowledged = at, expected, acknowledged
		if ratio < .95 {
			return fmt.Errorf("endurance load invalid: acknowledged window achieved %.3f of scheduled input (need 0.95)", ratio)
		}
	}
	if !e.graceChecked && elapsed >= duration+5*time.Second {
		e.AcknowledgedAfterGrace, e.graceChecked = acknowledged, true
		e.LoadValidated = submitted == e.TargetRecords && acknowledged == e.TargetRecords
		if !e.LoadValidated {
			return fmt.Errorf("endurance load invalid: all scheduled input must be submitted and acknowledged within five seconds of publishing end")
		}
	}
	return nil
}
