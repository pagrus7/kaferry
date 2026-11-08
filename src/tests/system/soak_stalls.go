//go:build integration && load

package compat

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Results describe command completion or local fixture state, not broker epochs.
type stallObservation struct {
	Operation string     `json:"operation"`
	Role      string     `json:"role"`
	Started   time.Time  `json:"started_at"`
	Finished  *time.Time `json:"finished_at,omitempty"`
	Result    string     `json:"result"`
	Paused    *bool      `json:"fixture_paused,omitempty"`
	NotBefore *time.Time `json:"hold_not_before,omitempty"`
}

func stallTrace(t *testing.T) func(stallObservation) {
	t.Helper()
	directory := os.Getenv("KAFERRY_REPORT_DIR")
	if directory == "" {
		return nil
	}
	file, err := os.Create(filepath.Join(directory, "soak.stalls.jsonl"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, file.Close()) })
	encoder := json.NewEncoder(file)
	return func(event stallObservation) {
		if err := encoder.Encode(event); err != nil {
			t.Error("write stall observation:", err)
		}
	}
}

func ownStallBroker(t *testing.T, name, project, role string, record func(stallObservation)) *ownedBroker {
	t.Helper()
	if record == nil {
		return ownBroker(t, name, project, role)
	}
	event := stallObservation{Operation: "inspect ownership", Role: role, Started: time.Now(), Result: "unknown"}
	record(event)
	defer func() {
		finished := time.Now()
		event.Finished = &finished
		record(event)
	}()
	broker := ownBroker(t, name, project, role)
	event.Result = "completed"
	broker.observe = func(operation string, started, finished time.Time, err error) {
		result := "unknown"
		var end *time.Time
		if !finished.IsZero() {
			result, end = "completed", &finished
		}
		if err != nil {
			result = "failed"
		}
		record(stallObservation{Operation: operation, Role: role, Started: started, Finished: end, Result: result})
	}
	return broker
}

func (s *destinationStalls) state(operation string, observed time.Time) {
	if s.record != nil {
		paused := s.paused()
		var until *time.Time
		if !s.until.IsZero() {
			value := s.until
			until = &value
		}
		finished := time.Now()
		s.record(stallObservation{Operation: operation, Role: "destination cluster", Started: observed,
			Finished: &finished, Result: "observed", Paused: &paused, NotBefore: until})
	}
}
