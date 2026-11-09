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

// Operation times include waits in the fixture; they do not isolate copier cost.
type soakOperationTime struct {
	Count   int64         `json:"count"`
	Last    time.Duration `json:"last_nanoseconds"`
	Maximum time.Duration `json:"max_nanoseconds"`
	Total   time.Duration `json:"total_nanoseconds"`
}

func (o *soakOperationTime) record(elapsed time.Duration) {
	o.Count++
	o.Last = elapsed
	o.Maximum = max(o.Maximum, elapsed)
	o.Total += elapsed
}

type soakOperationTimes struct {
	Poll            soakOperationTime `json:"verifier_poll"`
	Verify          soakOperationTime `json:"verification_and_report_drain"`
	DestinationEnds soakOperationTime `json:"destination_end_offsets"`
	RSS             soakOperationTime `json:"rss"`
	SavedOffsets    soakOperationTime `json:"saved_offsets"`
}

type enduranceRecoveryObservation struct {
	Result       string             `json:"result"`
	Started      time.Time          `json:"started_at"`
	Deadline     time.Time          `json:"deadline"`
	Checked      time.Time          `json:"checked_at"`
	Elapsed      time.Duration      `json:"elapsed_nanoseconds"`
	Submitted    int                `json:"submitted"`
	Acknowledged int                `json:"acknowledged"`
	Unique       int                `json:"unique_received"`
	Backlog      int                `json:"backlog"`
	Observed     *observedDelivery  `json:"observed,omitempty"`
	Operations   soakOperationTimes `json:"operations"`
}

func recoveryTrace(t *testing.T) func(enduranceRecoveryObservation) {
	t.Helper()
	directory := os.Getenv("KAFERRY_REPORT_DIR")
	if directory == "" {
		return nil
	}
	file, err := os.Create(filepath.Join(directory, "soak.recovery.jsonl"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, file.Close()) })
	encoder := json.NewEncoder(file)
	return func(event enduranceRecoveryObservation) {
		if err := encoder.Encode(event); err != nil {
			t.Error("write recovery observation:", err)
		}
	}
}

func (e *enduranceEvidence) beginRecovery(started time.Time, records *streamRecords, operations *soakOperationTimes, trace func(enduranceRecoveryObservation)) {
	e.recoveryAt = started
	*operations = soakOperationTimes{}
	e.observeRecovery("started", started, 0, records, *operations, trace)
}

func (e *enduranceEvidence) observeRecovery(result string, checked time.Time, elapsed time.Duration, records *streamRecords, operations soakOperationTimes, trace func(enduranceRecoveryObservation)) {
	observation := enduranceRecoveryObservation{Result: result, Started: e.recoveryAt,
		Deadline: e.recoveryAt.Add(15 * time.Second), Checked: checked, Elapsed: elapsed,
		Submitted: records.published, Acknowledged: records.acknowledged, Unique: records.unique,
		Backlog: records.acknowledged - records.unique, Operations: operations}
	if records.observer != nil {
		observed := records.observer.snapshot()
		observation.Observed = &observed
	}
	e.Recovery = &observation
	if trace != nil {
		trace(observation)
	}
}

func (e *enduranceEvidence) checkRecovery(t *testing.T, elapsed time.Duration, records *streamRecords, operations soakOperationTimes, rate int, trace func(enduranceRecoveryObservation)) {
	t.Helper()
	if e.recoveryAt.IsZero() {
		return
	}
	checked := e.recoveryAt.Add(elapsed)
	if elapsed > 15*time.Second {
		e.observeRecovery("failed", checked, elapsed, records, operations, trace)
	}
	require.LessOrEqual(t, elapsed, 15*time.Second, "backlog failed to recover after destination stall")
	if records.acknowledged-records.unique <= rate/10 {
		e.observeRecovery("recovered", checked, elapsed, records, operations, trace)
		e.MaxRecoverySeconds = max(e.MaxRecoverySeconds, elapsed.Seconds())
		e.Recoveries++
		e.recoveryAt = time.Time{}
	}
}
