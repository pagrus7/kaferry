package copy

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
)

func acknowledgeWritesOnFlush(sink *testSink) {
	acknowledged := 0
	sink.flush = func() error {
		for i := len(sink.writes) - 1; i >= acknowledged; i-- {
			sink.reports <- Delivery{Token: sink.writes[i]}
		}
		acknowledged = len(sink.writes)
		return nil
	}
}

func workerStopCursor(partition int32, position int64) Cursor {
	return Cursor{Partition: partition, Position: position, Stop: &Stop{Reason: "worker_record_cap", At: position}}
}

func TestWorkerStopCapsBatchedWritesAcrossPartitions(t *testing.T) {
	// Given two partitions and acknowledgements held until the sink flushes.
	r, source, sink, store, _ := fixture(t)
	r.plan.WorkerStopAfterRecords = 5
	observer := &recordingObserver{}
	r.observer = observer
	source.assignments = []Assignment{{Cursor: Cursor{Partition: 0}}, {Cursor: Cursor{Partition: 1}}}
	for i := range 8 {
		input := incoming(int32(i%2), int64(i/2), 1)
		source.events = append(source.events, Event{Input: &input})
	}
	acknowledgeWritesOnFlush(sink)
	// When copying with a worker-wide limit.
	err := r.Run(t.Context())
	// Then exactly five asynchronous writes finish, without reading the remaining inputs.
	require.NoError(t, err)
	require.Len(t, sink.writes, 5)
	require.EqualValues(t, 5, r.confirmed)
	require.Len(t, source.events, 3)
	require.True(t, source.paused[0])
	require.True(t, source.paused[1])
	require.EqualValues(t, 2, observer.snapshots[len(observer.snapshots)-1].WorkerRecordStops, "final metrics count both stopped partitions")
	require.Equal(t, [][]Cursor{{workerStopCursor(0, 3), workerStopCursor(1, 2)}}, store.saves,
		"save the confirmed prefix and worker stop reason for each owned partition")
}

func TestWorkerStopCountsAcrossProgressSaves(t *testing.T) {
	// Given a cap larger than the worker's uncommitted-work allowance.
	r, source, sink, store, _ := fixture(t)
	r.plan.WorkerStopAfterRecords = 5
	r.limits.RecordCap, r.limits.PartitionRecordCap = 2, 2
	for i := range 6 {
		input := incoming(0, int64(i), 1)
		source.events = append(source.events, Event{Input: &input})
	}
	acknowledgeWritesOnFlush(sink)
	// When progress is saved repeatedly before the cap is reached.
	err := r.Run(t.Context())
	// Then saves do not reset the invocation's count.
	require.NoError(t, err)
	require.Greater(t, len(store.saves), 1)
	require.Len(t, sink.writes, 5)
	require.Equal(t, []Cursor{workerStopCursor(0, 5)}, store.saves[len(store.saves)-1])
	require.Len(t, source.events, 1)
}

func TestWorkerStopPreservesSourceStopsAndMarksIdleOwnedPartitions(t *testing.T) {
	// Given a completed partition, an untouched owned partition and one writable partition.
	r, source, sink, store, _ := fixture(t)
	r.plan.Source.Kind, r.plan.WorkerStopAfterRecords = "TOPIC", 1
	require.NoError(t, source.Start(r, nil))
	r.partitions[0].stop = &Stop{Reason: "tip", At: 0}
	require.NoError(t, r.Assign([]Assignment{{Cursor: Cursor{Partition: 1}}, {Cursor: Cursor{Partition: 2}}}))
	require.NoError(t, r.input(incoming(2, 0, 1)))
	acknowledgeWritesOnFlush(sink)
	// When reaching the worker threshold.
	done, err := r.step(t.Context())
	// Then preserve source completion and save the worker reason even at an unchanged offset.
	require.NoError(t, err)
	require.True(t, done)
	require.Equal(t, [][]Cursor{{{Partition: 0, Stop: &Stop{Reason: "tip", At: 0}}, workerStopCursor(1, 0), workerStopCursor(2, 1)}}, store.saves)
}

func TestWorkerStopIgnoresFilteredSkippedAndBusyAttempts(t *testing.T) {
	// Given filtered, missing-timestamp and synchronously rejected inputs before two writes.
	r, source, sink, store, _ := fixture(t)
	r.plan.WorkerStopAfterRecords = 2
	r.plan.Policies = plan.Policies{MissingTimestamp: "SKIP", OtherRecord: "SKIP"}
	lower, before, within := int64(10), int64(0), int64(20)
	source.assignments[0].Window.Lower = &lower
	for i, stamp := range []*int64{&before, nil, &within, &within, &within, &within} {
		input := incoming(0, int64(i), 1)
		input.Record.TimestampMS = stamp
		source.events = append(source.events, Event{Input: &input})
	}
	busy := true
	sink.submit = func(_ Record, token Token) error {
		if token.Sequence == 4 {
			return &RecordError{Err: errors.New("invalid record")}
		}
		if token.Sequence == 5 && busy {
			busy = false
			return &Busy{}
		}
		return nil
	}
	acknowledgeWritesOnFlush(sink)
	// When filtering, skipping and retrying admission.
	err := r.Run(t.Context())
	// Then only the two accepted writes consume the cap, including the busy retry.
	require.NoError(t, err)
	require.Len(t, sink.writes, 2)
	require.EqualValues(t, 1, r.filtered)
	require.EqualValues(t, 2, r.skipped)
	require.False(t, busy, "the final write must retry before the cap can be reached")
	require.Equal(t, [][]Cursor{{workerStopCursor(0, 5)}}, store.saves)
	require.Len(t, source.events, 1)
}

func TestWorkerStopDoesNotConsumeEOFEvidencePastTheCap(t *testing.T) {
	// Given an input event also carrying a later EOF cursor.
	r, source, sink, store, _ := fixture(t)
	r.plan.WorkerStopAfterRecords = 1
	input := incoming(0, 0, 1)
	source.events = []Event{{Input: &input, EOF: &Cursor{Partition: 0, Position: 9}}}
	acknowledgeWritesOnFlush(sink)
	// When the record reaches the cap.
	err := r.Run(t.Context())
	// Then save its next position with the worker reason, without advancing to the EOF.
	require.NoError(t, err)
	require.Equal(t, [][]Cursor{{workerStopCursor(0, 1)}}, store.saves)
}

func TestWorkerStopAllowsEarlierSourceCompletion(t *testing.T) {
	// Given a file with fewer records than the cap.
	r, source, sink, store, _ := fixture(t)
	r.plan.WorkerStopAfterRecords = 5
	input := incoming(0, 0, 1)
	source.events = []Event{{Input: &input}, {EOF: &Cursor{Partition: 0, Position: 1}}}
	acknowledgeWritesOnFlush(sink)
	// When the file reaches its real EOF.
	err := r.Run(t.Context())
	// Then exit successfully with the genuine EOF stop, without waiting for five records.
	require.NoError(t, err)
	require.Len(t, sink.writes, 1)
	require.Equal(t, &Stop{Reason: "eof", At: 1}, store.saves[len(store.saves)-1][0].Stop)
}

func TestWorkerStopDiscardsDeferredEOFAfterAdmissionReachesCap(t *testing.T) {
	// Given a fetched record and EOF waiting for work allowance to become available.
	r, source, sink, store, _ := fixture(t)
	r.plan.WorkerStopAfterRecords = 1
	require.NoError(t, source.Start(r, nil))
	input := incoming(0, 0, 1)
	r.deferred, r.deferredEOF = &input, &Cursor{Partition: 0, Position: 9}
	acknowledgeWritesOnFlush(sink)
	// When admitting the deferred record reaches the cap during advance.
	done, err := r.step(t.Context())
	// Then ignore its deferred EOF and save only the accepted record's position.
	require.NoError(t, err)
	require.True(t, done)
	require.Equal(t, [][]Cursor{{workerStopCursor(0, 1)}}, store.saves)
}

func TestSignalAfterWorkerStopPreservesCompletionAndSaveFailures(t *testing.T) {
	for _, failSave := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "save failure"}[failSave], func(t *testing.T) {
			// Given a signal arriving immediately after the final write is accepted.
			r, source, sink, store, _ := fixture(t)
			r.plan.Source.Kind, r.plan.WorkerStopAfterRecords = "TOPIC", 1
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			input := incoming(0, 0, 1)
			source.events = []Event{{Input: &input}, {Input: &input}}
			sink.submit = func(Record, Token) error { cancel(); return nil }
			acknowledgeWritesOnFlush(sink)
			if failSave {
				store.fail = map[int32]error{0: errors.New("save failed")}
			}
			// When draining after the signal.
			err := r.Run(ctx)
			// Then an achieved cap succeeds unless writing or saving actually fails.
			if failSave {
				require.ErrorContains(t, err, "save failed")
			} else {
				require.NoError(t, err)
			}
			require.Len(t, source.events, 1)
			require.Equal(t, [][]Cursor{{workerStopCursor(0, 1)}}, store.saves)
		})
	}
}

func TestWorkerStopFailuresSaveOnlyConfirmedProgress(t *testing.T) {
	for _, failure := range []string{"delivery", "timeout", "commit"} {
		t.Run(failure, func(t *testing.T) {
			// Given a two-write limit with a failure during completion.
			r, source, sink, store, _ := fixture(t)
			r.plan.WorkerStopAfterRecords = 2
			for i := range 3 {
				input := incoming(0, int64(i), 1)
				source.events = append(source.events, Event{Input: &input})
			}
			want := failure + " failed"
			switch failure {
			case "delivery":
				sink.flush = func() error {
					if len(sink.writes) < 2 {
						return nil
					}
					sink.reports <- Delivery{Token: sink.writes[0]}
					sink.reports <- Delivery{Token: sink.writes[1], Err: errors.New(want)}
					sink.flush = nil
					return nil
				}
			case "timeout":
				want = "outstanding writes did not finish within 10 seconds"
			case "commit":
				acknowledgeWritesOnFlush(sink)
				store.fail = map[int32]error{0: errors.New(want)}
			}
			// When stopping at the cap.
			err := r.Run(t.Context())
			// Then fail without consuming a third input or saving past unconfirmed output.
			require.ErrorContains(t, err, want)
			var exit *Exit
			require.ErrorAs(t, err, &exit)
			require.Equal(t, 1, exit.Code)
			require.Len(t, sink.writes, 2)
			require.Len(t, source.events, 1)
			if failure == "timeout" {
				require.Empty(t, store.saves)
			} else {
				cursor := Cursor{Partition: 0, Position: 1}
				if failure == "commit" {
					cursor = workerStopCursor(0, 2) // The save was attempted but failed.
				}
				require.Equal(t, [][]Cursor{{cursor}}, store.saves)
			}
		})
	}
}

func TestWorkerStopDoesNotRefundAcceptedWritesAfterRevocation(t *testing.T) {
	// Given an accepted write whose assignment is lost before its acknowledgement.
	r, _, sink, store, _ := fixture(t)
	r.plan.WorkerStopAfterRecords = 2
	require.NoError(t, r.Assign([]Assignment{{Cursor: Cursor{Partition: 0}}}))
	require.NoError(t, r.input(incoming(0, 0, 1)))
	require.NoError(t, r.Revoke([]int32{0}, true))
	// When the record replays in a new assignment and reaches the cap.
	require.NoError(t, r.Assign([]Assignment{{Cursor: Cursor{Partition: 0}}}))
	require.NoError(t, r.input(incoming(0, 0, 1)))
	acknowledgeWritesOnFlush(sink)
	done, err := r.step(t.Context())
	// Then stale confirmation cannot refund the cap or advance the new assignment.
	require.True(t, done)
	require.NoError(t, err, "ownership loss retains the normal rebalance behavior")
	require.Len(t, sink.writes, 2)
	require.EqualValues(t, 1, r.confirmed)
	require.Equal(t, [][]Cursor{{workerStopCursor(0, 1)}}, store.saves)
}

func TestWorkerStopMakesTopicSignalsAndUnassignedExitBounded(t *testing.T) {
	for _, assigned := range []bool{false, true} {
		t.Run(map[bool]string{false: "unassigned", true: "quiet assigned partition"}[assigned], func(t *testing.T) {
			// Given a limited topic with no new records.
			r, source, _, _, clock := fixture(t)
			r.plan.Source.Kind, r.plan.WorkerStopAfterRecords = "TOPIC", 1
			r.limits.IdleExit = "20ms"
			ctx := t.Context()
			if !assigned {
				source.assignments = nil
			} else {
				var cancel func()
				ctx, cancel = context.WithCancel(ctx)
				t.Cleanup(cancel)
				clock.tick = func() { cancel() }
			}
			// When the unassigned timeout or a signal interrupts the unfinished copy.
			err := r.Run(ctx)
			// Then use bounded-copy exit codes rather than report success.
			var exit *Exit
			require.ErrorAs(t, err, &exit)
			want := 3
			if assigned {
				want = 1
			}
			require.Equal(t, want, exit.Code)
			require.LessOrEqual(t, clock.Now().Sub(time.Unix(100, 0)), time.Second)
		})
	}
}
