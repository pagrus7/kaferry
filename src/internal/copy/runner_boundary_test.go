package copy

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/src/internal/plan"
	"github.com/stretchr/testify/require"
)

func TestRunnerIgnoresLateInputAndEOFAfterStopOrRevocation(t *testing.T) {
	// Given a timestamp-stopped partition and a partition whose ownership was lost.
	r, _, sink, store, _ := fixture(t)
	upper, stamp := int64(100), int64(101)
	r.plan.Source.Kind = "TOPIC"
	r.plan.MaxOutOfOrderMS = new(int64(0))
	r.plan.Window.Upper = &plan.Expression{LiteralMS: upper}
	require.NoError(t, r.Assign([]Assignment{{Cursor: Cursor{Partition: 0, Position: 0}, Window: plan.Window{Upper: &upper}}, {Cursor: Cursor{Partition: 1, Position: 0}}}))
	trigger := incoming(0, 5, 1)
	trigger.Record.TimestampMS = &stamp
	require.NoError(t, r.input(context.Background(), trigger))
	require.NoError(t, r.Revoke([]int32{1}, true))
	beforeRecords, beforeBytes := r.tracker.Total()

	// When queued records and EOFs arrive for either partition after completion.
	for _, id := range []int32{0, 1} {
		require.NoError(t, r.input(context.Background(), incoming(id, 6, 100)))
		require.NoError(t, r.eof(Cursor{Partition: id, Position: 99}))
	}
	if records, size := r.tracker.Total(); records != beforeRecords || size != beforeBytes {
		t.Fatal("late input changed uncommitted work", records, size)
	}
	require.NoError(t, r.commit(r.ids()))

	// Then no writes or new work occur and the trigger itself remains replayable.
	if records, size := r.tracker.Total(); records != 0 || size != 0 || len(sink.writes) != 0 || !reflect.DeepEqual(store.saves, [][]Cursor{{{Partition: 0, Position: 5, Stop: &Stop{Reason: "timestamp", At: 5}}}}) {
		t.Fatal("late input moved progress or was published", store.saves, sink.writes, records, size)
	}
}

func TestRunnerRejectsNewInputWhileSubmissionIsPending(t *testing.T) {
	// Given a full destination queue holding an unsubmitted record.
	r, source, sink, store, clock := fixture(t)
	require.NoError(t, r.Assign([]Assignment{{Cursor: Cursor{Partition: 0, Position: 0}}}))
	sink.submit = func(Record, Token) error { return ErrBusy }
	require.NoError(t, r.input(context.Background(), incoming(0, 0, 10)))
	r.flow.lastCommit = clock.Now()
	require.NoError(t, r.pause())
	require.True(t, source.paused[0], "pending submission pauses intake below capacity")
	require.False(t, r.shouldCommit(), "pending submission alone does not force a save")
	pending := r.pending
	beforeRecords, beforeBytes := r.tracker.Total()

	// When a source violates pause and returns a second record.
	err := r.input(context.Background(), incoming(0, 1, 20))

	// Then the first submission is retained, neither record is saved or published,
	// and the rejected record does not enter the uncommitted-work budget.
	if records, size := r.tracker.Total(); err == nil || !strings.Contains(err.Error(), "while intake was paused") || pending == nil || r.pending != pending || records != beforeRecords || size != beforeBytes || len(sink.writes) != 0 || len(store.saves) != 0 {
		t.Fatal("paused intake replaced or advanced pending work", err, r.pending, records, size)
	}
}

func TestDrainFlushFailureCommitsOnlyConfirmedPrefix(t *testing.T) {
	// Given one confirmed write followed by one unconfirmed write.
	r, source, sink, store, _ := fixture(t)
	require.NoError(t, r.Assign([]Assignment{{Cursor: Cursor{Partition: 0, Position: 0}}}))
	for _, position := range []int64{0, 1} {
		require.NoError(t, r.input(context.Background(), incoming(0, position, 1)))
	}
	sink.reports <- Delivery{Token: sink.writes[0]}
	require.NoError(t, r.deliveries())
	failure := errors.New("destination flush failed")
	sink.flush = func() error { return failure }

	// When shutdown stops intake and the destination fails its flush.
	err := r.drain(context.Background())

	// Then the failure is reported and only the already-confirmed prefix is saved.
	if !errors.Is(err, failure) || !source.paused[0] || !reflect.DeepEqual(store.saves, [][]Cursor{{{Partition: 0, Position: 1}}}) || r.tracker.Saved(0) != 1 {
		t.Fatal("flush failure lost confirmed progress or advanced past it", err, store.saves)
	}
}

func TestRunnerReportsWriteAndFinalSaveFailures(t *testing.T) {
	// Given a confirmed write followed by a failed write, and a failing progress store.
	r, source, sink, store, _ := fixture(t)
	writeErr, saveErr := errors.New("write failed"), errors.New("final save failed")
	store.fail = map[int32]error{0: saveErr}
	for i := range 2 {
		input := incoming(0, int64(i), 1)
		source.events = append(source.events, Event{Input: &input})
	}
	source.events = append(source.events, Event{EOF: &Cursor{Partition: 0, Position: 2}})
	sink.flush = func() error {
		if len(sink.writes) == 2 {
			sink.reports <- Delivery{Token: sink.writes[0]}
			sink.reports <- Delivery{Token: sink.writes[1], Err: writeErr}
			sink.flush = nil
		}
		return nil
	}
	// When the copy fails and tries to save the confirmed prefix before returning.
	err := r.Run(context.Background())
	// Then both failures are available and no progress is reported as saved.
	var exit *Exit
	if !errors.Is(err, writeErr) || !errors.Is(err, saveErr) || !errors.As(err, &exit) || exit.Code != 1 || r.tracker.Saved(0) != 0 || !reflect.DeepEqual(store.saves, [][]Cursor{{{Partition: 0, Position: 1}}}) {
		t.Fatal("failure hid final save error or saved unsafe progress", err, store.saves)
	}
	require.EqualError(t, err, "write failed\nsave progress: final save failed")
	require.Equal(t, uint64(2), r.submitted)
	require.Equal(t, uint64(1), r.confirmed, "failed delivery was confirmed")
}

func TestRunnerPollFailureDrainsAcceptedWritesAndPreservesCause(t *testing.T) {
	// Given an accepted write and a typed source failure on the next poll.
	r, source, sink, store, clock := fixture(t)
	input := incoming(0, 0, 1)
	r.plan.Source.Kind = "TOPIC"
	r.flow.lastCommit = clock.Now()
	source.events = []Event{{Input: &input}}
	failure := operatorConflict{}
	source.pollErr = failure
	sink.flush = func() error {
		require.Equal(t, uint64(1), r.submitted)
		require.Zero(t, r.confirmed, "accepted write was already drained before the failure")
		sink.reports <- Delivery{Token: sink.writes[0]}
		sink.flush = nil
		return nil
	}

	// When polling fails after intake, then accepted output drains before exit 1.
	err := r.Run(context.Background())
	require.EqualError(t, err, "poll source: fingerprint mismatch")
	require.ErrorIs(t, err, failure)
	var original operatorConflict
	require.ErrorAs(t, err, &original)
	var exit *Exit
	require.ErrorAs(t, err, &exit)
	require.Equal(t, 1, exit.Code)
	require.Equal(t, uint64(1), r.submitted)
	require.Equal(t, r.submitted, r.confirmed)
	require.Equal(t, int64(1), r.tracker.Saved(0))
	require.Equal(t, [][]Cursor{{{Partition: 0, Position: 1}}}, store.saves)
}

func TestDrainTimeoutReportsWriteAndFinalSaveFailures(t *testing.T) {
	// Given a confirmed record, a failed record, and a write that never finishes.
	r, _, sink, store, clock := fixture(t)
	require.NoError(t, r.Assign([]Assignment{{Cursor: Cursor{Partition: 0, Position: 0}}}))
	for i := range 3 {
		require.NoError(t, r.input(context.Background(), incoming(0, int64(i), 1)))
	}
	writeErr, saveErr := errors.New("write failed"), errors.New("final save failed")
	sink.reports <- Delivery{Token: sink.writes[0]}
	sink.reports <- Delivery{Token: sink.writes[1], Err: writeErr}
	store.fail = map[int32]error{0: saveErr}
	started := clock.Now()
	// When the ten-second drain expires and saving the confirmed prefix also fails.
	err := r.exit(r.drain(context.Background()))
	// Then the runtime exit retains all causes and does not claim the prefix was saved.
	var exit *Exit
	if !errors.Is(err, writeErr) || !errors.Is(err, saveErr) || !errors.As(err, &exit) || exit.Code != 1 || !strings.Contains(err.Error(), "outstanding writes") || clock.Now().Sub(started) != 10*time.Second || r.tracker.Saved(0) != 0 || !reflect.DeepEqual(store.saves, [][]Cursor{{{Partition: 0, Position: 1}}}) {
		t.Fatal("drain timeout hid a failure or saved unsafe progress", err, store.saves)
	}
}

type failingProgress struct {
	ProgressTracker
	failure  error
	observed []Cursor
}

func (*failingProgress) Assign(Cursor) {}
func (p *failingProgress) Observe(cursor Cursor, _, _ int64) (Token, error) {
	p.observed = append(p.observed, cursor)
	return Token{}, p.failure
}
func (*failingProgress) Total() (int, int64)     { return 0, 0 }
func (*failingProgress) Work(int32) (int, int64) { return 0, 0 }
func (*failingProgress) HasProgress(int32) bool  { return false }
func (*failingProgress) InFlight() bool          { return false }
func TestRunnerStopsBeforePublishingWhenProgressRejectsInput(t *testing.T) {
	// Given
	r, source, sink, store, _ := fixture(t)
	progress := &failingProgress{failure: errors.New("invalid cursor")}
	r.tracker = progress
	input := incoming(0, 0, 1)
	source.events = []Event{{Input: &input}}

	// When
	runErr := r.Run(context.Background())

	// Then
	require.ErrorIs(t, runErr, progress.failure, "progress failure lost")
	if len(progress.observed) != 1 || len(sink.writes) != 0 || len(store.saves) != 0 {
		t.Fatal("rejected input was published or committed")
	}
}
func TestProgressFactoryDoesNotShareWorkBetweenQueries(t *testing.T) {
	// Given
	factory := NewProgressFactory()
	first, second := factory.New(), factory.New()
	first.Assign(Cursor{Partition: 0, Position: 5})

	// When
	token, err := first.Observe(Cursor{Partition: 0, Position: 5}, 6, 100)

	// Then
	require.NoError(t, err)
	if records, bytes := second.Total(); records != 0 || bytes != 0 {
		t.Fatal("query state leaked")
	}
	second.Assign(Cursor{Partition: 0, Position: 20})
	if first.Saved(0) != 5 || second.Saved(0) != 20 || !first.Settle(token, true) {
		t.Fatal("query ownership shared")
	}
}

func TestRunnerStopsBeforeNextInputWhenPeriodicCheckpointFails(t *testing.T) {
	for _, tc := range []struct{ stage, prefix string }{{"save", "save progress: "}, {"flush", ""}} {
		t.Run(tc.stage, func(t *testing.T) {
			stage := tc.stage
			// Given one confirmed record, a periodic save due, and an unread next record.
			r, source, sink, store, clock := fixture(t)
			r.plan.Source.Kind = "TOPIC"
			first, next := incoming(0, 0, 1), incoming(0, 1, 1)
			source.events = []Event{{Input: &first}, {Input: &next}}
			failure := errors.New("periodic " + stage + " failed")
			r.flow.lastCommit = clock.Now()
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			deadline := clock.Now().Add(5 * time.Second)
			clock.tick = func() {
				if !clock.Now().Before(deadline) {
					cancel()
				}
			}
			sink.submit = func(_ Record, token Token) error {
				sink.reports <- Delivery{Token: token}
				clock.now = clock.now.Add(time.Second)
				return nil
			}
			if stage == "save" {
				store.fail = map[int32]error{0: failure}
				store.onSave = func() {
					// The periodic save fails; cleanup can subsequently save the same prefix.
					store.onSave = func() { store.fail = nil }
				}
			} else {
				sink.flush = func() error {
					sink.flush = nil
					return failure
				}
			}

			// When the normal copy loop reaches its checkpoint.
			err := r.Run(ctx)

			// Then the original failure remains fatal even after cleanup saves the prefix.
			require.ErrorIs(t, err, failure)
			require.EqualError(t, err, tc.prefix+failure.Error())
			var exit *Exit
			require.ErrorAs(t, err, &exit)
			require.Equal(t, 1, exit.Code)
			require.Len(t, sink.writes, 1, "unread record was submitted after checkpoint failure")
			require.Equal(t, []Event{{Input: &next}}, source.events)
			require.Equal(t, int64(1), r.tracker.Saved(0))
			wantSaves := [][]Cursor{{{Partition: 0, Position: 1}}}
			if stage != "flush" {
				wantSaves = append(wantSaves, []Cursor{{Partition: 0, Position: 1}})
			}
			require.Equal(t, wantSaves, store.saves)
		})
	}
}

func TestRunnerSaveRequestFailureRemainsFatalAfterFinalSave(t *testing.T) {
	for _, finalSaveFails := range []bool{false, true} {
		t.Run(map[bool]string{false: "final save succeeds", true: "final save fails"}[finalSaveFails], func(t *testing.T) {
			// Given confirmed output and a request-level failure on the first periodic save.
			r, source, sink, store, clock := fixture(t)
			r.plan.Source.Kind = "TOPIC"
			first, unread := incoming(0, 0, 1), incoming(0, 1, 1)
			source.events = []Event{{Input: &first}, {Input: &unread}}
			failure := errors.New("coordinator request failed")
			finalFailure := errors.New(failure.Error())
			r.flow.lastCommit = clock.Now()
			sink.submit = func(_ Record, token Token) error {
				sink.reports <- Delivery{Token: token}
				clock.now = clock.now.Add(time.Second)
				return nil
			}
			failed := false
			r.store = saveFunc(func(ctx context.Context, cursors []Cursor) (map[int32]error, error) {
				results, err := store.Save(ctx, cursors)
				if !failed {
					failed = true
					return nil, failure
				}
				if finalSaveFails {
					return nil, finalFailure
				}
				return results, err
			})
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			deadline := clock.Now().Add(5 * time.Second)
			clock.tick = func() {
				if !clock.Now().Before(deadline) {
					cancel()
				}
			}

			// When the periodic save fails and shutdown retries the same confirmed prefix.
			err := r.Run(ctx)
			// Then exit 1 retains every save failure and never claims failed saves succeeded.
			wantError := "save progress: coordinator request failed"
			wantSaved := int64(1)
			if finalSaveFails {
				wantError += "\nsave progress: coordinator request failed"
				wantSaved = 0
				require.ErrorIs(t, err, finalFailure, "final save cause was lost")
			}
			require.EqualError(t, err, wantError)
			require.ErrorIs(t, err, failure)
			require.Equal(t, 1, exitCode(err))
			require.Equal(t, []Event{{Input: &unread}}, source.events)
			require.Equal(t, uint64(1), r.submitted)
			require.Equal(t, r.submitted, r.confirmed)
			require.Equal(t, wantSaved, r.tracker.Saved(0))
			require.Equal(t, [][]Cursor{{{Partition: 0, Position: 1}}, {{Partition: 0, Position: 1}}}, store.saves)
		})
	}
}
