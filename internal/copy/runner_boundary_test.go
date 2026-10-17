package copy

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/plan"
)

func TestRunnerIgnoresLateInputAndEOFAfterStopOrRevocation(t *testing.T) {
	// Given a timestamp-stopped partition and a partition whose ownership was lost.
	r, _, sink, store, _ := fixture(t)
	upper, stamp := int64(100), int64(101)
	r.plan.Source.Kind = "TOPIC"
	r.plan.Window.Upper = &plan.Expression{LiteralMS: upper}
	if err := r.Assign([]Assignment{{Cursor: Cursor{Partition: 0, Position: 0}, Window: plan.Window{Upper: &upper}}, {Cursor: Cursor{Partition: 1, Position: 0}}}); err != nil {
		t.Fatal(err)
	}
	trigger := incoming(0, 5, 1)
	trigger.Record.TimestampMS = &stamp
	if err := r.input(trigger); err != nil {
		t.Fatal(err)
	}
	if err := r.Revoke([]int32{1}, true); err != nil {
		t.Fatal(err)
	}
	beforeRecords, beforeBytes := r.tracker.Total()

	// When queued records and EOFs arrive for either partition after completion.
	for _, id := range []int32{0, 1} {
		if err := r.input(incoming(id, 6, 100)); err != nil {
			t.Fatal(err)
		}
		if err := r.eof(Cursor{Partition: id, Position: 99}); err != nil {
			t.Fatal(err)
		}
	}
	if records, size := r.tracker.Total(); records != beforeRecords || size != beforeBytes {
		t.Fatal("late input changed uncommitted work", records, size)
	}
	if err := r.commit(r.ids()); err != nil {
		t.Fatal(err)
	}

	// Then no writes or new work occur and the trigger itself remains replayable.
	if records, size := r.tracker.Total(); records != 0 || size != 0 || len(sink.writes) != 0 || !reflect.DeepEqual(store.saves, [][]Cursor{{{Partition: 0, Position: 5, Stop: &Stop{Reason: "timestamp", At: 5}}}}) {
		t.Fatal("late input moved progress or was published", store.saves, sink.writes, records, size)
	}
}

func TestRunnerRejectsNewInputWhileSubmissionIsPending(t *testing.T) {
	// Given a full destination queue holding an unsubmitted record.
	r, _, sink, store, _ := fixture(t)
	if err := r.Assign([]Assignment{{Cursor: Cursor{Partition: 0, Position: 0}}}); err != nil {
		t.Fatal(err)
	}
	sink.submit = func(Record, Token) error { return &Busy{} }
	if err := r.input(incoming(0, 0, 10)); err != nil {
		t.Fatal(err)
	}
	pending := r.pending
	beforeRecords, beforeBytes := r.tracker.Total()

	// When a source violates pause and returns a second record.
	err := r.input(incoming(0, 1, 20))

	// Then the first submission is retained, neither record is saved or published,
	// and the rejected record does not enter the uncommitted-work budget.
	if records, size := r.tracker.Total(); err == nil || !strings.Contains(err.Error(), "while intake was paused") || pending == nil || r.pending != pending || records != beforeRecords || size != beforeBytes || len(sink.writes) != 0 || len(store.saves) != 0 {
		t.Fatal("paused intake replaced or advanced pending work", err, r.pending, records, size)
	}
}

func TestDrainFlushFailureCommitsOnlyConfirmedPrefix(t *testing.T) {
	// Given one confirmed write followed by one unconfirmed write.
	r, source, sink, store, _ := fixture(t)
	if err := r.Assign([]Assignment{{Cursor: Cursor{Partition: 0, Position: 0}}}); err != nil {
		t.Fatal(err)
	}
	for _, position := range []int64{0, 1} {
		if err := r.input(incoming(0, position, 1)); err != nil {
			t.Fatal(err)
		}
	}
	sink.reports <- Delivery{Token: sink.writes[0]}
	if err := r.deliveries(); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("destination flush failed")
	sink.flush = func() error { return failure }

	// When shutdown stops intake and the destination fails its flush.
	err := r.drain()

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
}

func TestDrainTimeoutReportsWriteAndFinalSaveFailures(t *testing.T) {
	// Given a confirmed record, a failed record, and a write that never finishes.
	r, _, sink, store, clock := fixture(t)
	if err := r.Assign([]Assignment{{Cursor: Cursor{Partition: 0, Position: 0}}}); err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		if err := r.input(incoming(0, int64(i), 1)); err != nil {
			t.Fatal(err)
		}
	}
	writeErr, saveErr := errors.New("write failed"), errors.New("final save failed")
	sink.reports <- Delivery{Token: sink.writes[0]}
	sink.reports <- Delivery{Token: sink.writes[1], Err: writeErr}
	store.fail = map[int32]error{0: saveErr}
	started := clock.Now()
	// When the ten-second drain expires and saving the confirmed prefix also fails.
	err := r.exit(r.drain())
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
	if !errors.Is(runErr, progress.failure) {
		t.Fatal("progress failure lost", runErr)
	}
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
	if err != nil {
		t.Fatal(err)
	}
	if records, bytes := second.Total(); records != 0 || bytes != 0 {
		t.Fatal("query state leaked")
	}
	second.Assign(Cursor{Partition: 0, Position: 20})
	if first.Saved(0) != 5 || second.Saved(0) != 20 || !first.Settle(token, true) {
		t.Fatal("query ownership shared")
	}
}
