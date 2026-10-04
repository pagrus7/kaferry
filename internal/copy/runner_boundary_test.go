package copy

import (
	"context"
	"errors"
	"testing"
)

type failingProgress struct {
	ProgressTracker
	failure  error
	observed []Cursor
}

func (*failingProgress) Assign(Cursor) {}
func (p *failingProgress) Observe(cursor Cursor, _, _ int64, _ bool) (Token, error) {
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
	token, err := first.Observe(Cursor{Partition: 0, Position: 5}, 6, 100, false)

	// Then
	if err != nil {
		t.Fatal(err)
	}
	if records, bytes := second.Total(); records != 0 || bytes != 0 {
		t.Fatal("query state leaked")
	}
	second.Assign(Cursor{Partition: 0, Position: 20})
	if first.Saved(0) != 5 || second.Saved(0) != 20 || !first.Resolve(token) {
		t.Fatal("query ownership shared")
	}
}
