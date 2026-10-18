package file

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
)

type fileClock struct{ now time.Time }

func (c *fileClock) Now() time.Time { return c.now }

func (*fileClock) Sleep(time.Duration) {}

func TestFileSavedMixedWindowSurvivesClockAdvance(t *testing.T) {
	// Given a saved window with a relative lower bound and a literal upper bound.
	dir := t.TempDir()
	path := filepath.Join(dir, "source.cbor")
	c := codec(t)
	header, err := c.EncodeHeader(Header{Version: 1, ID: "source", Group: "fixture", Fingerprint: "fixture"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, header, 0600); err != nil {
		t.Fatal(err)
	}
	checkpoints, err := NewCheckpoints(dir, "mixed", DurableWriter{})
	if err != nil {
		t.Fatal(err)
	}
	defer checkpoints.Close()
	hi := int64(7200000)
	spec := plan.WindowSpec{Lower: &plan.Expression{Now: true, AdjustmentMS: -3600000}, Upper: &plan.Expression{LiteralMS: hi}}
	clock := &fileClock{now: time.UnixMilli(7200000)}
	source, err := NewSource(path, "fp", spec, c, checkpoints, clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Start(&lifecycle{}, nil); err != nil {
		t.Fatal(err)
	}
	saved := source.window
	source.Close()
	clock.now = time.UnixMilli(14400000)

	// When reopening after a fresh resolution would produce an empty window.
	source, err = NewSource(path, "fp", spec, c, checkpoints, clock)

	// Then the authoritative saved window is reused without resolving NOW again.
	if err != nil {
		t.Fatal("restart re-resolved the saved window", err)
	}
	defer source.Close()
	if !reflect.DeepEqual(source.window, saved) {
		t.Fatal("saved window changed", source.window, saved)
	}
}

type truncatingFileSource struct {
	*Source
	path  string
	cut   int64
	polls int
}

func (s *truncatingFileSource) Poll(timeout time.Duration) (core.Event, error) {
	if s.polls > 0 {
		if err := os.Truncate(s.path, s.cut); err != nil {
			return core.Event{}, err
		}
	}
	s.polls++
	return s.Source.Poll(timeout)
}

type unusedFileSink struct {
	core.Sink
	writes  int
	reports chan core.Delivery
}

func (s *unusedFileSink) Submit(_ core.Record, token core.Token) error {
	s.writes++
	s.reports <- core.Delivery{Token: token}
	return nil
}

func (s *unusedFileSink) Deliveries() <-chan core.Delivery { return s.reports }

func (*unusedFileSink) Flush() error { return nil }

func TestOpenSourceTruncationFailsUnderSkipPolicy(t *testing.T) {
	// Given an open archive with two complete records and a query that allows SKIP.
	dir := t.TempDir()
	path := filepath.Join(dir, "source.cbor")
	c := codec(t)
	header, err := c.EncodeHeader(Header{Version: 1, ID: "source", Group: "fixture", Fingerprint: "fixture"})
	if err != nil {
		t.Fatal(err)
	}
	data, err := c.EncodeRecord(sample())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(append(header, data...), data...), 0600); err != nil {
		t.Fatal(err)
	}
	cp, err := NewCheckpoints(dir, "truncate", DurableWriter{})
	if err != nil {
		t.Fatal(err)
	}
	defer cp.Close()
	source, err := NewSource(path, "fp", plan.WindowSpec{}, c, cp, core.RealClock{})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	sink := &unusedFileSink{reports: make(chan core.Delivery, 2)}
	p := plan.Plan{Source: plan.Endpoint{Kind: "FILE"}, Policies: plan.Policies{MissingTimestamp: "SKIP", RejectedTimestamp: "SKIP", OtherRecord: "SKIP"}}
	runner := core.NewRunner(&truncatingFileSource{Source: source, path: path, cut: int64(len(header)+len(data)) + 2}, sink, cp, core.RealClock{}, logger(), p, config.Defaults(dir).Worker, core.NewTracker(), core.NoopStopObserver{}, nil)

	// When the file shrinks after intake starts and the runner polls it.
	err = runner.Run(context.Background())

	// Then the source fails instead of completing or skipping the missing record.
	var exit *core.Exit
	if !errors.As(err, &exit) || exit.Code != 1 || sink.writes != 1 || cp.Existing().Position != int64(len(header)+len(data)) {
		t.Fatal("changed source did not fail", err, sink.writes)
	}
}

func TestFileAssignmentClearsStopAndRefreshesOpeningSize(t *testing.T) {
	// Given a saved EOF stop and an input with bytes appended after it stopped.
	dir := t.TempDir()
	path := filepath.Join(dir, "source.cbor")
	codec := codec(t)
	header, err := codec.EncodeHeader(Header{Version: 1, ID: "source", Group: "fixture", Fingerprint: "fixture"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(header, 0), 0600); err != nil {
		t.Fatal(err)
	}
	cp, err := NewCheckpoints(dir, "job", DurableWriter{})
	if err != nil {
		t.Fatal(err)
	}
	defer cp.Close()
	pos := int64(len(header))
	cp.Initialize(Checkpoint{Version: 2, Fingerprint: "fp", SourceID: "source", Position: pos, OpeningSize: pos})
	if _, err := cp.Save(t.Context(), []core.Cursor{{Position: pos, Stop: &core.Stop{Reason: "eof", At: pos}}}); err != nil {
		t.Fatal(err)
	}
	// When reopening and assigning the same file query.
	source, err := NewSource(path, "fp", plan.WindowSpec{}, codec, cp, core.RealClock{})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if err := source.Start(&lifecycle{}, nil); err != nil {
		t.Fatal(err)
	}
	// Then the first saved checkpoint clears the old stop before reading and records the new opening size.
	saved, err := readCheckpoint(cp.path)
	if err != nil || saved.Stop != nil || saved.Position != pos || saved.OpeningSize != pos+1 {
		t.Fatal(saved, err)
	}
}
