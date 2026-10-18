package file

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
)

func TestFreshFileRejectsEmptyResolvedWindowWithoutSavingOrKeepingLock(t *testing.T) {
	// Given a fresh query whose NOW bound has passed its literal upper bound.
	p, limits, _, _ := inspectionFixture(t)
	p.Window = plan.WindowSpec{Lower: &plan.Expression{Now: true}, Upper: &plan.Expression{LiteralMS: 500}}
	factory := NewFactory(&fileClock{now: time.UnixMilli(1000)}, logger(), DurableWriter{}, NewPublisher())

	// When opening the source.
	input, err := factory.OpenSource(p, "query", limits)

	// Then it fails before saving, and correcting the query can reuse the group.
	require.Nil(t, input)
	var conflict *Conflict
	require.ErrorAs(t, err, &conflict)
	require.Equal(t, 2, conflict.ExitCode())
	require.ErrorContains(t, err, "time range is empty")
	_, statErr := os.Stat(checkpointPath(*limits.CheckpointDir, p.Group))
	require.ErrorIs(t, statErr, os.ErrNotExist, "invalid startup must not save progress")
	p.Window = plan.WindowSpec{}
	input, err = factory.OpenSource(p, "query", limits)
	require.NoError(t, err, "failed startup must release the group lock")
	t.Cleanup(func() { require.NoError(t, input.Close()) })
}

type fileClock struct{ now time.Time }

func (c *fileClock) Now() time.Time { return c.now }

func (*fileClock) Sleep(time.Duration) {}

func TestFileSavedMixedWindowSurvivesClockAdvance(t *testing.T) {
	// Given a saved window with a relative lower bound and a literal upper bound.
	dir := t.TempDir()
	path := filepath.Join(dir, "source.cbor")
	c := codec(t)
	header, err := c.EncodeHeader(Header{Version: 1, ID: "source", Group: "fixture", Fingerprint: "fixture"})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, header, 0600))
	checkpoints, err := NewCheckpoints(dir, "mixed", DurableWriter{})
	require.NoError(t, err)
	defer checkpoints.Close()
	hi := int64(7200000)
	spec := plan.WindowSpec{Lower: &plan.Expression{Now: true, AdjustmentMS: -3600000}, Upper: &plan.Expression{LiteralMS: hi}}
	clock := &fileClock{now: time.UnixMilli(7200000)}
	source, err := NewSource(path, "fp", spec, c, checkpoints, clock)
	require.NoError(t, err)
	require.NoError(t, source.Start(&lifecycle{}, nil))
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
	require.Equal(t, saved, source.window, "resume keeps the saved window")
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
	require.NoError(t, err)
	data, err := c.EncodeRecord(sample())
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, append(append(header, data...), data...), 0600))
	cp, err := NewCheckpoints(dir, "truncate", DurableWriter{})
	require.NoError(t, err)
	defer cp.Close()
	source, err := NewSource(path, "fp", plan.WindowSpec{}, c, cp, core.RealClock{})
	require.NoError(t, err)
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
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, append(header, 0), 0600))
	cp, err := NewCheckpoints(dir, "job", DurableWriter{})
	require.NoError(t, err)
	defer cp.Close()
	pos := int64(len(header))
	cp.Initialize(Checkpoint{Version: 3, Fingerprint: "fp", SourceID: "source", Position: pos, OpeningSize: pos})
	if _, err := cp.Save(t.Context(), []core.Cursor{{Position: pos, Stop: &core.Stop{Reason: "eof", At: pos}}}); err != nil {
		t.Fatal(err)
	}
	// When reopening and assigning the same file query.
	source, err := NewSource(path, "fp", plan.WindowSpec{}, codec, cp, core.RealClock{})
	require.NoError(t, err)
	defer source.Close()
	require.NoError(t, source.Start(&lifecycle{}, nil))
	// Then the first saved checkpoint clears the old stop before reading and records the new opening size.
	saved, err := readCheckpoint(cp.path)
	if err != nil || saved.Stop != nil || saved.Position != pos || saved.OpeningSize != pos+1 {
		t.Fatal(saved, err)
	}
}
