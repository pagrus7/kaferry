package file

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"kafka-fwd2/internal/config"
	core "kafka-fwd2/internal/copy"
	"kafka-fwd2/internal/plan"
)

type fileClock struct{ now time.Time }

func (c *fileClock) Now() time.Time    { return c.now }
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
	if err := source.Start(&lifecycle{}); err != nil {
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

func TestPhysicalShortReadsCannotBecomeSnapshotEOF(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		length int
	}{{"prefix", 2}, {"body", 7}} {
		t.Run(scenario.name, func(t *testing.T) {
			// Given a complete frame in the open-time snapshot, now physically shorter.
			data := frame([]byte{1, 2, 3, 4})
			reader := bytes.NewReader(data[:scenario.length])

			// When reading the advertised snapshot extent.
			_, _, _, err := readFrame(reader, 0, int64(len(data)), 1024)

			// Then a changed-source conflict fails, without matching normal EOF.
			var changed *Conflict
			if err == nil || errors.Is(err, io.EOF) || !errors.As(err, &changed) {
				t.Fatal("short read treated as snapshot completion", err)
			}
		})
	}
	// Given an originally partial trailing frame.
	data := frame([]byte{1, 2, 3, 4})

	// When its original snapshot ends before the complete frame.
	_, _, _, err := readFrame(bytes.NewReader(data[:7]), 0, 7, 1024)

	// Then the partial trailing frame retains normal snapshot EOF semantics.
	if !errors.Is(err, io.EOF) {
		t.Fatal("original partial tail no longer ends the snapshot", err)
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
func (*unusedFileSink) Flush() error                       { return nil }

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
	runner := core.NewRunner(&truncatingFileSource{Source: source, path: path, cut: int64(len(header)+len(data)) + 2}, sink, cp, core.RealClock{}, logger(), p, config.Defaults(dir).Worker, core.NewTracker())

	// When the file shrinks after intake starts and the runner polls it.
	err = runner.Run(context.Background())

	// Then the source fails instead of completing or skipping the missing record.
	var exit *core.Exit
	if !errors.As(err, &exit) || exit.Code != 1 || sink.writes != 1 || cp.Existing().Position != int64(len(header)+len(data)) {
		t.Fatal("changed source did not fail", err, sink.writes)
	}
}

func TestProvenanceArchiveWireBytesAreStable(t *testing.T) {
	// Given the documented nullable record fields and diagnostic provenance.
	c := codec(t)
	record := core.Record{Key: nil, Value: []byte{}, TimestampType: "unknown", Source: &core.Provenance{Topic: "orders", Partition: 3, Offset: 42}}

	// When encoding the full logical record.
	data, err := c.EncodeRecord(record)

	// Then the archive wire representation remains byte-identical.
	if err != nil {
		t.Fatal(err)
	}
	const golden = "00000064a6636b6579f66576616c75654066736f75726365a365746f706963666f7264657273666f6666736574182a69706172746974696f6e036768656164657273806c74696d657374616d705f6d73f66e74696d657374616d705f7479706567756e6b6e6f776e3b069d95"
	if hex.EncodeToString(data) != golden {
		t.Fatalf("archive provenance wire bytes changed: %x", data)
	}
	decoded, err := c.DecodeRecord(data[4 : len(data)-4])
	if err != nil || !reflect.DeepEqual(decoded, record) {
		t.Fatal("provenance round trip changed", decoded, err)
	}
}

type failedReader struct{ err error }

func (r failedReader) ReadAt([]byte, int64) (int, error) { return 0, r.err }

func TestFrameReadPreservesNonEOFFailure(t *testing.T) {
	// Given an input device failure rather than a shortened file.
	cause := errors.New("injected device failure")
	// When reading the snapshot prefix.
	_, _, _, err := readFrame(failedReader{err: cause}, 0, 12, 1024)
	// Then the original cause remains available to the caller.
	var changed *Conflict
	if !errors.Is(err, cause) || errors.Is(err, io.EOF) || errors.As(err, &changed) {
		t.Fatal("device failure misclassified as truncation", err)
	}
}
