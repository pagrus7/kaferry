package file

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"kafka-fwd2/internal/config"
	core "kafka-fwd2/internal/copy"
	"kafka-fwd2/internal/plan"
)

// A failed fsync must never be retried into an acknowledgment: after Linux
// writeback errors a later fsync can succeed although the data was dropped.
func TestFailedSyncIsNeverAcknowledged(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	c := codec(t)
	dir := t.TempDir()
	src, dst := filepath.Join(dir, "source.cbor"), filepath.Join(dir, "output.cbor")
	fixture, err := NewOutput(NewPublisher(), src, "fixture", "fixture", "", 0, c, 10, logger)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"a", "b", "c"} {
		if err := fixture.Submit(core.Record{Value: []byte(value), TimestampType: "unknown"}, core.Token{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := fixture.Flush(); err != nil {
		t.Fatal(err)
	}
	fixture.Close()

	limits := config.Defaults(dir).Worker
	cp, err := NewCheckpoints(filepath.Join(dir, "checkpoints"), "durable", DurableWriter{})
	if err != nil {
		t.Fatal(err)
	}
	defer cp.Close()
	source, err := NewSource(src, "fp", plan.WindowSpec{}, c, cp, core.RealClock{})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	out, err := NewOutput(NewPublisher(), dst, "durable", "fp", "", 0, c, limits.RecordCap, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	_, initial := out.DurableOutput()
	syncs := 0
	failure := errors.New("fsync: input/output error")
	out.sync = func() error {
		syncs++
		if syncs == 1 {
			return failure
		}
		return nil // The retry would "succeed"; it must not be attempted.
	}
	cp.BindOutput(out)
	p := plan.Plan{Source: plan.Endpoint{Kind: "FILE"}, Destination: plan.Endpoint{Kind: "FILE"}}
	runner := core.NewRunner(source, out, cp, core.RealClock{}, logger, p, limits, core.NewTracker())
	if err := runner.Run(context.Background()); !errors.Is(err, failure) {
		t.Fatalf("run error = %v, want the fsync failure", err)
	}
	if syncs != 1 {
		t.Fatalf("fsync attempted %d times after failing", syncs)
	}
	if _, extent := out.DurableOutput(); extent != initial {
		t.Fatalf("durable extent moved from %d to %d", initial, extent)
	}
	saved := cp.Existing()
	if saved.LastFrame != nil || saved.Extent != initial {
		t.Fatalf("checkpoint advanced past failed fsync: %+v", saved)
	}
	if err := out.Submit(core.Record{Value: []byte("d"), TimestampType: "unknown"}, core.Token{}); !errors.Is(err, failure) {
		t.Fatalf("submit after failure = %v", err)
	}
	if err := out.Submit(core.Record{Value: make([]byte, c.limit+1), TimestampType: "unknown"}, core.Token{}); !errors.Is(err, failure) {
		t.Fatalf("validation masked the original I/O failure: %v", err)
	}
}
