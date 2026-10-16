package file

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
)

// A failed fsync must never be retried into an acknowledgment: after Linux
// writeback errors a later fsync can succeed although the data was dropped.
func TestFailedSyncIsNeverAcknowledged(t *testing.T) {
	// Given
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
	cp, err := NewCheckpoints(t.TempDir(), "durable", DurableWriter{})
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
	runner := core.NewRunner(source, out, cp, core.RealClock{}, logger, p, limits, core.NewTracker(), core.NoopStopObserver{})

	// When copying records to an output whose first fsync fails.
	runErr := runner.Run(context.Background())

	// Then the failure stays latched and no progress acknowledges those records.
	if err := runErr; !errors.Is(err, failure) {
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

	// When another record is submitted after the failure.
	submitErr := out.Submit(core.Record{Value: []byte("d"), TimestampType: "unknown"}, core.Token{})

	// Then the original I/O failure is returned.
	if err := submitErr; !errors.Is(err, failure) {
		t.Fatalf("submit after failure = %v", err)
	}

	// When a record that also fails validation is submitted.
	validationErr := out.Submit(core.Record{Value: make([]byte, c.limit+1), TimestampType: "unknown"}, core.Token{})

	// Then validation cannot hide the latched I/O failure.
	if err := validationErr; !errors.Is(err, failure) {
		t.Fatalf("validation masked the original I/O failure: %v", err)
	}
}
