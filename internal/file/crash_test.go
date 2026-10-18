package file

import (
	"context"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
)

func TestFileCrashAfterFsyncBeforeCheckpoint(t *testing.T) {
	// Given
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	c := codec(t)
	if os.Getenv("KAFERRY_FILE_CRASH_HELPER") == "1" {
		args := os.Args[len(os.Args)-3:]
		cp, err := NewCheckpoints(args[2], "crash", DurableWriter{})
		require.NoError(t, err)
		source, err := NewSource(args[0], "fp", plan.WindowSpec{}, c, cp, core.RealClock{})
		require.NoError(t, err)
		saved := cp.Existing()
		out, err := NewOutput(NewPublisher(), args[1], "crash", "fp", saved.DestinationID, saved.Extent, c, 10, logger)
		require.NoError(t, err)
		e, err := source.Poll(0)
		if err != nil || e.Input == nil {
			t.Fatal("source unreadable", err)
		}
		require.NoError(t, out.Submit(e.Input.Record, core.Token{}))
		require.NoError(t, out.Flush())
		os.Exit(99) // Durable data, deliberately no source checkpoint or deferred closes.
	}
	dir := t.TempDir()
	src, dst, cpdir := filepath.Join(dir, "source.cbor"), filepath.Join(dir, "output.cbor"), t.TempDir()
	o, err := NewOutput(NewPublisher(), src, "fixture", "fixture", "", 0, c, 10, logger)
	require.NoError(t, err)
	for _, value := range []string{"a", "b", "c"} {
		require.NoError(t, o.Submit(core.Record{Value: []byte(value), TimestampType: "unknown"}, core.Token{}))
	}
	require.NoError(t, o.Flush())
	o.Close()
	cp, err := NewCheckpoints(cpdir, "crash", DurableWriter{})
	require.NoError(t, err)
	source, err := NewSource(src, "fp", plan.WindowSpec{}, c, cp, core.RealClock{})
	require.NoError(t, err)
	out, err := NewOutput(NewPublisher(), dst, "crash", "fp", "", 0, c, 10, logger)
	require.NoError(t, err)
	cp.BindOutput(out)
	require.NoError(t, source.Start(&lifecycle{}, nil))
	source.Close()
	out.Close()
	cp.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFileCrashAfterFsyncBeforeCheckpoint$", "--", src, dst, cpdir)
	child.Env = append(os.Environ(), "KAFERRY_FILE_CRASH_HELPER=1")

	// When
	data, err := child.CombinedOutput()

	// Then
	if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 99 {
		t.Fatalf("crash helper: %v %s", err, data)
	}
	cp, err = NewCheckpoints(cpdir, "crash", DurableWriter{})
	require.NoError(t, err)
	defer cp.Close()
	source, err = NewSource(src, "fp", plan.WindowSpec{}, c, cp, core.RealClock{})
	require.NoError(t, err)
	defer source.Close()
	saved := cp.Existing()
	out, err = NewOutput(NewPublisher(), dst, "crash", "fp", saved.DestinationID, saved.Extent, c, 10, logger)
	require.NoError(t, err)
	defer out.Close()
	cp.BindOutput(out)
	r := core.NewRunner(source, out, cp, core.RealClock{}, logger, plan.Plan{Source: plan.Endpoint{Kind: "FILE"}}, config.Defaults(dir).Worker, core.NewTracker(), core.NoopStopObserver{}, nil)
	require.NoError(t, r.Run(ctx))
	require.Equal(t, []string{"a", "a", "b", "c"}, outputValues(t, dst), "crash recovery preserves order and may replay the last durable record")
}

func TestOutputWriteFailureCannotAcknowledge(t *testing.T) {
	// Given
	path := filepath.Join(t.TempDir(), "out.cbor")
	o, err := NewOutput(NewPublisher(), path, "g", "fp", "", 0, codec(t), 10, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	_, before := o.DurableOutput()
	require.NoError(t, o.Submit(core.Record{Value: []byte("record"), TimestampType: "unknown"}, core.Token{}))
	_ = o.file.Close()

	// When
	flushErr := o.Flush()

	// Then
	if flushErr == nil {
		t.Fatal("write to closed file succeeded")
	}
	_, after := o.DurableOutput()
	if before != after || len(o.deliveries) != 0 {
		t.Fatal("failed write advanced acknowledged extent")
	}
}
