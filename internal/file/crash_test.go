package file

import (
	"context"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"kafka-fwd2/internal/config"
	core "kafka-fwd2/internal/copy"
	"kafka-fwd2/internal/plan"
)

func TestFileCrashAfterFsyncBeforeCheckpoint(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	c := codec(t)
	if os.Getenv("KFWD_FILE_CRASH_HELPER") == "1" {
		args := os.Args[len(os.Args)-3:]
		cp, err := NewCheckpoints(args[2], "crash", DurableWriter{})
		if err != nil {
			t.Fatal(err)
		}
		source, err := NewSource(args[0], "fp", plan.WindowSpec{}, c, cp, core.RealClock{})
		if err != nil {
			t.Fatal(err)
		}
		saved := cp.Existing()
		out, err := NewOutput(NewPublisher(), args[1], "crash", "fp", saved.DestinationID, saved.Extent, c, 10, logger)
		if err != nil {
			t.Fatal(err)
		}
		e, err := source.Poll(0)
		if err != nil || e.Input == nil {
			t.Fatal("source unreadable", err)
		}
		if err := out.Submit(e.Input.Record, core.Token{}); err != nil {
			t.Fatal(err)
		}
		if err := out.Flush(); err != nil {
			t.Fatal(err)
		}
		os.Exit(99) // Durable data, deliberately no source checkpoint or deferred closes.
	}
	dir := t.TempDir()
	src, dst, cpdir := filepath.Join(dir, "source.cbor"), filepath.Join(dir, "output.cbor"), filepath.Join(dir, "checkpoints")
	o, err := NewOutput(NewPublisher(), src, "fixture", "fixture", "", 0, c, 10, logger)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"a", "b", "c"} {
		if err := o.Submit(core.Record{Value: []byte(value), TimestampType: "unknown"}, core.Token{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := o.Flush(); err != nil {
		t.Fatal(err)
	}
	o.Close()
	cp, err := NewCheckpoints(cpdir, "crash", DurableWriter{})
	if err != nil {
		t.Fatal(err)
	}
	source, err := NewSource(src, "fp", plan.WindowSpec{}, c, cp, core.RealClock{})
	if err != nil {
		t.Fatal(err)
	}
	out, err := NewOutput(NewPublisher(), dst, "crash", "fp", "", 0, c, 10, logger)
	if err != nil {
		t.Fatal(err)
	}
	cp.BindOutput(out)
	if err := source.Start(&lifecycle{}); err != nil {
		t.Fatal(err)
	}
	source.Close()
	out.Close()
	cp.Close()
	child := exec.Command(os.Args[0], "-test.run=^TestFileCrashAfterFsyncBeforeCheckpoint$", "--", src, dst, cpdir)
	child.Env = append(os.Environ(), "KFWD_FILE_CRASH_HELPER=1")
	data, err := child.CombinedOutput()
	if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 99 {
		t.Fatalf("crash helper: %v %s", err, data)
	}
	cp, err = NewCheckpoints(cpdir, "crash", DurableWriter{})
	if err != nil {
		t.Fatal(err)
	}
	defer cp.Close()
	source, err = NewSource(src, "fp", plan.WindowSpec{}, c, cp, core.RealClock{})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	saved := cp.Existing()
	out, err = NewOutput(NewPublisher(), dst, "crash", "fp", saved.DestinationID, saved.Extent, c, 10, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	cp.BindOutput(out)
	r := core.NewRunner(source, out, cp, core.RealClock{}, logger, plan.Plan{Source: plan.Endpoint{Kind: "FILE"}}, config.Defaults(dir).Worker, core.NewTracker())
	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(dst)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, _ := f.Stat()
	_, pos, err := c.Header(f, info.Size())
	if err != nil {
		t.Fatal(err)
	}
	var values []string
	for pos < info.Size() {
		payload, next, _, err := readFrame(f, pos, info.Size(), c.limit)
		if err != nil {
			t.Fatal(err)
		}
		record, err := c.DecodeRecord(payload)
		if err != nil {
			t.Fatal(err)
		}
		values = append(values, string(record.Value))
		pos = next
	}
	if len(values) != 4 || values[0] != "a" || values[1] != "a" || values[2] != "b" || values[3] != "c" {
		t.Fatalf("crash/restart lost data or altered order: %v", values)
	}
}

func TestOutputWriteFailureCannotAcknowledge(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.cbor")
	o, err := NewOutput(NewPublisher(), path, "g", "fp", "", 0, codec(t), 10, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	_, before := o.DurableOutput()
	if err := o.Submit(core.Record{Value: []byte("record"), TimestampType: "unknown"}, core.Token{}); err != nil {
		t.Fatal(err)
	}
	_ = o.file.Close()
	if err := o.Flush(); err == nil {
		t.Fatal("write to closed file succeeded")
	}
	_, after := o.DurableOutput()
	if before != after || len(o.deliveries) != 0 {
		t.Fatal("failed write advanced acknowledged extent")
	}
}
