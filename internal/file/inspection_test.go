package file

import (
	"bytes"
	"encoding/json"
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

func inspectionFixture(t *testing.T) (plan.Plan, config.Worker, *Inspector, int64) {
	t.Helper()
	directory := t.TempDir()
	c := codec(t)
	header, err := c.EncodeHeader(Header{Version: 1, ID: "source", Group: "fixture", Fingerprint: "fixture"})
	if err != nil {
		t.Fatal(err)
	}
	record, err := c.EncodeRecord(sample())
	if err != nil {
		t.Fatal(err)
	}
	p := plan.Plan{Source: plan.Endpoint{Kind: "FILE", Name: filepath.Join(directory, "source.cbor")}, Destination: plan.Endpoint{Kind: "FILE", Name: filepath.Join(directory, "output.cbor")}, Group: "preview"}
	if err := os.WriteFile(p.Source.Name, append(bytes.Clone(header), record...), 0600); err != nil {
		t.Fatal(err)
	}
	limits := config.Defaults("").Worker
	limits.CheckpointDir = &directory
	return p, limits, NewInspector(&fileClock{now: time.UnixMilli(1000)}), int64(len(header))
}

func directoryContents(t *testing.T, directory string) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	contents := make(map[string]string)
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		contents[entry.Name()] = string(data)
	}
	return contents
}

func TestFileInspectionCreatesNoCheckpointLockOrOutput(t *testing.T) {
	// Given a source archive and no checkpoint, output or lock.
	p, limits, inspector, start := inspectionFixture(t)
	p.Window.Upper = &plan.Expression{Now: true}
	before := directoryContents(t, *limits.CheckpointDir)
	// When inspecting a fresh source and destination.
	source, err := inspector.Source(p, "query", limits)
	if err != nil {
		t.Fatal(err)
	}
	destination, err := inspector.Destination(p, "query", source.Output, limits)
	// Then report the header boundary and preview window without changing any file.
	if err != nil || source.Saved || source.Position != start || source.ID != "source" || *source.Window.Upper != 1000 || destination.Exists || !reflect.DeepEqual(before, directoryContents(t, *limits.CheckpointDir)) {
		t.Fatal(source, destination, err)
	}
}

func TestOneOffFileInspectionIgnoresUnrelatedCheckpoints(t *testing.T) {
	// Given an invalid checkpoint under a name a fresh one-off run will not use.
	p, limits, inspector, start := inspectionFixture(t)
	if err := os.WriteFile(checkpointPath(*limits.CheckpointDir, p.Group), []byte("invalid checkpoint"), 0600); err != nil {
		t.Fatal(err)
	}
	p.OneOff = true
	before := directoryContents(t, *limits.CheckpointDir)
	// When inspecting ONE_OFF, then read from the header without choosing a group name.
	info, err := inspector.Source(p, "query", limits)
	if err != nil || info.Saved || info.Position != start || info.CheckpointPath != "" || !reflect.DeepEqual(before, directoryContents(t, *limits.CheckpointDir)) {
		t.Fatal(info, err)
	}
}

func TestFileInspectionRejectsAnEmptyResolvedWindowWithoutChanges(t *testing.T) {
	// Given a mixed NOW/literal window that becomes empty at the injected clock time.
	p, limits, inspector, _ := inspectionFixture(t)
	p.Window = plan.WindowSpec{Lower: &plan.Expression{Now: true}, Upper: &plan.Expression{LiteralMS: 500}}
	before := directoryContents(t, *limits.CheckpointDir)
	// When inspecting, then classify it as invalid input without changing any files.
	_, err := inspector.Source(p, "query", limits)
	var classified interface{ ExitCode() int }
	if !errors.As(err, &classified) || classified.ExitCode() != 2 {
		t.Fatalf("expected invalid-input exit 2, got %v", err)
	}
	if !reflect.DeepEqual(before, directoryContents(t, *limits.CheckpointDir)) {
		t.Fatal("failed inspection changed files")
	}
}

func TestFileInspectionReadsSavedProgressWhileItsLockIsHeld(t *testing.T) {
	// Given an active checkpoint owner with a saved frame boundary and NOW window.
	p, limits, inspector, _ := inspectionFixture(t)
	p.Window.Upper = &plan.Expression{Now: true}
	cp, err := NewCheckpoints(*limits.CheckpointDir, p.Group, DurableWriter{})
	if err != nil {
		t.Fatal(err)
	}
	defer cp.Close()
	source, err := NewSource(p.Source.Name, "query", p.Window, codec(t), cp, &fileClock{now: time.UnixMilli(500)})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if err := source.Start(&lifecycle{}); err != nil {
		t.Fatal(err)
	}
	event, err := source.Poll(0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cp.Save(t.Context(), []core.Cursor{{Partition: 0, Position: event.Input.Next}}); err != nil {
		t.Fatal(err)
	}
	before := directoryContents(t, *limits.CheckpointDir)
	// When inspecting without acquiring the owner's checkpoint lock.
	info, err := inspector.Source(p, "query", limits)
	// Then saved bytes and the old NOW window survive and the owner retains its lock.
	if err != nil || !info.Saved || info.Position != event.Input.Next || *info.Window.Upper != 500 || !reflect.DeepEqual(before, directoryContents(t, *limits.CheckpointDir)) {
		t.Fatal(info, err)
	}
	other, err := os.OpenFile(checkpointPath(*limits.CheckpointDir, p.Group)+".lock", os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err := lock(other); err == nil {
		t.Fatal("inspection released the checkpoint owner's lock")
	}
	// Given an unrelated checkpoint for another fingerprint, then inspection fails without rewriting it.
	if _, err := inspector.Source(p, "other-query", limits); err == nil {
		t.Fatal("foreign checkpoint accepted")
	}
	if !reflect.DeepEqual(before, directoryContents(t, *limits.CheckpointDir)) {
		t.Fatal("failed inspection changed files")
	}
}

func TestDestinationInspectionDoesNotRecoverOrLockATornTail(t *testing.T) {
	// Given an output locked by its owner with acknowledged bytes and a partial tail.
	p, limits, inspector, _ := inspectionFixture(t)
	out, err := NewOutput(NewPublisher(), p.Destination.Name, p.Group, "query", "", 0, codec(t), 10, logger())
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	if err := out.Submit(sample(), core.Token{}); err != nil {
		t.Fatal(err)
	}
	if err := out.Flush(); err != nil {
		t.Fatal(err)
	}
	id, extent := out.DurableOutput()
	tail, err := os.OpenFile(p.Destination.Name, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tail.Write([]byte{0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	tail.Close()
	before := directoryContents(t, *limits.CheckpointDir)
	// When inspecting the acknowledged extent without opening the output for writing.
	info, err := inspector.Destination(p, "query", DestinationProgress{Exists: true, ID: id, Extent: extent}, limits)
	// Then report the tail's size without truncating it or conflicting with the owner.
	if err != nil || info.ID != id || info.Size != extent+3 || !reflect.DeepEqual(before, directoryContents(t, *limits.CheckpointDir)) {
		t.Fatal(info, err)
	}
	if _, err := inspector.Destination(p, "other", DestinationProgress{Exists: true, ID: id, Extent: extent}, limits); err == nil {
		t.Fatal("foreign destination accepted")
	}
}

func TestFileInspectionRejectsInvalidIdentityAndMissingSavedOutput(t *testing.T) {
	for _, problem := range []string{"missing output", "missing identity", "hardlink", "bad header", "bad checkpoint", "missing directory"} {
		t.Run(problem, func(t *testing.T) {
			// Given invalid input or saved output identity.
			p, limits, inspector, _ := inspectionFixture(t)
			var err error
			switch problem {
			case "missing output", "missing identity":
				progress := DestinationProgress{Exists: true, ID: "missing", Extent: 100}
				if problem == "missing identity" {
					progress.ID, progress.Extent = "", 0
				}
				_, err = inspector.Destination(p, "query", progress, limits)
			case "hardlink":
				if err := os.Link(p.Source.Name, p.Destination.Name); err != nil {
					t.Fatal(err)
				}
			case "bad header":
				if err := os.WriteFile(p.Source.Name, []byte("invalid"), 0600); err != nil {
					t.Fatal(err)
				}
			case "bad checkpoint":
				data, _ := json.Marshal(Checkpoint{Version: 50})
				if err := os.WriteFile(checkpointPath(*limits.CheckpointDir, p.Group), data, 0600); err != nil {
					t.Fatal(err)
				}
			case "missing directory":
				missing := filepath.Join(*limits.CheckpointDir, "missing")
				limits.CheckpointDir = &missing
			}
			// When inspecting, then fail rather than creating or repairing anything.
			if problem != "missing output" && problem != "missing identity" {
				_, err = inspector.Source(p, "query", limits)
			}
			if err == nil {
				t.Fatal("invalid state accepted")
			}
		})
	}
}
