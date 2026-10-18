package file

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
)

func inspectionFixture(t *testing.T) (plan.Plan, config.Worker, *Inspector, int64) {
	t.Helper()
	directory := t.TempDir()
	c := codec(t)
	header, err := c.EncodeHeader(Header{Version: 1, ID: "source", Group: "fixture", Fingerprint: "fixture"})
	require.NoError(t, err)
	record, err := c.EncodeRecord(sample())
	require.NoError(t, err)
	p := plan.Plan{Source: plan.Endpoint{Kind: "FILE", Name: filepath.Join(directory, "source.cbor")}, Destination: plan.Endpoint{Kind: "FILE", Name: filepath.Join(directory, "output.cbor")}, Group: "preview"}
	require.NoError(t, os.WriteFile(p.Source.Name, append(bytes.Clone(header), record...), 0600))
	limits := config.Defaults("").Worker
	limits.CheckpointDir = &directory
	return p, limits, NewInspector(&fileClock{now: time.UnixMilli(1000)}), int64(len(header))
}

func directoryContents(t *testing.T, directory string) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(directory)
	require.NoError(t, err)
	contents := make(map[string]string)
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		require.NoError(t, err)
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
	require.NoError(t, err)
	destination, err := inspector.Destination(p, "query", source.Output, limits)
	// Then report the header boundary and preview window without changing any file.
	require.NoError(t, err)
	require.False(t, source.Saved)
	require.Equal(t, start, source.Position)
	require.Equal(t, "source", source.ID)
	require.Equal(t, new(int64(1000)), source.Window.Upper)
	require.False(t, destination.Exists)
	require.Equal(t, before, directoryContents(t, *limits.CheckpointDir), "inspection does not change files")
}

func TestOneOffFileInspectionIgnoresUnrelatedCheckpoints(t *testing.T) {
	// Given an invalid checkpoint under a name a fresh one-off run will not use.
	p, limits, inspector, start := inspectionFixture(t)
	require.NoError(t, os.WriteFile(checkpointPath(*limits.CheckpointDir, p.Group), []byte("invalid checkpoint"), 0600))
	p.OneOff = true
	before := directoryContents(t, *limits.CheckpointDir)
	// When inspecting ONE_OFF, then read from the header without choosing a group name.
	info, err := inspector.Source(p, "query", limits)
	require.NoError(t, err)
	require.False(t, info.Saved)
	require.Equal(t, start, info.Position)
	require.Empty(t, info.CheckpointPath)
	require.Equal(t, before, directoryContents(t, *limits.CheckpointDir), "one-off inspection does not change files")
}

func TestFileInspectionRejectsAnEmptyResolvedWindowWithoutChanges(t *testing.T) {
	// Given a mixed NOW/literal window that becomes empty at the injected clock time.
	p, limits, inspector, _ := inspectionFixture(t)
	p.Window = plan.WindowSpec{Lower: &plan.Expression{Now: true}, Upper: &plan.Expression{LiteralMS: 500}}
	before := directoryContents(t, *limits.CheckpointDir)
	// When inspecting, then classify it as invalid input without changing any files.
	_, err := inspector.Source(p, "query", limits)
	var classified interface{ ExitCode() int }
	require.ErrorAs(t, err, &classified)
	require.Equal(t, 2, classified.ExitCode())
	require.Equal(t, before, directoryContents(t, *limits.CheckpointDir), "failed inspection does not change files")
}

func TestFileInspectionReadsSavedProgressWhileItsLockIsHeld(t *testing.T) {
	// Given an active checkpoint owner with a saved frame boundary and NOW window.
	p, limits, inspector, _ := inspectionFixture(t)
	p.Window.Upper = &plan.Expression{Now: true}
	cp, err := NewCheckpoints(*limits.CheckpointDir, p.Group, DurableWriter{})
	require.NoError(t, err)
	defer cp.Close()
	source, err := NewSource(p.Source.Name, "query", p.Window, codec(t), cp, &fileClock{now: time.UnixMilli(500)})
	require.NoError(t, err)
	defer source.Close()
	require.NoError(t, source.Start(&lifecycle{}, nil))
	event, err := source.Poll(0)
	require.NoError(t, err)
	if _, err := cp.Save(t.Context(), []core.Cursor{{Partition: 0, Position: event.Input.Next}}); err != nil {
		t.Fatal(err)
	}
	before := directoryContents(t, *limits.CheckpointDir)
	// When inspecting without acquiring the owner's checkpoint lock.
	info, err := inspector.Source(p, "query", limits)
	// Then saved bytes and the old NOW window survive and the owner retains its lock.
	require.NoError(t, err)
	require.True(t, info.Saved)
	require.Equal(t, event.Input.Next, info.Position)
	require.Equal(t, new(int64(500)), info.Window.Upper)
	require.Equal(t, before, directoryContents(t, *limits.CheckpointDir), "inspection leaves the owner's files unchanged")
	other, err := os.OpenFile(checkpointPath(*limits.CheckpointDir, p.Group)+".lock", os.O_RDWR, 0)
	require.NoError(t, err)
	defer other.Close()
	if err := lock(other); err == nil {
		t.Fatal("inspection released the checkpoint owner's lock")
	}
	// Given an unrelated checkpoint for another fingerprint, then inspection fails without rewriting it.
	if _, err := inspector.Source(p, "other-query", limits); err == nil {
		t.Fatal("foreign checkpoint accepted")
	}
	require.Equal(t, before, directoryContents(t, *limits.CheckpointDir), "failed inspection does not change files")
}

func TestDestinationInspectionDoesNotRecoverOrLockATornTail(t *testing.T) {
	// Given an output locked by its owner with acknowledged bytes and a partial tail.
	p, limits, inspector, _ := inspectionFixture(t)
	out, err := NewOutput(NewPublisher(), p.Destination.Name, p.Group, "query", "", 0, codec(t), 10, logger())
	require.NoError(t, err)
	defer out.Close()
	require.NoError(t, out.Submit(sample(), core.Token{}))
	require.NoError(t, out.Flush())
	id, extent := out.DurableOutput()
	tail, err := os.OpenFile(p.Destination.Name, os.O_WRONLY|os.O_APPEND, 0)
	require.NoError(t, err)
	if _, err := tail.Write([]byte{0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	tail.Close()
	before := directoryContents(t, *limits.CheckpointDir)
	// When inspecting the acknowledged extent without opening the output for writing.
	info, err := inspector.Destination(p, "query", DestinationProgress{Exists: true, ID: id, Extent: extent}, limits)
	// Then report the tail's size without truncating it or conflicting with the owner.
	require.NoError(t, err)
	require.Equal(t, id, info.ID)
	require.Equal(t, extent+3, info.Size)
	require.Equal(t, before, directoryContents(t, *limits.CheckpointDir), "inspection leaves the torn tail unchanged")
	if _, err := inspector.Destination(p, "other", DestinationProgress{Exists: true, ID: id, Extent: extent}, limits); err == nil {
		t.Fatal("foreign destination accepted")
	}
}

func TestFileInspectionRejectsInvalidIdentityAndMissingSavedOutput(t *testing.T) {
	for _, problem := range []string{"missing output", "missing identity", "bad output header", "directory source", "hardlink", "bad header", "bad checkpoint", "missing directory"} {
		t.Run(problem, func(t *testing.T) {
			// Given invalid input or saved output identity.
			p, limits, inspector, _ := inspectionFixture(t)
			directory := *limits.CheckpointDir
			progress := DestinationProgress{Exists: true, ID: "missing", Extent: 100}
			destination := false
			switch problem {
			case "missing output", "missing identity", "bad output header":
				destination = true
				if problem == "missing identity" {
					progress.ID, progress.Extent = "", 0
				}
				if problem == "bad output header" {
					require.NoError(t, os.WriteFile(p.Destination.Name, []byte("invalid"), 0600))
				}
			case "directory source":
				p.Source.Name = directory
			case "hardlink":
				require.NoError(t, os.Link(p.Source.Name, p.Destination.Name))
			case "bad header":
				require.NoError(t, os.WriteFile(p.Source.Name, []byte("invalid"), 0600))
			case "bad checkpoint":
				data, _ := json.Marshal(Checkpoint{Version: 50})
				require.NoError(t, os.WriteFile(checkpointPath(directory, p.Group), data, 0600))
			case "missing directory":
				missing := filepath.Join(directory, "missing")
				limits.CheckpointDir = &missing
			}
			before := directoryContents(t, directory)
			// When inspecting, then reject with exit 2 without creating, repairing or rewriting any file.
			var err error
			if destination {
				_, err = inspector.Destination(p, "query", progress, limits)
			} else {
				_, err = inspector.Source(p, "query", limits)
			}
			var classified interface{ ExitCode() int }
			require.ErrorAs(t, err, &classified)
			require.Equal(t, 2, classified.ExitCode())
			require.Equal(t, before, directoryContents(t, directory), "invalid-state inspection does not change files")
		})
	}
}
