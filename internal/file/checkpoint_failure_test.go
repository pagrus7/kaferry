package file

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/stretchr/testify/require"
)

type checkpointFaultFS struct {
	nativeReplacement
	stage   string
	failure error
}

func (f checkpointFaultFS) CreateTemp(dir, pattern string) (*os.File, error) {
	if f.stage == "create" {
		return nil, f.failure
	}
	return f.nativeReplacement.CreateTemp(dir, pattern)
}
func (f checkpointFaultFS) Write(file *os.File, data []byte) error {
	if f.stage == "write" {
		_, _ = file.Write(data[:len(data)/2])
		return f.failure
	}
	return f.nativeReplacement.Write(file, data)
}
func (f checkpointFaultFS) Sync(file *os.File) error {
	if f.stage == "sync" {
		return f.failure
	}
	return f.nativeReplacement.Sync(file)
}
func (f checkpointFaultFS) Rename(old, new string) error {
	if f.stage == "rename" {
		return f.failure
	}
	return f.nativeReplacement.Rename(old, new)
}
func (f checkpointFaultFS) SyncDirectory(path string) error {
	if f.stage == "directory sync" {
		return f.failure
	}
	return f.nativeReplacement.SyncDirectory(path)
}

func TestCheckpointReplacementFailuresPreserveAtomicPublication(t *testing.T) {
	for _, stage := range []string{"create", "write", "sync", "rename", "directory sync"} {
		t.Run(stage, func(t *testing.T) {
			// Given an existing checkpoint and a failure at one replacement boundary.
			path := filepath.Join(t.TempDir(), "checkpoint.json")
			before, after := []byte("old checkpoint"), []byte("new checkpoint")
			require.NoError(t, os.WriteFile(path, before, 0600))
			failure := errors.New("checkpoint I/O failed")

			// When attempting the atomic replacement.
			err := replaceCheckpoint(checkpointFaultFS{stage: stage, failure: failure}, path, after)

			// Then failures before rename retain the old bytes; failed directory sync
			// leaves the complete new file visible but does not certify its durability.
			want := before
			if stage == "directory sync" {
				want = after
			}
			got, readErr := os.ReadFile(path)
			temporary, globErr := filepath.Glob(filepath.Join(filepath.Dir(path), ".checkpoint-*"))
			if !errors.Is(err, failure) || readErr != nil || !bytes.Equal(got, want) || globErr != nil || len(temporary) != 0 {
				t.Fatal("replacement lost atomicity or temporary cleanup", err, readErr, got, temporary, globErr)
			}
		})
	}
}

func TestCorruptCheckpointFailsAndReleasesLock(t *testing.T) {
	valid := []byte(`{"version":2,"fingerprint":"fp","source_id":"source","position":0}`)
	for _, tc := range []struct{ name, data string }{
		{"oversized", strings.Repeat("x", 65537)},
		{"trailing JSON", string(valid) + `{}`},
		{"unknown field", `{"version":2,"fingerprint":"fp","source_id":"source","position":0,"extra":1}`},
		{"unsupported version", `{"version":3,"fingerprint":"fp","source_id":"source","position":0}`},
		{"opening size below position", `{"version":2,"fingerprint":"fp","source_id":"source","position":1,"opening_size":0}`},
		{"wrong stop reason", `{"version":2,"fingerprint":"fp","source_id":"source","position":0,"stop":{"reason":"tip","at":0}}`},
		{"different stop position", `{"version":2,"fingerprint":"fp","source_id":"source","position":0,"stop":{"reason":"eof","at":1}}`},
		{"negative position", `{"version":2,"fingerprint":"fp","source_id":"source","position":-1}`},
		{"destination without extent", `{"version":2,"fingerprint":"fp","source_id":"source","position":0,"destination_id":"output"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given an invalid saved checkpoint for this group.
			dir := t.TempDir()
			path := checkpointPath(dir, "group")
			require.NoError(t, os.WriteFile(path, []byte(tc.data), 0600))

			// When opening progress for a resumed query.
			cp, err := NewCheckpoints(dir, "group", DurableWriter{})

			// Then corruption is a conflict, and repairing the file permits a new owner.
			var conflict *Conflict
			if cp != nil || !errors.As(err, &conflict) {
				t.Fatal("corrupt checkpoint treated as new progress", cp, err)
			}
			require.NoError(t, os.WriteFile(path, valid, 0600))
			cp, err = NewCheckpoints(dir, "group", DurableWriter{})
			if err != nil {
				t.Fatal("failed open retained group lock", err)
			}
			cp.Close()
		})
	}
}

func TestUnreadableCheckpointFailsAndReleasesLock(t *testing.T) {
	// Given a checkpoint path that cannot be read as a file.
	dir := t.TempDir()
	path := checkpointPath(dir, "group")
	require.NoError(t, os.Mkdir(path, 0700))

	// When a job attempts to resume that group's progress.
	store, err := NewCheckpoints(dir, "group", DurableWriter{})

	// Then a read failure cannot silently initialize fresh progress or alter the path.
	if store != nil {
		store.Close()
		t.Fatal("unreadable checkpoint accepted")
	}
	info, statErr := os.Stat(path)
	if err == nil || statErr != nil || !info.IsDir() {
		t.Fatal("read failure was ignored or changed the checkpoint", err, statErr)
	}
	require.NoError(t, os.Remove(path))

	// When the path is repaired, then a new owner can acquire the released lock.
	store, err = NewCheckpoints(dir, "group", DurableWriter{})
	if err != nil {
		t.Fatal("failed read retained the group lock", err)
	}
	defer store.Close()
}

func TestCheckpointRejectsInvalidSaveWithoutChangingDurableProgress(t *testing.T) {
	for _, tc := range []struct {
		name    string
		cursors []core.Cursor
		cancel  bool
	}{
		{"canceled", []core.Cursor{{Partition: 0}}, true},
		{"missing cursor", nil, false},
		{"multiple cursors", []core.Cursor{{Partition: 0}, {Partition: 1}}, false},
		{"foreign partition", []core.Cursor{{Partition: 1}}, false},
		{"unknown frame boundary", []core.Cursor{{Position: 123}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given durable progress at the initial frame boundary.
			cp, err := NewCheckpoints(t.TempDir(), "group", DurableWriter{})
			require.NoError(t, err)
			defer cp.Close()
			cp.Initialize(Checkpoint{Version: 2, OpeningSize: 0, Fingerprint: "fp", SourceID: "source"})
			if _, err := cp.Save(t.Context(), []core.Cursor{{Partition: 0}}); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(cp.path)
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.cancel {
				cancel()
			}

			// When saving canceled or invalid progress.
			_, err = cp.Save(ctx, tc.cursors)

			// Then saved progress and file bytes remain unchanged.
			after, readErr := os.ReadFile(cp.path)
			if err == nil || tc.cancel && !errors.Is(err, context.Canceled) || readErr != nil || !bytes.Equal(before, after) || cp.Existing().Position != 0 {
				t.Fatal("invalid save advanced progress", err, readErr)
			}
		})
	}
}

type checkpointOutput struct {
	id     string
	extent int64
}

func (o checkpointOutput) DurableOutput() (string, int64) { return o.id, o.extent }
func TestInvalidCheckpointSavePreservesSavedBytes(t *testing.T) {
	for _, mode := range []string{"stop reason", "stop position", "opening size", "output extent"} {
		t.Run(mode, func(t *testing.T) {
			// Given a valid checkpoint at a known source boundary.
			cp, err := NewCheckpoints(t.TempDir(), "group", DurableWriter{})
			require.NoError(t, err)
			defer cp.Close()
			cp.Initialize(Checkpoint{Version: 2, Fingerprint: "fp", SourceID: "source"})
			if _, err := cp.Save(t.Context(), []core.Cursor{{}}); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(cp.path)
			require.NoError(t, err)
			cursor := core.Cursor{}
			switch mode {
			case "stop reason":
				cursor.Stop = &core.Stop{Reason: "tip"}
			case "stop position":
				cursor.Stop = &core.Stop{Reason: "eof", At: 1}
			case "opening size":
				cp.RememberFrame(1, FrameIdentity{})
				cursor.Position = 1
			case "output extent":
				cp.BindOutput(checkpointOutput{id: "file", extent: 0})
			}
			// When an invalid stop, source extent or output snapshot is presented.
			_, err = cp.Save(t.Context(), []core.Cursor{cursor})
			// Then the previous checkpoint remains unchanged and readable.
			var conflict *Conflict
			after, readErr := os.ReadFile(cp.path)
			saved, savedErr := readCheckpoint(cp.path)
			if !errors.As(err, &conflict) || readErr != nil || !bytes.Equal(before, after) || savedErr != nil || saved.Position != 0 || saved.Stop != nil {
				t.Fatal(err, readErr, saved, savedErr)
			}
		})
	}
}
