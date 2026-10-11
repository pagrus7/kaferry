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
			if err := os.WriteFile(path, before, 0600); err != nil {
				t.Fatal(err)
			}
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
	valid := []byte(`{"version":1,"fingerprint":"fp","source_id":"source","position":0}`)
	for _, tc := range []struct{ name, data string }{
		{"oversized", strings.Repeat("x", 65537)},
		{"trailing JSON", string(valid) + `{}`},
		{"unknown field", `{"version":1,"fingerprint":"fp","source_id":"source","position":0,"extra":1}`},
		{"unsupported version", `{"version":2,"fingerprint":"fp","source_id":"source","position":0}`},
		{"negative position", `{"version":1,"fingerprint":"fp","source_id":"source","position":-1}`},
		{"destination without extent", `{"version":1,"fingerprint":"fp","source_id":"source","position":0,"destination_id":"output"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given an invalid saved checkpoint for this group.
			dir := t.TempDir()
			path := checkpointPath(dir, "group")
			if err := os.WriteFile(path, []byte(tc.data), 0600); err != nil {
				t.Fatal(err)
			}

			// When opening progress for a resumed query.
			cp, err := NewCheckpoints(dir, "group", DurableWriter{})

			// Then corruption is a conflict, and repairing the file permits a new owner.
			var conflict *Conflict
			if cp != nil || !errors.As(err, &conflict) {
				t.Fatal("corrupt checkpoint treated as new progress", cp, err)
			}
			if err := os.WriteFile(path, valid, 0600); err != nil {
				t.Fatal(err)
			}
			cp, err = NewCheckpoints(dir, "group", DurableWriter{})
			if err != nil {
				t.Fatal("failed open retained group lock", err)
			}
			cp.Close()
		})
	}
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
			if err != nil {
				t.Fatal(err)
			}
			defer cp.Close()
			cp.Initialize(Checkpoint{Version: 1, Fingerprint: "fp", SourceID: "source"})
			if _, err := cp.Save(t.Context(), []core.Cursor{{Partition: 0}}); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(cp.path)
			if err != nil {
				t.Fatal(err)
			}
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
