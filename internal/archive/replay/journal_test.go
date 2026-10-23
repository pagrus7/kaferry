package replay

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/checkpoint"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func emptyState() State {
	return State{Version: 1, Group: query().Group, Fingerprint: query().Fingerprint, Complete: true, Files: []Progress{}}
}

func TestGroupLockSurvivesCheckpointReplacementAndReleasesOnClose(t *testing.T) {
	// Given an exclusively locked replay group and an empty pinned selection.
	directory := t.TempDir()
	j, err := NewJournal(directory, query().Group, NativeGroupFiles{}, checkpoint.DurableWriter{})
	require.NoError(t, err)
	defer j.Close()
	before, err := os.Stat(j.path + ".lock")
	require.NoError(t, err)
	// When repeated atomic saves replace the checkpoint, then the same stable lock excludes another owner.
	for range 2 {
		require.NoError(t, j.Save(emptyState()))
		_, err := NewJournal(directory, query().Group, NativeGroupFiles{}, checkpoint.DurableWriter{})
		require.ErrorIs(t, err, ErrGroupBusy)
	}
	after, err := os.Stat(j.path + ".lock")
	require.NoError(t, err)
	require.True(t, os.SameFile(before, after))
	require.NoError(t, j.Close())
	require.NoError(t, j.Close())
	// Then a new owner can acquire and load the pinned state.
	next, err := NewJournal(directory, query().Group, NativeGroupFiles{}, checkpoint.DurableWriter{})
	require.NoError(t, err)
	defer next.Close()
	got, err := next.Load()
	require.NoError(t, err)
	require.Equal(t, emptyState(), *got)
	_, err = j.Load()
	require.Error(t, err)
	require.Error(t, j.Save(emptyState()))
}

func TestCheckpointRejectsCorruptionAmbiguityAndInvalidState(t *testing.T) {
	good, err := checkpoint.Encode(emptyState(), maxCheckpointBytes)
	require.NoError(t, err)
	for _, kind := range []string{"checksum", "whitespace", "unknown field", "unknown version", "wrong group", "nil files", "completion", "negative sequence", "bad file", "bad digest", "bad start", "bad size", "bad cursor", "duplicate file", "window"} {
		t.Run(kind, func(t *testing.T) {
			// Given a corrupt or semantically invalid replay checkpoint.
			j, err := NewJournal(t.TempDir(), query().Group, NativeGroupFiles{}, checkpoint.DurableWriter{})
			require.NoError(t, err)
			defer j.Close()
			state := emptyState()
			data := append([]byte(nil), good...)
			switch kind {
			case "checksum":
				data = bytes.Replace(data, []byte("query-fingerprint"), []byte("query-fingerprinx"), 1)
			case "whitespace":
				data = append([]byte(" "), data...)
			case "unknown field":
				data = bytes.Replace(data, []byte(`"sha256":`), []byte(`"other":0,"sha256":`), 1)
			case "unknown version":
				state.Version = 2
			case "wrong group":
				state.Group = "other"
			case "nil files":
				state.Files = nil
			case "completion":
				state.Complete = false
			case "negative sequence":
				state.Position = -1
			case "window":
				state.Window.Lower = new(int64(2))
				state.Window.Upper = new(int64(1))
			default:
				f := descriptor(1, records(42, 43))
				state.Complete = false
				state.Files = []Progress{{File: f, Next: initial(f)}}
				switch kind {
				case "bad file":
					state.Files[0].File.Identity.FileID = "../bad"
				case "bad digest":
					state.Files[0].File.SHA256 = "not-a-digest"
				case "bad start":
					state.Files[0].File.Start = 0
				case "bad size":
					state.Files[0].File.Size = state.Files[0].File.Start
				case "bad cursor":
					state.Files[0].Next.Record = -1
				case "duplicate file":
					state.Files = append(state.Files, state.Files[0])
				}
			}
			if kind != "checksum" && kind != "whitespace" && kind != "unknown field" {
				data, err = checkpoint.Encode(state, maxCheckpointBytes)
				require.NoError(t, err)
			}
			require.NoError(t, os.WriteFile(j.path, data, 0600))
			// When loaded, then it fails rather than inventing an empty or advanced selection.
			_, err = j.Load()
			require.Error(t, err)
			require.ErrorContains(t, err, "replay checkpoint "+j.path)
		})
	}
}

func TestCheckpointReadSizeLimitAndSafeNames(t *testing.T) {
	// Given an unsafe-looking group name and a checkpoint larger than the read cap.
	dir := t.TempDir()
	group := "../../orders/" + strings.Repeat("x", 100)
	j, err := NewJournal(dir, group, NativeGroupFiles{}, checkpoint.DurableWriter{})
	require.NoError(t, err)
	defer j.Close()
	require.Equal(t, dir, filepath.Dir(j.path))
	f, err := os.Create(j.path)
	require.NoError(t, err)
	require.NoError(t, f.Truncate(maxCheckpointBytes+1))
	require.NoError(t, f.Close())
	// When read, then the size check rejects it before allocating its contents.
	_, err = j.Load()
	require.ErrorContains(t, err, "size limit")
	_, err = NewJournal(dir, "", NativeGroupFiles{}, checkpoint.DurableWriter{})
	require.Error(t, err)
	// A missing checkpoint is distinct from malformed state.
	require.NoError(t, os.Remove(j.path))
	saved, err := j.Load()
	require.NoError(t, err)
	require.Nil(t, saved)
}

func TestGroupRefusesSymlinkedCheckpointAndLock(t *testing.T) {
	// Given a checkpoint or lock path replaced by a symlink to an unrelated file.
	for _, suffix := range []string{"", ".lock"} {
		t.Run(suffix, func(t *testing.T) {
			dir := t.TempDir()
			target := filepath.Join(t.TempDir(), "keep")
			require.NoError(t, os.WriteFile(target, []byte("untouched"), 0600))
			path := checkpoint.Path(dir, query().Group)
			require.NoError(t, os.Symlink(target, path+suffix))
			// When opening/reading the group, then it fails without following the symlink.
			j, err := NewJournal(dir, query().Group, NativeGroupFiles{}, checkpoint.DurableWriter{})
			if suffix == ".lock" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				defer j.Close()
				_, err = j.Load()
				require.Error(t, err)
			}
			data, err := os.ReadFile(target)
			require.NoError(t, err)
			require.Equal(t, "untouched", string(data))
		})
	}
}

func TestSpecialCheckpointFilesDoNotBlock(t *testing.T) {
	if dir := os.Getenv("KAFERRY_REPLAY_FIFO_TEST"); dir != "" {
		// The subprocess checks both read and lock paths under its parent's deadline.
		path := filepath.Join(dir, "fifo")
		require.NoError(t, unix.Mkfifo(path, 0600))
		_, err := (NativeGroupFiles{}).Read(path, maxCheckpointBytes)
		require.ErrorContains(t, err, "regular file")
		_, err = (NativeGroupFiles{}).Acquire(path)
		require.ErrorContains(t, err, "regular file")
		return
	}
	// Given a FIFO that would block a normal read-only open.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSpecialCheckpointFilesDoNotBlock$")
	cmd.Env = append(os.Environ(), "KAFERRY_REPLAY_FIFO_TEST="+t.TempDir())
	// When the child reads or locks it, then both paths reject it before the deadline.
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "child: %s", output)
	require.NoError(t, ctx.Err())
}

func FuzzCheckpoint(f *testing.F) {
	seed, err := checkpoint.Encode(emptyState(), maxCheckpointBytes)
	require.NoError(f, err)
	f.Add(seed)
	payload, err := json.Marshal(partialState())
	require.NoError(f, err)
	f.Add(payload)
	f.Add([]byte("{}"))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 64<<10 {
			t.Skip()
		}
		wires := [][]byte{data}
		var payload bytes.Buffer
		if json.Compact(&payload, data) == nil {
			// Recompute the envelope so state mutations reach cursor and identity validation.
			wires = append(wires, fmt.Appendf(nil, `{"state":%s,"sha256":"%x"}`, payload.Bytes(), sha256.Sum256(payload.Bytes())))
		}
		for _, wire := range wires {
			state, err := checkpoint.Decode[State](wire, maxCheckpointBytes)
			if err == nil {
				_ = state.validate()
			}
		}
	})
}

func TestCheckpointWireRoundTripIsCanonical(t *testing.T) {
	// Given an empty pinned group, when encoded, then the on-disk wire bytes stay stable.
	data, err := checkpoint.Encode(emptyState(), maxCheckpointBytes)
	require.NoError(t, err)
	require.Equal(t, `{"state":{"version":1,"group":"replay-test","fingerprint":"query-fingerprint","window":{"lower":null,"upper":null},"position":0,"complete":true,"files":[]},"sha256":"9b69f4869f5bab00ef0f58579c54ec81dbc4f2ba8116b960089abadfbaad27fc"}`, string(data))
	got, err := checkpoint.Decode[State](data, maxCheckpointBytes)
	require.NoError(t, err)
	require.Equal(t, emptyState(), got)
	_, err = checkpoint.Decode[State](append(data, []byte("{}")...), maxCheckpointBytes)
	require.Error(t, err)
}

func TestRejectedSavePreservesExistingCheckpoint(t *testing.T) {
	// Given a valid checkpoint whose bytes must survive an invalid replacement request.
	j, err := NewJournal(t.TempDir(), query().Group, NativeGroupFiles{}, checkpoint.DurableWriter{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, j.Close()) })
	require.NoError(t, j.Save(emptyState()))
	before, err := os.ReadFile(j.path)
	require.NoError(t, err)
	for _, invalid := range []State{
		{Version: 2},
		{Version: 1, Group: "foreign-group", Fingerprint: query().Fingerprint, Complete: true, Files: []Progress{}},
	} {
		// When saving invalid or foreign-group state, then the previous durable bytes remain intact.
		require.Error(t, j.Save(invalid))
		after, err := os.ReadFile(j.path)
		require.NoError(t, err)
		require.Equal(t, before, after)
	}
}

func TestPartialCheckpointWireContract(t *testing.T) {
	// Given a confirmed record inside a selected file's block and a literal wire fixture.
	want, err := os.ReadFile("testdata/partial-checkpoint-v1.json")
	require.NoError(t, err)
	// When encoding or loading, then all per-file field names and physical cursor values stay stable.
	data, err := checkpoint.Encode(partialState(), maxCheckpointBytes)
	require.NoError(t, err)
	require.Equal(t, want, data)
	got, err := checkpoint.Decode[State](want, maxCheckpointBytes)
	require.NoError(t, err)
	require.Equal(t, partialState(), got)
}
