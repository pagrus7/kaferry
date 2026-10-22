package capture

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pagrus7/kaferry/internal/archive/avro"
	"github.com/pagrus7/kaferry/internal/file"
	"github.com/stretchr/testify/require"
)

const emptyPayload = `{"version":1,"writer_id":"0123456789abcdef0123456789abcdef","files":{}}`

func stateWire(payload string) []byte {
	return fmt.Appendf(nil, `{"state":%s,"sha256":"%x"}`, payload, sha256.Sum256([]byte(payload)))
}

func TestStateWireContract(t *testing.T) {
	// Given the canonical empty writer state, when encoded, then its wire bytes are stable.
	want := `{"state":{"version":1,"writer_id":"0123456789abcdef0123456789abcdef","files":{}},"sha256":"e4a3af4ec77d0c6be248581b50bf6926858f30c07efb0834c5cfa856634468c0"}`
	state := emptyState("0123456789abcdef0123456789abcdef")
	data, err := encodeState(state)
	require.NoError(t, err)
	require.Equal(t, want, string(data))
	got, err := decodeState(data)
	require.NoError(t, err)
	require.Equal(t, state, got)
}

func TestStateRejectsAmbiguityCorruptionAndUnknownVersions(t *testing.T) {
	for name, wire := range map[string][]byte{
		"missing": nil, "empty": {}, "whitespace": []byte(" \n"), "null": []byte("null"),
		"empty envelope": []byte("{}"), "trailing": append(stateWire(emptyPayload), []byte("{}")...),
		"noncanonical envelope": append([]byte(" "), stateWire(emptyPayload)...),
		"checksum":              bytes.Replace(stateWire(emptyPayload), []byte("0123456789abcdef"), []byte("1123456789abcdef"), 1),
		"wrong payload type":    stateWire(`[]`),
		"wrong field type":      stateWire(strings.Replace(emptyPayload, `"version":1`, `"version":"1"`, 1)),
		"unknown version":       stateWire(strings.Replace(emptyPayload, `"version":1`, `"version":2`, 1)),
		"uppercase ID":          stateWire(strings.ReplaceAll(emptyPayload, "abcdef", "ABCDEF")),
		"null files":            stateWire(strings.Replace(emptyPayload, `"files":{}`, `"files":null`, 1)),
		"duplicate":             stateWire(strings.Replace(emptyPayload, `"version":1`, `"version":1,"version":1`, 1)),
		"unknown field":         stateWire(strings.Replace(emptyPayload, `"version":1`, `"unexpected":0,"version":1`, 1)),
		"alternate spelling":    stateWire(strings.Replace(emptyPayload, "writer_id", "WRITER_ID", 1)),
	} {
		t.Run(name, func(t *testing.T) {
			// Given damaged or ambiguous required state, when decoded, then recovery cannot treat it as empty.
			_, err := decodeState(wire)
			require.Error(t, err)
			require.NotErrorIs(t, err, io.EOF)
		})
	}
}

func TestJournalRoundTripAndInvalidBoundaries(t *testing.T) {
	// Given a real checksummed journal containing one acknowledged boundary.
	root := t.TempDir()
	s, boundary := savedPrefix(t, root)
	got, err := s.states.Load(s.statePath())
	require.NoError(t, err)
	require.Equal(t, s.state, got)
	for _, change := range []func(*Boundary){
		func(b *Boundary) { b.Extent = 0 }, func(b *Boundary) { b.Records = 0 },
		func(b *Boundary) { b.Identity.FileID = "../invalid" }, func(b *Boundary) { b.Identity.WriterID = "wrong" },
	} {
		// When a caller supplies an invalid boundary, then saving fails before replacing protected state.
		bad := boundary
		change(&bad)
		state := emptyState(s.state.WriterID)
		state.Files[boundary.Identity.FileID] = bad
		require.Error(t, s.states.Save(s.statePath(), state))
		again, err := s.states.Load(s.statePath())
		require.NoError(t, err)
		require.Equal(t, got, again)
	}
}

func TestJournalReadAndWriteSizeBoundsMatch(t *testing.T) {
	// Given enough legal maximum-length source identities to exceed the saved-state limit.
	state := emptyState("0123456789abcdef0123456789abcdef")
	for i := range 700 {
		id := fmt.Sprintf("%032x", i)
		identity := avro.Identity{Version: 1, WriterID: state.WriterID, FileID: id, Source: testRecord(42).Source, FirstOffset: 42, BlockLimit: 8 << 20}
		identity.Source.Cluster = strings.Repeat("\x00", 4096)
		state.Files[id] = Boundary{Identity: identity, Extent: 1000, Records: 1}
	}
	fs := &observedFiles{Filesystem: NativeFiles{}}
	journal := NewJournal(fs, &observedWriter{AtomicWriter: file.DurableWriter{}, files: fs})
	path := filepath.Join(t.TempDir(), "state.json")
	// When saved, then it is rejected before disk replacement, never creating unrecoverable state.
	err := journal.Save(path, state)
	require.ErrorContains(t, err, "exceeds")
	require.ErrorContains(t, err, "archive writer state "+path)
	require.Empty(t, fs.events)
	require.NoFileExists(t, path)
	// When an oversized file exists on disk, then loading rejects it before reading its contents.
	f, err := os.Create(path)
	require.NoError(t, err)
	require.NoError(t, f.Truncate(maxStateBytes+1))
	require.NoError(t, f.Close())
	_, err = journal.Load(path)
	require.ErrorContains(t, err, "size limit")
	require.ErrorContains(t, err, "archive writer state "+path)
	_, err = journal.Load(filepath.Dir(path))
	require.ErrorContains(t, err, "regular file")
}

func FuzzState(f *testing.F) {
	f.Add(stateWire(emptyPayload))
	f.Add([]byte("{}"))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 64<<10 {
			t.Skip()
		}
		_, _ = decodeState(data)
	})
}
