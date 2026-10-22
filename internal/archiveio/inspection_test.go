package archiveio

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pagrus7/kaferry/internal/archive/replay"
	"github.com/pagrus7/kaferry/internal/checkpoint"
	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
)

func TestInspectionRejectsRegularFileDestinationWithoutChangingIt(t *testing.T) {
	// Given a regular file where an archive directory is required.
	path := filepath.Join(t.TempDir(), "output")
	original := []byte("keep this file")
	require.NoError(t, os.WriteFile(path, original, 0600))
	// When inspecting, then report the conflict without overwriting the file.
	_, err := NewInspector(core.RealClock{}).Destination(path)
	var conflict *Conflict
	require.ErrorAs(t, err, &conflict)
	require.Equal(t, 2, conflict.ExitCode())
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, original, data)
}

func TestInspectionRejectsChangedSavedWindowWithoutWriting(t *testing.T) {
	// Given a checksummed checkpoint with the query's identity but an altered literal bound.
	directory := t.TempDir()
	limits := config.Defaults("").Worker
	limits.CheckpointDir = &directory
	p := plan.Plan{Source: plan.Endpoint{Kind: "FILE", Format: "avro", Name: t.TempDir()}, Destination: plan.Endpoint{Kind: "FILE", Format: "avro", Name: filepath.Join(t.TempDir(), "output")}, Group: "g", Window: plan.WindowSpec{Lower: &plan.Expression{LiteralMS: 10}}}
	fp, err := p.Fingerprint("", "")
	require.NoError(t, err)
	lower := int64(11)
	state := replay.State{Version: 1, Group: p.Group, Fingerprint: fp, Window: plan.Window{Lower: &lower}, Complete: true, Files: []replay.Progress{}}
	encoded, err := checkpoint.Encode(state, 64<<20)
	require.NoError(t, err)
	path := checkpoint.Path(directory, p.Group)
	require.NoError(t, os.WriteFile(path, encoded, 0600))
	// When inspecting, then reject the conflicting window without changing progress or creating a lock.
	_, err = NewInspector(core.RealClock{}).Source(p, fp, limits)
	require.ErrorContains(t, err, "stored literal time bound differs")
	var conflict *Conflict
	require.ErrorAs(t, err, &conflict)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, encoded, data)
	entries, err := os.ReadDir(directory)
	require.NoError(t, err)
	require.Len(t, entries, 1, "read-only inspection must not create a lock")
}
