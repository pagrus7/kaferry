package delivery

import (
	"github.com/pagrus7/kaferry/internal/archive/replay"
	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCheckpointRejectsIncompatibleOrIncompleteAuthority(t *testing.T) {
	// Given a complete versioned cursor bound to a query and both endpoint identities.
	session := New(plan.Plan{Source: plan.Endpoint{Kind: "TOPIC"}}, config.Defaults("").Worker)
	session.Identity = Identity{Group: "job", Fingerprint: "query", SourceTopic: [16]byte{1}, DestinationTopic: [16]byte{2}, ProgressTopic: [16]byte{3}}
	valid := Frame{Version: Version, Identity: session.Identity, Cursor: core.Cursor{Partition: 2, Position: 7}, Metadata: "encoded source metadata"}
	cases := []struct {
		name  string
		alter func(*Frame)
	}{
		{"unversioned", func(f *Frame) { f.Version = 0 }},
		{"unsupported version", func(f *Frame) { f.Version++ }},
		{"another query", func(f *Frame) { f.Identity.Fingerprint = "another" }},
		{"recreated destination", func(f *Frame) { f.Identity.DestinationTopic = [16]byte{9} }},
		{"negative cursor", func(f *Frame) { f.Cursor.Position = -1 }},
		{"negative partition", func(f *Frame) { f.Cursor.Partition = -1 }},
		{"missing metadata", func(f *Frame) { f.Metadata = "" }},
		{"mismatched source", func(f *Frame) { f.Replay = &replay.State{Position: 7} }},
		{"beyond end", func(f *Frame) { f.Cursor.End = new(int64(6)) }},
		{"stop beyond cursor", func(f *Frame) { f.Cursor.Stop = &core.Stop{At: 8} }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := valid
			c.alter(&f)
			encoded, err := Encode(f)
			require.NoError(t, err)
			// When authority is decoded, then unusable state cannot be mistaken for a fresh job.
			_, err = session.Decode(encoded)
			require.Error(t, err)
		})
	}
	data, err := Encode(valid)
	require.NoError(t, err)
	decoded, err := session.Decode(data)
	require.NoError(t, err)
	require.Equal(t, valid, decoded)
}
func TestReplayAuthorityRequiresItsPhysicalCursor(t *testing.T) {
	// Given archive input whose physical occurrences can share identical source offsets and values.
	s := New(plan.Plan{Source: plan.Endpoint{Kind: "ARCHIVE"}}, config.Defaults("").Worker)
	f := Frame{Version: Version, Identity: s.Identity, Cursor: core.Cursor{Position: 4}}
	// When its physical checkpoint is absent or disagrees, then recovery refuses it.
	require.Error(t, s.Validate(f))
	f.Replay = &replay.State{Version: 1, Group: s.Identity.Group, Fingerprint: s.Identity.Fingerprint, Files: []replay.Progress{}, Position: 3}
	require.Error(t, s.Validate(f))
	f.Replay.Position = 4
	require.NoError(t, s.Validate(f))
}
