package plan

import (
	"testing"

	"github.com/pagrus7/kaferry/internal/sql"
	"github.com/stretchr/testify/require"
)

func TestPartitionIdentityIsCanonicalAndKeepsPositioningDefaults(t *testing.T) {
	// Given equivalent predicates with different order, duplicates and locations.
	const base = "INSERT INTO TOPIC 'b' SELECT * FROM TOPIC 'a' "
	a := build(t, base+"WHERE PARTITION IN (2,0,2)")
	b := build(t, base+"WITH(START=LATEST)\nWHERE PARTITION IN (0,2)")
	// When fingerprinting, then syntax locations and START do not change selection identity.
	x, err := a.Fingerprint("source", "dest")
	require.NoError(t, err)
	y, err := b.Fingerprint("source", "dest")
	require.NoError(t, err)
	require.Equal(t, x, y)
	require.Equal(t, "EARLIEST", a.Start)
	require.Equal(t, "LATEST", b.Start)
	require.True(t, a.OneOff)
	for _, suffix := range []string{"", "WHERE PARTITION IN (0)", "WHERE PARTITION NOT IN (0,2)"} {
		p := build(t, base+suffix)
		fp, err := p.Fingerprint("source", "dest")
		require.NoError(t, err)
		require.NotEqual(t, x, fp)
	}
}

func TestPartitionValidationIsLocatedAndRedacted(t *testing.T) {
	for _, source := range []string{"TOPIC 'a'", "FILE avro('private')"} {
		// Given an overflowing partition literal for either supported source.
		query := "INSERT INTO TOPIC 'b' SELECT * FROM " + source + "\nWHERE PARTITION IN (2147483648)"
		ast, err := sql.NewParser().Parse(query)
		require.NoError(t, err)
		// When planning, then locate the value and keep SQL values private.
		_, err = NewPlanner(environment{}).Build(ast, profiles)
		var diagnostic *sql.Diagnostic
		require.ErrorAs(t, err, &diagnostic)
		require.Equal(t, sql.Position{Line: 2, Column: 21}, diagnostic.Position)
		require.NotContains(t, err.Error(), "2147483648")
		require.NotContains(t, err.Error(), "private")
	}
}

func TestFilePartitionPredicatesUseCanonicalQueryIdentity(t *testing.T) {
	// Given equivalent file predicates with different list order and locations.
	const base = "INSERT INTO FILE avro('out') SELECT * FROM FILE avro('in') "
	a := build(t, base+"WHERE PARTITION NOT IN (5,0,5)")
	b := build(t, base+"\nWHERE PARTITION NOT IN (0,5)")
	// When planning and fingerprinting, then preserve file start rules and canonical identity.
	require.Empty(t, a.Start)
	x, err := a.Fingerprint("", "")
	require.NoError(t, err)
	y, err := b.Fingerprint("", "")
	require.NoError(t, err)
	require.Equal(t, x, y)
	for _, suffix := range []string{"", "WHERE PARTITION IN (0,5)", "WHERE PARTITION NOT IN (0)"} {
		p := build(t, base+suffix)
		fp, err := p.Fingerprint("", "")
		require.NoError(t, err)
		require.NotEqual(t, x, fp)
	}
}

func TestPartitionSelectionTracksExplicitAndComplementSets(t *testing.T) {
	for _, exclude := range []bool{false, true} {
		// Given an immutable normalized predicate and a growing topic.
		p := &PartitionFilter{Exclude: exclude, IDs: []int32{0, 2}}
		// When refreshing topology, then only NOT IN admits added IDs.
		initial, err := p.Select([]int32{0, 1, 2})
		require.NoError(t, err)
		grown, err := p.Select([]int32{0, 1, 2, 3})
		require.NoError(t, err)
		if exclude {
			require.Equal(t, []int32{1}, initial)
			require.Equal(t, []int32{1, 3}, grown)
		} else {
			require.Equal(t, []int32{0, 2}, initial)
			require.Equal(t, initial, grown)
		}
		_, err = p.Select([]int32{0, 1})
		require.ErrorContains(t, err, "absent from source metadata")
	}
}
