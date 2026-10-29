package replay

import (
	"testing"

	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
)

func TestCheckSavedValidatesQueryBeforeReadingFixedSelection(t *testing.T) {
	for _, tc := range []struct {
		name    string
		change  func(*Query, *State)
		message string
	}{
		{"matching query", func(*Query, *State) {}, ""},
		{"different group", func(q *Query, _ *State) { q.Group = "other" }, "query identity mismatch"},
		{"different fingerprint", func(q *Query, _ *State) { q.Fingerprint = "other" }, "query identity mismatch"},
		{"different window", func(q *Query, _ *State) {
			q.Window.Lower = &plan.Expression{LiteralMS: 10}
		}, "stored time window"},
		{"excluded partition", func(q *Query, _ *State) {
			q.Partitions = &plan.PartitionFilter{IDs: []int32{1}}
		}, "excluded original partition"},
		{"destination layout", func(q *Query, s *State) {
			q.PartitionCount = 4
			s.Files[0].File.Identity.Source.Partition = 5
		}, "original partition 5"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given saved progress and a catalogue whose files cannot be validated.
			q, saved := query(), partialState()
			tc.change(&q, &saved)
			inputs := &memorySnapshots{validateErr: errDisk}

			// When checking the saved selection against the query.
			err := q.CheckSaved(saved, inputs)

			// Then query conflicts precede file reads; compatible progress uses
			// the saved files without selecting the current directory again.
			if tc.message != "" {
				require.ErrorContains(t, err, tc.message)
				require.Zero(t, inputs.validates)
			} else {
				require.ErrorIs(t, err, errDisk)
				require.Equal(t, 1, inputs.validates)
				require.Equal(t, []File{saved.Files[0].File}, inputs.validated)
			}
			require.Zero(t, inputs.selects)
		})
	}
}

func TestCheckSavedCompletedSelectionNeedsNoFiles(t *testing.T) {
	// Given completed saved progress whose selected file is no longer readable.
	saved := partialState()
	saved.Complete = true
	saved.Files[0].Next = Position{Block: saved.Files[0].File.Size, Previous: 43}
	require.NoError(t, saved.validate())
	inputs := &memorySnapshots{selectErr: errDisk, validateErr: errDisk}

	// When checking it for resume or inspection.
	err := query().CheckSaved(saved, inputs)

	// Then completion is preserved without accessing the catalogue.
	require.NoError(t, err)
	require.Zero(t, inputs.selects)
	require.Zero(t, inputs.validates)
}
