package replay

import (
	"testing"

	"github.com/pagrus7/kaferry/internal/archive/avro"
	"github.com/stretchr/testify/require"
)

func TestReplayValidatesOriginalPartitionsBeforeSavingSelection(t *testing.T) {
	// Given an archive with an original partition beyond the destination layout.
	r := record(0)
	r.Source.Partition = 5
	file := descriptor(1, []avro.Record{r})
	inputs := &memorySnapshots{files: []File{file}}
	store := &memoryCheckpoints{}
	blocks := &memoryBlocks{values: map[string][]avro.Record{file.Identity.FileID: {r}}}
	q := query()
	q.PartitionCount = 4
	// When selecting it for replay, then reject it before saving progress or reading records.
	_, err := NewSession(q, clock{}, store, inputs, blocks, 0)
	require.ErrorContains(t, err, "original partition 5")
	require.Empty(t, store.writes)
	require.Zero(t, blocks.reads)
	// When the destination is expanded, then the same selection can start and retains provenance.
	q.PartitionCount = 8
	s, err := NewSession(q, clock{}, store, inputs, blocks, 0)
	require.NoError(t, err)
	defer s.Close()
	item, err := s.Next()
	require.NoError(t, err)
	require.EqualValues(t, 5, item.Record.Source.Partition)
	require.Len(t, store.writes, 1)
}

func TestReplayValidatesSavedSelectionInsteadOfReselecting(t *testing.T) {
	// Given saved input from partition 5 and a changed directory now containing partition 0.
	r := record(0)
	r.Source.Partition = 5
	file := descriptor(1, []avro.Record{r})
	inputs := &memorySnapshots{files: []File{file}}
	store := &memoryCheckpoints{}
	s, err := NewSession(query(), clock{}, store, inputs, &memoryBlocks{}, 0)
	require.NoError(t, err)
	require.NoError(t, s.Close())
	saved := store.saved.clone()
	inputs.files = []File{descriptor(2, records(0))}
	inputs.selects = 0
	q := query()
	q.PartitionCount = 4
	// When resuming into too few partitions, then reject the fixed selection without replacing progress.
	_, err = NewSession(q, clock{}, store, inputs, &memoryBlocks{}, 0)
	require.ErrorContains(t, err, "original partition 5")
	require.Equal(t, saved, *store.saved)
	require.Len(t, store.writes, 1)
	require.Zero(t, inputs.selects)
	q.PartitionCount = -1
	_, err = NewSession(q, clock{}, store, inputs, &memoryBlocks{}, 0)
	require.ErrorContains(t, err, "partition count must not be negative")
}
