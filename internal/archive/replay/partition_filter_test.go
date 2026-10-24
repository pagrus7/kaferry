package replay

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
)

func TestPartitionSelectionNeverReadsExcludedPayloads(t *testing.T) {
	for _, filter := range []*plan.PartitionFilter{{IDs: []int32{0}}, {Exclude: true, IDs: []int32{5}}} {
		// Given two original partitions and unreadable payload bytes in the excluded file.
		root := t.TempDir()
		selected := writeFile(t, root, 1, records(0, 1), 1)
		other := records(0)
		other[0].Source.Partition = 5
		excluded := writeFile(t, root, 2, other, 0)
		fs := &faultyFiles{Filesystem: NativeFiles{}, fault: "hash", target: excluded.name(), start: excluded.Start}
		catalog := NewCatalog(root, codec(t), fs)
		// When inspecting and selecting, then only matching files contribute data and metadata.
		preview, err := catalog.Inspect(filter)
		require.NoError(t, err)
		require.Len(t, preview.Files, 1)
		require.Equal(t, selected.Identity, preview.Files[0].Identity)
		require.Empty(t, preview.Files[0].SHA256)
		selection, err := catalog.Select(filter)
		require.NoError(t, err)
		require.Equal(t, []File{selected}, selection.Files)
		require.Zero(t, fs.active)
		// The same unreadable bytes still fail an unfiltered run.
		_, err = catalog.Select(nil)
		require.ErrorIs(t, err, errDisk)
		require.Zero(t, fs.active)
	}
}

func TestExcludedArchiveHeadersStillRequireAValidFileIdentity(t *testing.T) {
	// Given an excluded partition whose completed filename disagrees with its otherwise valid header.
	root := t.TempDir()
	writeFile(t, root, 1, records(0), 0)
	other := records(0)
	other[0].Source.Partition = 5
	excluded := writeFile(t, root, 2, other, 0)
	require.NoError(t, os.Rename(filepath.Join(root, excluded.name()), filepath.Join(root, "renamed.avro")))
	filter := &plan.PartitionFilter{IDs: []int32{0}}
	catalog := NewCatalog(root, codec(t), NativeFiles{})
	// When selecting, inspecting or starting replay, then metadata validation still fails and no selection is pinned.
	_, err := catalog.Select(filter)
	require.ErrorContains(t, err, "filename disagrees with file identity")
	_, err = catalog.Inspect(filter)
	require.ErrorContains(t, err, "filename disagrees with file identity")
	q := query()
	q.Partitions = filter
	store := &memoryCheckpoints{}
	_, err = NewSession(q, clock{}, store, catalog, &memoryBlocks{}, 10)
	require.ErrorContains(t, err, "filename disagrees with file identity")
	require.Empty(t, store.writes)
}

func TestPartitionReplayPinsOnlyMatchingFilesAndKeepsThatSnapshotOnResume(t *testing.T) {
	// Given a complement predicate and multiple physical records in the selected original partition.
	root, checkpoints := t.TempDir(), t.TempDir()
	want := records(0, 1, 2)
	selected := writeFile(t, root, 1, want, 1)
	other := records(0)
	other[0].Source.Partition = 5
	excluded := writeFile(t, root, 2, other, 0)
	q := query()
	q.Partitions = &plan.PartitionFilter{Exclude: true, IDs: []int32{5}}
	q.PartitionCount = 1
	s, _ := diskSession(t, root, checkpoints, q, Limits{}, 10)
	require.Len(t, s.Saved().Files, 1)
	require.Equal(t, selected, s.Saved().Files[0].File)
	_, err := s.Next()
	require.NoError(t, err)
	require.NoError(t, s.Checkpoint(1))
	require.NoError(t, s.Close())
	// When excluded files disappear and new eligible files arrive, then resume never reselects them.
	require.NoError(t, os.Remove(filepath.Join(root, excluded.name())))
	extra := records(3)
	extra[0].Source.Partition = 1
	writeFile(t, root, 3, extra, 0)
	resumed, _ := diskSession(t, root, checkpoints, q, Limits{}, 10)
	require.Equal(t, want[1:], drain(t, resumed, true))
	require.True(t, resumed.Saved().Complete)
	require.EqualValues(t, 3, resumed.Saved().Position)
	require.NoError(t, resumed.Close())
	// A new group selects the expanded snapshot, once its listed exclusion is present again.
	writeFile(t, root, 2, other, 0)
	q.Group, q.PartitionCount = "fresh", 0
	fresh, _ := diskSession(t, root, checkpoints, q, Limits{}, 10)
	require.Equal(t, append(want, extra...), drain(t, fresh, true))
}

func TestMissingArchivePartitionIDsNeverPinASelection(t *testing.T) {
	for _, filter := range []*plan.PartitionFilter{{IDs: []int32{7}}, {Exclude: true, IDs: []int32{7}}} {
		// Given a predicate referring to an ID absent from completed-file headers.
		root := t.TempDir()
		writeFile(t, root, 1, records(0), 0)
		q := query()
		q.Partitions = filter
		store, blocks := &memoryCheckpoints{}, &memoryBlocks{}
		catalog := NewCatalog(root, codec(t), NativeFiles{})
		// When starting or previewing, then validation fails before progress is pinned or records returned.
		_, err := NewSession(q, clock{}, store, catalog, blocks, 10)
		require.ErrorContains(t, err, "absent from source metadata")
		require.Empty(t, store.writes)
		require.Zero(t, blocks.reads)
		_, err = catalog.Inspect(filter)
		require.ErrorContains(t, err, "absent from source metadata")
	}
}

func TestExcludingEveryArchivePartitionCompletesAnEmptyFixedSelection(t *testing.T) {
	// Given a valid exclusion covering every represented original partition.
	root, checkpoints := t.TempDir(), t.TempDir()
	writeFile(t, root, 1, records(0), 0)
	q := query()
	q.Partitions = &plan.PartitionFilter{Exclude: true, IDs: []int32{0}}
	s, _ := diskSession(t, root, checkpoints, q, Limits{}, 10)
	// When replaying, then EOF is a fixed empty snapshot and remains complete after additions.
	require.Empty(t, drain(t, s, true))
	require.True(t, s.Saved().Complete)
	require.Empty(t, s.Saved().Files)
	require.NoError(t, s.Close())
	extra := records(1)
	extra[0].Source.Partition = 1
	writeFile(t, root, 2, extra, 0)
	resumed, _ := diskSession(t, root, checkpoints, q, Limits{}, 10)
	require.Empty(t, drain(t, resumed, true))
}
