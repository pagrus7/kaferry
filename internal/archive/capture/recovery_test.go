package capture

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/pagrus7/kaferry/internal/archive/avro"
	"github.com/pagrus7/kaferry/internal/file"
	"github.com/stretchr/testify/require"
)

func savedPrefix(t testing.TB, root string) (*Store, Boundary) {
	t.Helper()
	s := openStore(t, root, NativeFiles{}, file.DurableWriter{})
	r := testRecord(42)
	part, err := s.Create(r.Source, r.Offset)
	require.NoError(t, err)
	_, err = s.Append(part.ID, r)
	require.NoError(t, err)
	require.NoError(t, s.Checkpoint())
	return s, s.state.Files[part.ID]
}

func TestRecoveryTruncatesOnlyUnacknowledgedTail(t *testing.T) {
	// Given a saved prefix followed by a complete but never-acknowledged block.
	root := t.TempDir()
	s, boundary := savedPrefix(t, root)
	late := testRecord(43)
	late.Value = bytes.Repeat([]byte{1}, 300<<10)
	_, err := s.Append(boundary.Identity.FileID, late)
	require.NoError(t, err)
	info, err := os.Stat(dataPath(root, boundary.Identity, true))
	require.NoError(t, err)
	require.Greater(t, info.Size(), boundary.Extent)
	require.NoError(t, s.Close())
	// When another owner recovers, then only the saved record becomes visible.
	_ = openStore(t, root, NativeFiles{}, file.DurableWriter{})
	paths := completeFiles(t, root)
	require.Len(t, paths, 1)
	require.Equal(t, []avro.Record{testRecord(42)}, readFile(t, paths[0]))
	info, err = os.Stat(paths[0])
	require.NoError(t, err)
	require.Equal(t, boundary.Extent, info.Size())
}

func TestRecoveryRejectsDamagedAcknowledgedData(t *testing.T) {
	for _, damage := range []string{"missing", "shortened", "checksum", "identity", "count", "inside block", "completed longer", "completed shorter", "both names", "missing state", "corrupt state", "wrong writer"} {
		t.Run(damage, func(t *testing.T) {
			// Given one durable prefix and a damaged file or required state.
			root := t.TempDir()
			s, boundary := savedPrefix(t, root)
			require.NoError(t, s.Close())
			private := dataPath(root, boundary.Identity, true)
			completed := dataPath(root, boundary.Identity, false)
			data, err := os.ReadFile(private)
			require.NoError(t, err)
			switch damage {
			case "missing":
				require.NoError(t, os.Remove(private))
			case "shortened":
				require.NoError(t, os.Truncate(private, boundary.Extent-1))
			case "checksum":
				data[len(data)-17] ^= 1
				require.NoError(t, os.WriteFile(private, data, 0600))
			case "identity":
				data = bytes.Replace(data, []byte("orders"), []byte("broken"), 1)
				require.NoError(t, os.WriteFile(private, data, 0600))
			case "count", "inside block":
				if damage == "count" {
					boundary.Records++
				} else {
					boundary.Extent--
				}
				s.state.Files[boundary.Identity.FileID] = boundary
				require.NoError(t, s.states.Save(s.statePath(), s.state))
			case "completed longer", "completed shorter":
				require.NoError(t, os.Rename(private, completed))
				size := boundary.Extent + 1
				if damage == "completed shorter" {
					size = boundary.Extent - 1
				}
				require.NoError(t, os.Truncate(completed, size))
			case "both names":
				require.NoError(t, os.WriteFile(completed, data, 0600))
			case "missing state":
				require.NoError(t, os.Remove(s.statePath()))
			case "corrupt state":
				require.NoError(t, os.WriteFile(s.statePath(), []byte("{}"), 0600))
			case "wrong writer":
				require.NoError(t, s.states.Save(s.statePath(), emptyState("ffffffffffffffffffffffffffffffff")))
			}
			beforePrivate, _ := os.ReadFile(private)
			beforeCompleted, _ := os.ReadFile(completed)
			// When startup validates recovery, then it fails without modifying either data path.
			_, err = Open(root, testCodec(t), NativeFiles{}, NewJournal(NativeFiles{}, file.DurableWriter{}))
			require.Error(t, err)
			afterPrivate, _ := os.ReadFile(private)
			afterCompleted, _ := os.ReadFile(completed)
			require.Equal(t, beforePrivate, afterPrivate)
			require.Equal(t, beforeCompleted, afterCompleted)
		})
	}
}

func TestRecoveryHonorsRecordedCodecLimit(t *testing.T) {
	// Given a valid saved file requiring a larger codec budget than this process permits.
	root := t.TempDir()
	s, boundary := savedPrefix(t, root)
	require.NoError(t, s.Close())
	small, err := avro.New(1 << 20)
	require.NoError(t, err)
	// When recovering, then the configuration error preserves the acknowledged file.
	_, err = Open(root, small, NativeFiles{}, NewJournal(NativeFiles{}, file.DurableWriter{}))
	var limit *avro.LimitError
	require.ErrorAs(t, err, &limit)
	require.FileExists(t, dataPath(root, boundary.Identity, true))
	_ = openStore(t, root, NativeFiles{}, file.DurableWriter{})
	require.Len(t, completeFiles(t, root), 1)
}

func TestWorkersSkipBusyNamespacesAndRecoverAllReleasedOwners(t *testing.T) {
	// Given two concurrent workers, each with a durable private prefix.
	root := t.TempDir()
	first, one := savedPrefix(t, root)
	second, two := savedPrefix(t, root)
	require.NotEqual(t, one.Identity.WriterID, two.Identity.WriterID)
	require.Empty(t, completeFiles(t, root), "busy owners are neither recovered nor published")
	require.NoError(t, first.Close())
	require.NoError(t, second.Close())
	// When a new worker starts, then it recovers both released namespaces without assignments.
	_ = openStore(t, root, NativeFiles{}, file.DurableWriter{})
	paths := completeFiles(t, root)
	require.Len(t, paths, 2)
	for _, path := range paths {
		require.Equal(t, []avro.Record{testRecord(42)}, readFile(t, path))
	}
}

func TestRecoveryDiscardsOnlyOwnedUnlistedPrivateFiles(t *testing.T) {
	// Given an empty unacknowledged file, an unrelated owner and an unpublished namespace.
	root := t.TempDir()
	s := openStore(t, root, NativeFiles{}, file.DurableWriter{})
	part, err := s.Create(testRecord(42).Source, 42)
	require.NoError(t, err)
	owned := filepath.Join(root, "."+s.state.WriterID+"."+part.ID+".partial")
	foreign := filepath.Join(root, ".ffffffffffffffffffffffffffffffff.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.partial")
	require.NoError(t, os.WriteFile(foreign, nil, 0600))
	initializing := filepath.Join(root, ".kaferry", "writers", ".init-interrupted")
	require.NoError(t, os.Mkdir(initializing, 0700))
	require.NoError(t, s.Close())
	// When a new owner recovers, then only the file proven unacknowledged is removed.
	_ = openStore(t, root, NativeFiles{}, file.DurableWriter{})
	require.NoFileExists(t, owned)
	require.FileExists(t, foreign)
	require.DirExists(t, initializing)
	require.Empty(t, completeFiles(t, root))
}
