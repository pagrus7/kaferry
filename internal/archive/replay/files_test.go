package replay

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/pagrus7/kaferry/internal/archive/avro"
	"github.com/pagrus7/kaferry/internal/file"
	"github.com/stretchr/testify/require"
)

type measuredOpener struct {
	Opener
	active, peak, opens, blocks, closes int
	openErr, blockErr, closeErr         error
}
type measuredHandle struct {
	Handle
	owner *measuredOpener
}

func (m *measuredOpener) Open(f File) (Handle, error) {
	m.opens++
	if m.openErr != nil {
		return nil, m.openErr
	}
	h, err := m.Opener.Open(f)
	if err != nil {
		return nil, err
	}
	m.active++
	m.peak = max(m.peak, m.active)
	return &measuredHandle{Handle: h, owner: m}, nil
}
func (h *measuredHandle) Block(pos int64) (*avro.Block, error) {
	h.owner.blocks++
	if h.owner.blockErr != nil {
		return nil, h.owner.blockErr
	}
	return h.Handle.Block(pos)
}
func (h *measuredHandle) Close() error {
	h.owner.active--
	h.owner.closes++
	actual := h.Handle.Close()
	if actual != nil {
		return actual
	}
	return h.owner.closeErr
}

func TestMergePreservesDuplicatesFullRecordsAndPhysicalTiesWithBoundedResources(t *testing.T) {
	// Given overlapping files whose duplicate offsets must remain separate physical records.
	root := t.TempDir()
	var expected []avro.Record
	var inputs [][]avro.Record
	for id := range 20 {
		values := records(42, 42, 43, 44)
		for i := range values {
			values[i].Key = []byte{byte(id), byte(i)}
		}
		values[0].Value = nil
		values[1].Value = []byte{}
		values[2].TimestampMS = nil
		values[2].TimestampKind = "unknown"
		inputs = append(inputs, values)
		writeFile(t, root, id+1, values, 2)
	}
	for _, offset := range []int64{42, 43, 44} {
		for _, values := range inputs {
			for _, r := range values {
				if r.Offset == offset {
					expected = append(expected, r)
				}
			}
		}
	}
	catalog := NewCatalog(root, codec(t), NativeFiles{})
	selected, err := catalog.Select()
	require.NoError(t, err)
	meter := &measuredOpener{Opener: catalog}
	pool, err := NewPool(meter, Limits{OpenFiles: 2, Blocks: 2})
	require.NoError(t, err)
	progress := make([]Progress, len(selected.Files))
	for i, f := range selected.Files {
		progress[i] = Progress{File: f, Next: initial(f)}
	}
	r, err := NewReader(progress, pool)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, r.Close()) })
	// When merging beyond both cache budgets, then complete records and ties remain unchanged.
	var got []avro.Record
	for {
		entry, err := r.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		got = append(got, entry.Record)
		require.LessOrEqual(t, len(pool.blocks), 2)
	}
	require.Equal(t, expected, got)
	require.LessOrEqual(t, meter.peak, 2)
	require.Greater(t, meter.opens, 20, "the test crosses the handle budget")
	require.NoError(t, r.Close())
	require.Zero(t, meter.active)
	require.Equal(t, meter.opens, meter.closes)
}

func TestRestartUsesPhysicalCursorsAndIgnoresNewFiles(t *testing.T) {
	// Given two overlapping files, including an equal-offset duplicate in the first file.
	root, checkpoints := t.TempDir(), t.TempDir()
	a, b := records(42, 42, 43), records(42, 43)
	for i := range a {
		a[i].Key = []byte(fmt.Sprintf("a-%d", i))
	}
	for i := range b {
		b[i].Key = []byte(fmt.Sprintf("b-%d", i))
	}
	writeFile(t, root, 1, a, 0)
	writeFile(t, root, 2, b, 1)
	s, _ := diskSession(t, root, checkpoints, query(), Limits{OpenFiles: 1, Blocks: 1}, 10)
	for range 3 {
		_, err := s.Next()
		require.NoError(t, err)
	}
	require.NoError(t, s.Checkpoint(1))
	require.NoError(t, s.Close())
	writeFile(t, root, 3, records(41), 0)
	// When restarting the same group, then only the first confirmed physical record is excluded.
	resumed, _ := diskSession(t, root, checkpoints, query(), Limits{OpenFiles: 1, Blocks: 1}, 10)
	require.Equal(t, []avro.Record{a[1], b[0], a[2], b[1]}, drain(t, resumed, true))
	require.True(t, resumed.Saved().Complete)
	require.NoError(t, resumed.Close())
	// A new group sees the additional completed file too.
	q := query()
	q.Group = "new-replay-group"
	fresh, _ := diskSession(t, root, checkpoints, q, Limits{}, 10)
	got := drain(t, fresh, true)
	require.Len(t, got, 6)
	require.EqualValues(t, 41, got[0].Offset)
}

func TestIncompleteRestartRejectsMissingResizedOrModifiedSelectedInput(t *testing.T) {
	for _, damage := range []string{"missing", "shortened", "same length"} {
		t.Run(damage, func(t *testing.T) {
			// Given a fully consumed file and another still pending in an incomplete group.
			root, checkpoints := t.TempDir(), t.TempDir()
			a := writeFile(t, root, 1, records(42), 0)
			later := records(50)
			later[0].Source.Partition = 1
			writeFile(t, root, 2, later, 0)
			s, _ := diskSession(t, root, checkpoints, query(), Limits{}, 10)
			item, err := s.Next()
			require.NoError(t, err)
			require.NoError(t, s.Checkpoint(item.Next))
			require.NoError(t, s.Close())
			path := filepath.Join(root, a.name())
			switch damage {
			case "missing":
				require.NoError(t, os.Remove(path))
			case "shortened":
				require.NoError(t, os.Truncate(path, a.Size-1))
			case "same length":
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				data[len(data)-1] ^= 1
				require.NoError(t, os.WriteFile(path, data, 0600))
			}
			// When resuming, then even the already consumed file must still match its pinned contents.
			catalog := NewCatalog(root, codec(t), NativeFiles{})
			pool, err := NewPool(catalog, Limits{})
			require.NoError(t, err)
			defer pool.Close()
			journal, err := NewJournal(checkpoints, query().Group, NativeGroupFiles{}, file.DurableWriter{})
			require.NoError(t, err)
			defer journal.Close()
			_, err = NewSession(query(), clock{}, journal, catalog, pool, 10)
			require.Error(t, err)
			require.ErrorContains(t, err, a.name())
		})
	}
}

func TestReplayWritesOnlyToExternalCheckpointDirectory(t *testing.T) {
	// Given a read-only archive and a separate writable checkpoint directory.
	root, checkpoints := t.TempDir(), t.TempDir()
	f := writeFile(t, root, 1, records(42, 43), 1)
	require.NoError(t, os.Chmod(filepath.Join(root, f.name()), 0400))
	require.NoError(t, os.Chmod(root, 0500))
	t.Cleanup(func() { require.NoError(t, os.Chmod(root, 0700)) })
	// When replaying and saving, then the input archive remains unchanged.
	s, _ := diskSession(t, root, checkpoints, query(), Limits{}, 10)
	require.Equal(t, records(42, 43), drain(t, s, true))
	after, err := NewCatalog(root, codec(t), NativeFiles{}).describe(f.name())
	require.NoError(t, err)
	require.Equal(t, f, after)
	_, err = os.Stat(filepath.Join(root, ".kaferry"))
	require.ErrorIs(t, err, os.ErrNotExist)
	entries, err := os.ReadDir(checkpoints)
	require.NoError(t, err)
	require.Len(t, entries, 2)
}

func TestPoolFailuresAreTerminalAndCloseEveryOwnedHandle(t *testing.T) {
	for _, stage := range []string{"open", "block", "evict"} {
		t.Run(stage, func(t *testing.T) {
			// Given two files and a failure while opening, reading or evicting a handle.
			root := t.TempDir()
			a := writeFile(t, root, 1, records(42), 0)
			b := writeFile(t, root, 2, records(43), 0)
			meter := &measuredOpener{Opener: NewCatalog(root, codec(t), NativeFiles{})}
			pool, err := NewPool(meter, Limits{OpenFiles: 1, Blocks: 1})
			require.NoError(t, err)
			switch stage {
			case "open":
				meter.openErr = errDisk
			case "block":
				meter.blockErr = errDisk
			case "evict":
				_, _, err = pool.Read(a, initial(a))
				require.NoError(t, err)
				meter.closeErr = errDisk
				a = b
			}
			// When the operation fails, then retry does no further I/O and Close releases remaining handles.
			_, _, err = pool.Read(a, initial(a))
			require.ErrorIs(t, err, errDisk)
			require.ErrorContains(t, err, a.name())
			require.ErrorContains(t, err, "block byte")
			before := meter.opens + meter.blocks + meter.closes
			_, _, err = pool.Read(a, initial(a))
			require.ErrorIs(t, err, errDisk)
			require.Equal(t, before, meter.opens+meter.blocks+meter.closes)
			meter.closeErr = nil
			require.NoError(t, pool.Close())
			require.NoError(t, pool.Close())
			require.Zero(t, meter.active)
		})
	}
}

func TestCatalogPinsOnlyCompletedRegularFilesAndReportsPrivateFiles(t *testing.T) {
	// Given completed data, a summary, private output and an unrelated file.
	root := t.TempDir()
	f := writeFile(t, root, 1, records(42), 0)
	for _, name := range []string{".busy.partial", "worker.file.summary.json", "unrelated"} {
		require.NoError(t, os.WriteFile(filepath.Join(root, name), nil, 0600))
	}
	catalog := NewCatalog(root, codec(t), NativeFiles{})
	// When selecting, then only completed data is pinned and private output is reported.
	selection, err := catalog.Select()
	require.NoError(t, err)
	require.Equal(t, []File{f}, selection.Files)
	require.Equal(t, 1, selection.PrivateFiles)
	// A renamed or linked completed file cannot impersonate its embedded identity.
	require.NoError(t, os.Rename(filepath.Join(root, f.name()), filepath.Join(root, "renamed.avro")))
	_, err = catalog.Select()
	require.ErrorContains(t, err, "filename")
	require.NoError(t, os.Remove(filepath.Join(root, "renamed.avro")))
	require.NoError(t, os.Symlink("missing", filepath.Join(root, f.name())))
	_, err = catalog.Select()
	require.ErrorContains(t, err, "regular file")
}
