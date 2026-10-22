package capture

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/archive/avro"
	"github.com/pagrus7/kaferry/internal/file"
	"github.com/stretchr/testify/require"
)

func TestCloseAbortsWithoutPublishingAndPreventsFurtherAcknowledgements(t *testing.T) {
	// Given a private durable prefix, when capture closes without Finish, then it remains private.
	root := t.TempDir()
	c, _ := newCapture(t, root, Policy{})
	require.NoError(t, c.Append(testRecord(42)))
	require.NoError(t, c.Flush())
	require.NoError(t, c.Close())
	require.Empty(t, completeFiles(t, root))
	require.Error(t, c.Append(testRecord(43)))
	require.Error(t, c.Finish())
	// Then releasing the lock permits another owner to recover exactly the saved record.
	_ = openStore(t, root, NativeFiles{}, file.DurableWriter{})
	paths := completeFiles(t, root)
	require.Len(t, paths, 1)
	require.Equal(t, []avro.Record{testRecord(42)}, readFile(t, paths[0]))
}

func TestRotationPublicationFailuresStopEveryEntryPoint(t *testing.T) {
	for _, mode := range []string{"age flush", "age append", "rewind", "capacity", "finish"} {
		t.Run(mode, func(t *testing.T) {
			// Given one accepted file and a backend that starts rejecting publication.
			store := &memorySegments{files: make(map[string]Segment)}
			clock := &testClock{}
			c, err := New(store, clock, Policy{MaxOpenFiles: 1})
			require.NoError(t, err)
			require.NoError(t, c.Append(testRecord(42)))
			store.errors = map[string]error{"publish": errDisk}
			// When rotation or completion tries to publish, then failure remains terminal.
			switch mode {
			case "age flush", "age append":
				clock.now = clock.now.Add(time.Hour)
				if mode == "age flush" {
					err = c.Flush()
				} else {
					err = c.Append(testRecord(43))
				}
			case "rewind":
				err = c.Append(testRecord(41))
			case "capacity":
				r := testRecord(43)
				r.Source.Partition++
				err = c.Append(r)
			case "finish":
				err = c.Finish()
			}
			require.ErrorIs(t, err, errDisk)
			require.ErrorIs(t, c.Finish(), errDisk)
			require.NoError(t, c.Close(), "cleanup remains available after failure")
		})
	}
}

func TestAmbiguousStateSaveRetainsDataForAtLeastOnceReplay(t *testing.T) {
	// Given a state replacement that completes but reports an error afterward.
	root := t.TempDir()
	fs := &observedFiles{Filesystem: NativeFiles{}}
	w := &observedWriter{AtomicWriter: file.DurableWriter{}, files: fs}
	s := openStore(t, root, fs, w)
	r := testRecord(42)
	part, err := s.Create(r.Source, r.Offset)
	require.NoError(t, err)
	_, err = s.Append(part.ID, r)
	require.NoError(t, err)
	w.after = func() error { return errDisk }
	// When the batch fails, then closing does not roll back the possibly durable state.
	require.ErrorIs(t, s.Checkpoint(), errDisk)
	w.after = nil
	require.NoError(t, s.Close())
	_ = openStore(t, root, NativeFiles{}, file.DurableWriter{})
	paths := completeFiles(t, root)
	require.Len(t, paths, 1)
	require.Equal(t, []avro.Record{r}, readFile(t, paths[0]), "an unacknowledged record may survive and later duplicate")
}

func TestPublicationRefusesChangedLengthAndNeverOverwrites(t *testing.T) {
	for _, mode := range []string{"length", "collision"} {
		t.Run(mode, func(t *testing.T) {
			// Given a durable private prefix and an unexpected external file change.
			root := t.TempDir()
			s, boundary := savedPrefix(t, root)
			completed := dataPath(root, boundary.Identity, false)
			if mode == "length" {
				require.NoError(t, os.Truncate(dataPath(root, boundary.Identity, true), boundary.Extent+1))
			} else {
				require.NoError(t, os.WriteFile(completed, []byte("do not replace"), 0600))
			}
			// When publishing, then conflict stops capture and leaves both protected objects unchanged.
			err := s.Publish(boundary.Identity.FileID)
			require.Error(t, err)
			if mode == "collision" {
				require.ErrorIs(t, err, os.ErrExist)
				data, err := os.ReadFile(completed)
				require.NoError(t, err)
				require.Equal(t, "do not replace", string(data))
			}
			require.FileExists(t, dataPath(root, boundary.Identity, true))
		})
	}
}

func TestJournalReadFailureDoesNotMeanEmptyState(t *testing.T) {
	for _, failure := range []error{errDisk, io.EOF} {
		// Given required state whose bytes cannot all be read despite its advertised size.
		root := t.TempDir()
		s, _ := savedPrefix(t, root)
		fs := &observedFiles{Filesystem: NativeFiles{}, readError: failure}
		// When loaded, then neither a disk error nor premature EOF becomes normal completion.
		_, err := NewJournal(fs, file.DurableWriter{}).Load(s.statePath())
		require.Error(t, err)
		require.NotErrorIs(t, err, io.EOF)
		if !errors.Is(failure, io.EOF) {
			require.ErrorIs(t, err, failure)
		}
	}
}

func TestInvalidFirstSourceStopsWithoutAcknowledging(t *testing.T) {
	// Given source metadata that cannot identify an original Kafka partition.
	root := t.TempDir()
	c, _ := newCapture(t, root, Policy{})
	r := testRecord(42)
	r.Source.Cluster = ""
	err := c.Append(r)
	require.Error(t, err)
	var invalid *avro.InvalidRecord
	require.False(t, errors.As(err, &invalid), "identity mistakes are not SQL-skippable resource errors")
	require.ErrorIs(t, c.Flush(), err)
	require.NoError(t, c.Close())
	_ = openStore(t, root, NativeFiles{}, file.DurableWriter{})
	require.Empty(t, completeFiles(t, root))
}

func TestAppendIOAndCallerErrorsCannotBeSkipped(t *testing.T) {
	for _, mode := range []string{"write failure", "timestamp kind"} {
		t.Run(mode, func(t *testing.T) {
			// Given an open private file and either an I/O failure or invalid caller data.
			fs := &observedFiles{Filesystem: NativeFiles{}}
			s := openStore(t, t.TempDir(), fs, file.DurableWriter{})
			r := testRecord(42)
			part, err := s.Create(r.Source, r.Offset)
			require.NoError(t, err)
			if mode == "write failure" {
				r.Value = make([]byte, 300<<10)
				fs.hook = func(op, _ string) error {
					if op == "write" {
						return errDisk
					}
					return nil
				}
			} else {
				r.TimestampKind = "invalid"
			}
			// When submission fails, then it poisons the writer rather than becoming SQL-skippable.
			_, err = s.Append(part.ID, r)
			require.Error(t, err)
			var skippable *avro.InvalidRecord
			require.False(t, errors.As(err, &skippable))
			fs.hook = nil
			require.ErrorIs(t, s.Checkpoint(), err)
		})
	}
}

func TestOldSegmentHandlesCannotChangeCompletedFiles(t *testing.T) {
	for _, op := range []string{"append", "publish"} {
		t.Run(op, func(t *testing.T) {
			// Given a segment that has already become immutable output.
			root := t.TempDir()
			s, boundary := savedPrefix(t, root)
			require.NoError(t, s.Publish(boundary.Identity.FileID))
			// When a caller mistakenly reuses its old handle, then it fails without changing the file.
			var err error
			if op == "append" {
				_, err = s.Append(boundary.Identity.FileID, testRecord(43))
			} else {
				err = s.Publish(boundary.Identity.FileID)
			}
			require.ErrorContains(t, err, "not open")
			require.Equal(t, []avro.Record{testRecord(42)}, readFile(t, dataPath(root, boundary.Identity, false)))
		})
	}
}

func TestAbandonedPrivateFileCleanupFailuresPreserveTheFile(t *testing.T) {
	// Given a valid initialized namespace and one file never recorded as acknowledged.
	root := t.TempDir()
	s := openStore(t, root, NativeFiles{}, file.DurableWriter{})
	part, err := s.Create(testRecord(42).Source, 42)
	require.NoError(t, err)
	private := filepath.Join(root, "."+s.state.WriterID+"."+part.ID+".partial")
	require.NoError(t, s.Close())
	fs := &observedFiles{Filesystem: NativeFiles{}}
	fs.hook = func(op, _ string) error {
		if op == "remove" {
			return errDisk
		}
		return nil
	}
	// When cleanup fails, then startup reports failure and does not infer successful recovery.
	_, err = Open(root, testCodec(t), fs, NewJournal(fs, file.DurableWriter{}))
	require.ErrorIs(t, err, errDisk)
	require.FileExists(t, private)
	_ = openStore(t, root, NativeFiles{}, file.DurableWriter{})
	require.NoFileExists(t, private)
}
