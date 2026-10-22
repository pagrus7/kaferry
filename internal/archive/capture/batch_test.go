package capture

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/pagrus7/kaferry/internal/file"
	"github.com/stretchr/testify/require"
)

func TestBatchPublicationRecoversAfterPartialRenames(t *testing.T) {
	for _, fault := range []string{"second rename", "directory sync"} {
		t.Run(fault, func(t *testing.T) {
			// Given two acknowledged files and a failure during their joint publication.
			root := t.TempDir()
			fs := &observedFiles{Filesystem: NativeFiles{}}
			store := openStore(t, root, fs, file.DurableWriter{})
			var ids []string
			for partition := range 2 {
				record := testRecord(42)
				record.Source.Partition = int32(partition)
				segment, err := store.Create(record.Source, record.Offset)
				require.NoError(t, err)
				_, err = store.Append(segment.ID, record)
				require.NoError(t, err)
				ids = append(ids, segment.ID)
			}
			require.NoError(t, store.Checkpoint())
			renames := 0
			fs.hook = func(op, path string) error {
				if op == "rename" && strings.HasSuffix(path, ".avro") {
					renames++
					if fault == "second rename" && renames == 2 {
						return errDisk
					}
				}
				if fault == "directory sync" && op == "sync-directory" && filepath.Base(path) == filepath.Base(root) && renames > 0 {
					return errDisk
				}
				return nil
			}
			// When publication stops midway, then all acknowledged boundaries remain recoverable.
			require.ErrorIs(t, store.PublishMany(ids), errDisk)
			require.Len(t, store.state.Files, 2)
			require.NoError(t, store.Close())
			recovered := openStore(t, root, NativeFiles{}, file.DurableWriter{})
			require.Empty(t, recovered.state.Files)
			require.Len(t, completeFiles(t, root), 2)
			for _, name := range completeFiles(t, root) {
				require.Len(t, readFile(t, name), 1)
			}
		})
	}
}

func TestBatchPublicationSyncsDirectoryAndSavesStateOnce(t *testing.T) {
	// Given thirty-two already-durable files under one writer.
	root := t.TempDir()
	fs := &observedFiles{Filesystem: NativeFiles{}}
	writer := &batchWriter{}
	store := openStore(t, root, fs, writer)
	var ids []string
	for partition := range 32 {
		record := testRecord(42)
		record.Source.Partition = int32(partition)
		segment, err := store.Create(record.Source, record.Offset)
		require.NoError(t, err)
		_, err = store.Append(segment.ID, record)
		require.NoError(t, err)
		ids = append(ids, segment.ID)
	}
	require.NoError(t, store.Checkpoint())
	before := writer.saves
	fs.events = nil
	// When publishing together, then JSON is hashed/replaced once and the directory is synced once.
	require.NoError(t, store.PublishMany(ids))
	require.Equal(t, before+1, writer.saves)
	require.Equal(t, 1, strings.Count(strings.Join(fs.events, "\n"), "sync-directory:"+filepath.Base(root)))
	require.Len(t, completeFiles(t, root), 32)
}

type batchWriter struct {
	saves int
	file.DurableWriter
}

func (w *batchWriter) Replace(path string, data []byte) error {
	w.saves++
	return w.DurableWriter.Replace(path, data)
}
