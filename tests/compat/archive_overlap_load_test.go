//go:build integration && load

package compat

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/pagrus7/kaferry/internal/archive/avro"
	"github.com/pagrus7/kaferry/internal/archive/replay"
	"github.com/stretchr/testify/require"
)

func TestLoadArchiveOverlap(t *testing.T) {
	// Given sixteen files whose offset ranges fully overlap, exceeding the default block cache.
	root := t.TempDir()
	src := filepath.Join(root, "input")
	require.NoError(t, os.Mkdir(src, 0700))
	codec, err := avro.New(64 << 20)
	require.NoError(t, err)
	for id := range 16 {
		identity := avro.Identity{Version: 1, WriterID: fmt.Sprintf("%032x", id+1), FileID: fmt.Sprintf("%032x", id+1), Source: avro.Source{Cluster: "cluster", Topic: "orders", TopicID: avro.TopicID{1}}}
		f, err := os.Create(filepath.Join(src, identity.WriterID+"."+identity.FileID+".avro"))
		require.NoError(t, err)
		writer, err := codec.NewWriter(f, identity)
		require.NoError(t, err)
		for offset := range 1000 {
			require.NoError(t, writer.Append(avro.Record{Source: identity.Source, Offset: int64(offset), TimestampKind: "unknown", Value: []byte(fmt.Sprintf("%d:%d", id, offset)), Headers: []avro.Header{}}))
		}
		require.NoError(t, writer.Flush())
		require.NoError(t, f.Close())
	}
	for _, blocks := range []int{8, 32} {
		t.Run(fmt.Sprintf("cache-%d", blocks), func(t *testing.T) {
			directory := t.TempDir()
			out := filepath.Join(directory, "output")
			cfg := filepath.Join(directory, "worker.toml")
			require.NoError(t, os.WriteFile(cfg, []byte(fmt.Sprintf("[worker]\ncheckpoint_dir=%q\narchive_replay_blocks=%d\n", directory, blocks)), 0600))
			query := fmt.Sprintf("INSERT INTO FILE avro('%s') SELECT * FROM FILE avro('%s') WITH (GROUP='overlap') ", out, src)
			// When the engine copies the fixed snapshot, measure the bounded-cache behavior.
			report := measureArchiveCLI(t, query, cfg, 16000)
			report.ArchiveSourceFiles = 16
			report.ArchiveReplayBlocks = blocks
			// Then every duplicate source offset survives, with its distinct original value.
			catalog := replay.NewCatalog(out, codec, replay.NativeFiles{})
			selection, err := catalog.Select(nil)
			require.NoError(t, err)
			pool, err := replay.NewPool(catalog, replay.Limits{Blocks: 32})
			require.NoError(t, err)
			defer pool.Close()
			progress := make([]replay.Progress, len(selection.Files))
			for i, f := range selection.Files {
				progress[i] = replay.Progress{File: f, Next: replay.Position{Block: f.Start, Previous: -1}}
			}
			reader, err := replay.NewReader(progress, pool)
			require.NoError(t, err)
			seen := make(map[string]bool, 16000)
			for {
				entry, err := reader.Next()
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
				value := string(entry.Record.Value)
				require.False(t, seen[value])
				seen[value] = true
			}
			require.Len(t, seen, 16000)
			t.Logf("cache=%d records/s=%.0f elapsed=%.3fs cpu=%.3fs RSS=%.1fMiB", blocks, report.RecordsPerSecond, report.ElapsedSeconds, report.ChildCPUSeconds, float64(report.SampledPeakRSSBytes)/(1<<20))
		})
	}
}
