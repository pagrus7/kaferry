package archiveio

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/pagrus7/kaferry/internal/archive/replay"
	"github.com/pagrus7/kaferry/internal/checkpoint"
	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
)

func TestFactoryOwnsReplayCheckpointAndRejectsUnsafePaths(t *testing.T) {
	// Given an archive and checkpoint directory configured relative to the working directory.
	t.Chdir(t.TempDir())
	root := "."
	limits := config.Defaults("").Worker
	limits.CheckpointDir = &root
	factory := NewFactory(core.RealClock{}, checkpoint.DurableWriter{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	p := plan.Plan{Source: plan.Endpoint{Kind: "FILE", Format: "avro", Name: filepath.Join(root, "input")}, Destination: plan.Endpoint{Kind: "FILE", Format: "avro", Name: filepath.Join(root, "output")}, Group: "group"}
	seed := p
	seed.Destination = p.Source
	sink, err := factory.OpenOutput(seed.Destination, limits)
	require.NoError(t, err)
	record := core.Record{Source: &core.Provenance{Cluster: "cluster", TopicID: [16]byte{1}, Topic: "orders"}, TimestampType: "unknown"}
	require.NoError(t, sink.Submit(record, core.Token{}))
	require.NoError(t, sink.Flush())
	require.NoError(t, sink.Finish())
	sink.Close()
	// When one process owns a replay group, then another process cannot acquire it.
	fp, err := p.Fingerprint("", "")
	require.NoError(t, err)
	source, err := factory.OpenSource(p, fp, limits, 0)
	require.NoError(t, err)
	_, err = factory.OpenSource(p, fp, limits, 0)
	require.ErrorIs(t, err, replay.ErrGroupBusy)
	require.NoError(t, source.Close())
	source, err = factory.OpenSource(p, fp, limits, 0)
	require.NoError(t, err)
	require.NoError(t, source.Close())
	// When checkpoint storage is inside the input or output aliases the input, fail before output creation.
	inside := p.Source.Name
	limits.CheckpointDir = &inside
	_, err = factory.OpenSource(p, fp, limits, 0)
	require.ErrorContains(t, err, "outside")
	limits.CheckpointDir = &root
	alias := filepath.Join(root, "alias")
	require.NoError(t, os.Symlink(p.Source.Name, alias))
	p.Destination.Name = alias
	_, err = factory.OpenSource(p, fp, limits, 0)
	require.ErrorContains(t, err, "same archive")
	p.Source.Name = filepath.Join(root, "missing")
	p.Destination.Name = filepath.Join(root, "new-output")
	p.Group = "missing"
	_, err = factory.OpenSource(p, fp, limits, 0)
	require.Error(t, err)
	journal, err := replay.NewJournal(root, p.Group, replay.NativeGroupFiles{}, checkpoint.DurableWriter{})
	require.NoError(t, err, "failed construction releases checkpoint ownership")
	require.NoError(t, journal.Close())
	_, err = os.Stat(p.Destination.Name)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestInvalidCheckpointDirectoryFailsBeforeCreatingReplayState(t *testing.T) {
	for _, shape := range []string{"missing", "regular file"} {
		t.Run(shape, func(t *testing.T) {
			// Given an unusable checkpoint directory and otherwise valid archive paths.
			root := t.TempDir()
			directory := filepath.Join(root, "checkpoints")
			original := []byte("keep this file")
			if shape == "regular file" {
				require.NoError(t, os.WriteFile(directory, original, 0600))
			}
			limits := config.Defaults("").Worker
			limits.CheckpointDir = &directory
			p := plan.Plan{Source: plan.Endpoint{Kind: "FILE", Format: "avro", Name: t.TempDir()}, Destination: plan.Endpoint{Kind: "FILE", Format: "avro", Name: filepath.Join(root, "output")}, Group: "g"}
			factory := NewFactory(core.RealClock{}, checkpoint.DurableWriter{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
			// When starting replay, then fail as a configuration conflict without creating output or state.
			_, err := factory.OpenSource(p, "fingerprint", limits, 0)
			var conflict *Conflict
			require.ErrorAs(t, err, &conflict)
			require.Equal(t, 2, conflict.ExitCode())
			_, err = os.Stat(p.Destination.Name)
			require.ErrorIs(t, err, os.ErrNotExist)
			if shape == "regular file" {
				data, err := os.ReadFile(directory)
				require.NoError(t, err)
				require.Equal(t, original, data)
			} else {
				_, err = os.Stat(directory)
				require.ErrorIs(t, err, os.ErrNotExist)
			}
		})
	}
}
