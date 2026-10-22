package archiveio

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/pagrus7/kaferry/internal/archive/replay"
	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/file"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
)

func TestFactoryOwnsReplayCheckpointAndRejectsUnsafePaths(t *testing.T) {
	// Given an archive created through the real output adapter.
	root := t.TempDir()
	limits := config.Defaults("").Worker
	limits.CheckpointDir = &root
	factory := NewFactory(nil, core.RealClock{}, file.DurableWriter{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	p := plan.Plan{Source: plan.Endpoint{Kind: "FILE", Format: "avro", Name: filepath.Join(root, "input")}, Destination: plan.Endpoint{Kind: "FILE", Format: "avro", Name: filepath.Join(root, "output")}, Group: "group"}
	seed := p
	seed.Destination = p.Source
	sink, err := factory.OpenOutput(seed, "fixture", file.DestinationProgress{}, limits)
	require.NoError(t, err)
	record := core.Record{Source: &core.Provenance{Cluster: "cluster", TopicID: [16]byte{1}, Topic: "orders"}, TimestampType: "unknown"}
	require.NoError(t, sink.Submit(record, core.Token{}))
	require.NoError(t, sink.Flush())
	require.NoError(t, sink.(interface{ Finish() error }).Finish())
	sink.Close()
	// When one process owns a replay group, then another process cannot acquire it.
	fp, err := p.Fingerprint("", "")
	require.NoError(t, err)
	source, err := factory.OpenSource(p, fp, limits)
	require.NoError(t, err)
	_, err = factory.OpenSource(p, fp, limits)
	require.ErrorIs(t, err, replay.ErrGroupBusy)
	require.NoError(t, source.Close())
	source, err = factory.OpenSource(p, fp, limits)
	require.NoError(t, err)
	require.NoError(t, source.Close())
	// When checkpoint storage is inside the input or output aliases the input, fail before output creation.
	inside := p.Source.Name
	limits.CheckpointDir = &inside
	_, err = factory.OpenSource(p, fp, limits)
	require.ErrorContains(t, err, "outside")
	limits.CheckpointDir = &root
	alias := filepath.Join(root, "alias")
	require.NoError(t, os.Symlink(p.Source.Name, alias))
	p.Destination.Name = alias
	_, err = factory.OpenSource(p, fp, limits)
	require.ErrorContains(t, err, "same archive")
	p.Source.Name = filepath.Join(root, "missing")
	p.Destination.Name = filepath.Join(root, "new-output")
	p.Group = "missing"
	_, err = factory.OpenSource(p, fp, limits)
	require.Error(t, err)
	journal, err := replay.NewJournal(root, p.Group, replay.NativeGroupFiles{}, file.DurableWriter{})
	require.NoError(t, err, "failed construction releases checkpoint ownership")
	require.NoError(t, journal.Close())
	_, err = os.Stat(p.Destination.Name)
	require.ErrorIs(t, err, os.ErrNotExist)
}
