package archiveio

import (
	"context"
	"errors"
	"github.com/pagrus7/kaferry/internal/archive/avro"
	"github.com/pagrus7/kaferry/internal/archive/capture"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/pagrus7/kaferry/internal/archive/replay"
	"github.com/pagrus7/kaferry/internal/checkpoint"
	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/delivery"
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
	p := plan.Plan{Source: plan.Endpoint{Kind: "ARCHIVE", Format: "avro", Name: filepath.Join(root, "input")}, Destination: plan.Endpoint{Kind: "ARCHIVE", Format: "avro", Name: filepath.Join(root, "output")}, Group: "group"}
	seedArchiveForFactory(t, p.Source.Name)
	// When one process owns a replay group, then another process cannot acquire it.
	fp, err := p.Fingerprint("", "")
	require.NoError(t, err)
	source, err := testFactorySource(t, factory, p, fp, limits)
	require.NoError(t, err)
	_, err = testFactorySource(t, factory, p, fp, limits)
	require.ErrorIs(t, err, replay.ErrGroupBusy)
	require.NoError(t, source.Close())
	source, err = testFactorySource(t, factory, p, fp, limits)
	require.NoError(t, err)
	require.NoError(t, source.Close())
	// When checkpoint storage is inside the input or output aliases the input, fail before output creation.
	inside := p.Source.Name
	limits.CheckpointDir = &inside
	_, err = testFactorySource(t, factory, p, fp, limits)
	require.ErrorContains(t, err, "outside")
	limits.CheckpointDir = &root
	alias := filepath.Join(root, "alias")
	require.NoError(t, os.Symlink(p.Source.Name, alias))
	p.Destination.Name = alias
	_, err = testFactorySource(t, factory, p, fp, limits)
	require.ErrorContains(t, err, "same archive")
	p.Source.Name = filepath.Join(root, "missing")
	p.Destination.Name = filepath.Join(root, "new-output")
	p.Group = "missing"
	_, err = testFactorySource(t, factory, p, fp, limits)
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
			p := plan.Plan{Source: plan.Endpoint{Kind: "ARCHIVE", Format: "avro", Name: t.TempDir()}, Destination: plan.Endpoint{Kind: "ARCHIVE", Format: "avro", Name: filepath.Join(root, "output")}, Group: "g"}
			factory := NewFactory(core.RealClock{}, checkpoint.DurableWriter{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
			// When starting replay, then fail as a configuration conflict without creating output or state.
			_, err := testFactorySource(t, factory, p, "fingerprint", limits)
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

func seedArchiveForFactory(t *testing.T, path string) {
	t.Helper()
	codec, err := avro.New(64 << 20)
	require.NoError(t, err)
	files := capture.NativeFiles{}
	store, err := capture.Open(path, codec, files, capture.NewJournal(files, checkpoint.DurableWriter{}))
	require.NoError(t, err)
	writer, err := capture.New(store, core.RealClock{}, capture.Policy{})
	require.NoError(t, err)
	require.NoError(t, writer.Append(avro.Record{Source: avro.Source{Cluster: "cluster", TopicID: avro.TopicID{1}, Topic: "orders"}, TimestampKind: "unknown", Headers: []avro.Header{}}))
	require.NoError(t, writer.Finish())
	require.NoError(t, writer.Close())
}

type factorySource struct {
	Input
	session *delivery.Session
}

func (s *factorySource) Close() error { return errors.Join(s.Input.Close(), s.session.Close()) }
func testFactorySource(t *testing.T, f *Factory, p plan.Plan, fp string, w config.Worker) (Input, error) {
	t.Helper()
	s := delivery.New(p, w)
	s.Identity = delivery.Identity{Group: p.Group, Fingerprint: fp}
	s.Backend = NewCapture(s, core.RealClock{}, nil, checkpoint.DurableWriter{})
	input, err := f.OpenSource(context.Background(), s, w, 0)
	if err != nil {
		_ = s.Close()
		return nil, err
	}
	return &factorySource{Input: input, session: s}, nil
}
