package archiveio

import (
	"context"
	"errors"
	"maps"
	"path/filepath"
	"testing"

	"github.com/pagrus7/kaferry/internal/archive/avro"
	"github.com/pagrus7/kaferry/internal/checkpoint"
	"github.com/pagrus7/kaferry/internal/commitmeta"
	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/delivery"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
)

type captureCommitLog struct {
	values, pending map[string][]byte
	commits         int
	err             error
}

func (*captureCommitLog) Ensure(context.Context, []int32) error { return nil }
func (l *captureCommitLog) Open(context.Context, int32) (delivery.Transaction, error) {
	return l, nil
}
func (l *captureCommitLog) ReadValue(_ context.Context, _ int32, key string, _ bool) ([]byte, error) {
	return l.values[key], nil
}
func (*captureCommitLog) Publish(context.Context, int32, string, string, []byte) error {
	return errors.New("unexpected publication-only transaction")
}
func (l *captureCommitLog) Write(_ context.Context, key string, data []byte) error {
	l.pending[key] = data
	return nil
}
func (l *captureCommitLog) Commit(context.Context) error {
	if l.err != nil {
		return l.err
	}
	maps.Copy(l.values, l.pending)
	clear(l.pending)
	l.commits++
	return nil
}
func (*captureCommitLog) Close() error { return nil }

type captureStateWriter struct {
	checkpoint.DurableWriter
	saves int
	err   error
}

func (w *captureStateWriter) Replace(path string, data []byte) error {
	if w.err != nil {
		return w.err
	}
	if filepath.Base(path) == "state.json" {
		w.saves++
	}
	return w.DurableWriter.Replace(path, data)
}

func captureCommitFixture(t *testing.T) (*captureOutput, *archivePartition, *captureCommitLog, *captureStateWriter, string) {
	t.Helper()
	p := plan.Plan{Group: "save", Source: plan.Endpoint{Kind: "TOPIC", Name: "orders"}, Destination: plan.Endpoint{Kind: "ARCHIVE", Name: filepath.Join(t.TempDir(), "archive")}}
	s := delivery.New(p, config.Defaults("").Worker)
	s.Identity = delivery.Identity{Group: p.Group, Fingerprint: "query", SourceCluster: "source", SourceTopic: [16]byte{1}}
	log := &captureCommitLog{values: map[string][]byte{}, pending: map[string][]byte{}}
	w := &captureStateWriter{}
	j := NewCapture(s, core.RealClock{}, log, w)
	part, err := j.Acquire(t.Context(), 0)
	require.NoError(t, err)
	a := part.(*archivePartition)
	t.Cleanup(func() { require.NoError(t, a.Close()) })
	require.NoError(t, a.archive.openWriter())
	log.commits, w.saves = 0, 0
	metadata, err := commitmeta.NewCodec().Encode(commitmeta.Progress{Fingerprint: "query", TopicID: "orders"})
	require.NoError(t, err)
	return j, a, log, w, metadata
}

func TestCaptureSavesEachBatchOnceAndPublishesBoundaries(t *testing.T) {
	// Given a coordinated archive with one admitted record and no rotation.
	j, part, log, w, metadata := captureCommitFixture(t)
	codec, err := avro.New(j.session.Limits.ArchiveBlockMaxBytes)
	require.NoError(t, err)
	buffer := &captureBuffer{journal: j, codec: codec}
	r := avro.Record{Source: avro.Source{Cluster: "source", Topic: "orders", TopicID: avro.TopicID{1}}, TimestampKind: "unknown", Value: []byte("record")}
	require.NoError(t, buffer.Append(r, core.Cursor{Position: 1}))
	require.NoError(t, buffer.Flush())
	frame := delivery.Frame{Cursor: core.Cursor{Position: 1}, Metadata: metadata}
	// When committing the data batch, then one transaction and replacement save its cursor and synced boundary.
	require.NoError(t, part.Commit(t.Context(), frame))
	require.Equal(t, 1, log.commits)
	require.Equal(t, 1, w.saves)
	require.EqualValues(t, 1, part.Saved().Cursor.Position)
	boundaries := maps.Clone(part.archive.contents.Files)
	require.Len(t, boundaries, 1)
	for _, boundary := range boundaries {
		require.EqualValues(t, 1, boundary.Records)
		require.Positive(t, boundary.Extent)
	}
	// When saving stop metadata and then filter-only progress, then each survives reload without changing bytes.
	for _, cursor := range []core.Cursor{
		{Position: 1, Stop: &core.Stop{Reason: "worker_record_cap", At: 1}, End: new(int64(3))},
		{Position: 2, End: new(int64(3))},
	} {
		before := archiveSnapshot(t, j.session.Plan.Destination.Name)
		log.commits, w.saves = 0, 0
		require.NoError(t, part.Commit(t.Context(), delivery.Frame{Cursor: cursor, Metadata: metadata}))
		require.Equal(t, 1, log.commits)
		require.Equal(t, 1, w.saves)
		saved, err := j.Read(t.Context(), 0)
		require.NoError(t, err)
		require.Equal(t, cursor, saved.Cursor)
		decoded, err := commitmeta.NewCodec().Decode(saved.Metadata)
		require.NoError(t, err)
		require.Equal(t, cursor.Stop, decoded.Stop)
		require.Equal(t, cursor.End, decoded.End)
		require.Equal(t, boundaries, part.archive.contents.Files)
		after := archiveSnapshot(t, j.session.Plan.Destination.Name)
		delete(before, filepath.ToSlash(filepath.Join(".kaferry", "writers", part.archive.writerID, "state.json")))
		delete(after, filepath.ToSlash(filepath.Join(".kaferry", "writers", part.archive.writerID, "state.json")))
		require.Equal(t, before, after)
	}
	// When publishing, then removal of the renamed boundary is still saved without a pending stage.
	log.commits, w.saves = 0, 0
	require.NoError(t, buffer.Finish())
	require.Equal(t, 1, log.commits)
	require.Equal(t, 1, w.saves)
	owner, err := j.readOwner(t.Context(), 0, part.archive.writerID, false)
	require.NoError(t, err)
	require.Empty(t, owner.Files)
	files, err := filepath.Glob(filepath.Join(j.session.Plan.Destination.Name, "*.avro"))
	require.NoError(t, err)
	require.Len(t, files, 1)
}

func TestArchiveSaveKeepsProgressPendingUntilEverySaveSucceeds(t *testing.T) {
	for _, stage := range []string{"coordinator", "local", "mirror"} {
		t.Run(stage, func(t *testing.T) {
			// Given pending metadata and a failure at one persistence boundary.
			j, part, log, w, metadata := captureCommitFixture(t)
			a := part.archive
			require.NoError(t, a.stage(core.Cursor{Position: 1}, metadata))
			failure := errors.New("save failed")
			switch stage {
			case "coordinator":
				log.err = failure
			case "local":
				w.err = failure
			case "mirror":
				j.session.SaveMirror = func(*delivery.Frame) error { return failure }
			}
			// When saving fails, then the pending progress is retained for error handling.
			require.ErrorIs(t, a.Save(a.path, a.contents), failure)
			require.NotNil(t, a.staged)
			log.err, w.err, j.session.SaveMirror = nil, nil, nil
		})
	}
}
