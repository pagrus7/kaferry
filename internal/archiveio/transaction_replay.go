package archiveio

import (
	"context"
	"errors"

	"github.com/pagrus7/kaferry/internal/archive/avro"
	"github.com/pagrus7/kaferry/internal/archive/replay"
	"github.com/pagrus7/kaferry/internal/checkpoint"
	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
)

type memoryCheckpoint struct{ state *replay.State }

func (m *memoryCheckpoint) Load() (*replay.State, error) { return m.state, nil }
func (m *memoryCheckpoint) Save(state replay.State) error {
	if _, err := checkpoint.Encode(state, 64<<20); err != nil {
		return err
	}
	m.state = &state
	return nil
}
func (m *memoryCheckpoint) Close() error { return nil }

type replayProgress struct{ binding *Binding }

func (s replayProgress) Save(ctx context.Context, cursors []core.Cursor) (map[int32]error, error) {
	if len(cursors) != 1 || cursors[0].Partition != 0 {
		return nil, errors.New("replay requires one virtual cursor")
	}
	if err := s.binding.session.Commit(ctx, cursors[0], ""); err != nil {
		return nil, err
	}
	return map[int32]error{0: nil}, nil
}
func (b *Binding) Source(ctx context.Context, limits config.Worker, partitionCount int) (core.Source, core.ProgressStore, error) {
	j := b.session
	if err := j.Acquire(ctx, []int32{0}); err != nil {
		return nil, nil, err
	}
	memory := &memoryCheckpoint{}
	if frame := j.Partition(0).Saved(); frame != nil {
		if frame.Replay == nil {
			return nil, nil, errors.New("replay checkpoint lacks pinned file selection")
		}
		memory.state = frame.Replay
	} else if b.mirrorState != nil {
		return nil, nil, errors.New("destination checkpoint missing for initialized replay")
	}
	codec, err := avro.New(limits.ArchiveBlockMaxBytes)
	if err != nil {
		return nil, nil, err
	}
	catalog := replay.NewCatalog(j.Plan.Source.Name, codec, replay.NativeFiles{})
	pool, err := replay.NewPool(catalog, replay.Limits{OpenFiles: limits.ArchiveReplayOpenFiles, Blocks: limits.ArchiveReplayBlocks})
	if err != nil {
		return nil, nil, err
	}
	session, err := replay.NewSession(replay.Query{Group: j.Plan.Group, Fingerprint: j.Identity.Fingerprint, Window: j.Plan.Window, Partitions: j.Plan.Partitions, Offsets: j.Plan.Offsets, PartitionOffsets: j.Plan.PartitionOffsets, MaxOutOfOrderMS: j.Plan.MaxOutOfOrderMS, PartitionCount: partitionCount}, b.clock, memory, catalog, pool, limits.RecordCap)
	if err != nil {
		_ = pool.Close()
		return nil, nil, err
	}
	if session.PrivateFiles > 0 {
		b.log.Warn("archive snapshot excludes unfinished files", "private_files", session.PrivateFiles)
	}
	j.StageReplay = func(cursor core.Cursor) (*replay.State, error) {
		if err := session.CheckpointStop(cursor.Position, cursor.Stop); err != nil {
			return nil, err
		}
		saved := session.Saved()
		return &saved, nil
	}
	// Pin selection and initial physical cursors before accepting any output.
	if err = j.Commit(ctx, core.Cursor{Partition: 0, Position: session.Saved().Position}, ""); err != nil {
		_ = session.Close()
		return nil, nil, err
	}
	return NewSource(session, b.clock), replayProgress{binding: b}, nil
}
