package delivery

import (
	"context"
	"errors"
	"fmt"
	"github.com/pagrus7/kaferry/internal/archive/replay"
	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestCheckpointRejectsIncompatibleOrIncompleteAuthority(t *testing.T) {
	// Given a complete versioned cursor bound to a query and both endpoint identities.
	session := New(plan.Plan{Source: plan.Endpoint{Kind: "TOPIC"}}, config.Defaults("").Worker)
	session.Identity = Identity{Group: "job", Fingerprint: "query", SourceTopic: [16]byte{1}, DestinationTopic: [16]byte{2}, ProgressTopic: [16]byte{3}}
	valid := Frame{Version: Version, Identity: session.Identity, Cursor: core.Cursor{Partition: 2, Position: 7}, Metadata: "encoded source metadata"}
	cases := []struct {
		name  string
		alter func(*Frame)
	}{
		{"unversioned", func(f *Frame) { f.Version = 0 }},
		{"unsupported version", func(f *Frame) { f.Version++ }},
		{"another query", func(f *Frame) { f.Identity.Fingerprint = "another" }},
		{"recreated destination", func(f *Frame) { f.Identity.DestinationTopic = [16]byte{9} }},
		{"negative cursor", func(f *Frame) { f.Cursor.Position = -1 }},
		{"negative partition", func(f *Frame) { f.Cursor.Partition = -1 }},
		{"missing metadata", func(f *Frame) { f.Metadata = "" }},
		{"mismatched source", func(f *Frame) { f.Replay = &replay.State{Position: 7} }},
		{"beyond end", func(f *Frame) { f.Cursor.End = new(int64(6)) }},
		{"stop beyond cursor", func(f *Frame) { f.Cursor.Stop = &core.Stop{At: 8} }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := valid
			c.alter(&f)
			encoded, err := Encode(f)
			require.NoError(t, err)
			// When authority is decoded, then unusable state cannot be mistaken for a fresh job.
			_, err = session.Decode(encoded)
			require.Error(t, err)
		})
	}
	data, err := Encode(valid)
	require.NoError(t, err)
	decoded, err := session.Decode(data)
	require.NoError(t, err)
	require.Equal(t, valid, decoded)
}
func TestReplayAuthorityRequiresItsPhysicalCursor(t *testing.T) {
	// Given archive input whose physical occurrences can share identical source offsets and values.
	s := New(plan.Plan{Source: plan.Endpoint{Kind: "ARCHIVE"}}, config.Defaults("").Worker)
	f := Frame{Version: Version, Identity: s.Identity, Cursor: core.Cursor{Position: 4}}
	// When its physical checkpoint is absent or disagrees, then recovery refuses it.
	require.Error(t, s.Validate(f))
	f.Replay = &replay.State{Version: 1, Group: s.Identity.Group, Fingerprint: s.Identity.Fingerprint, Files: []replay.Progress{}, Position: 3}
	require.Error(t, s.Validate(f))
	f.Replay.Position = 4
	require.NoError(t, s.Validate(f))
}

func TestAcquireFailureClosesEveryObtainedPartition(t *testing.T) {
	for _, closeFails := range []bool{false, true} {
		t.Run(fmt.Sprint("close failure=", closeFails), func(t *testing.T) {
			// Given partition zero is acquired before partition one fails, and another acquisition succeeds.
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			backend := &rollbackBackend{fail: true, first: make(chan struct{}), closeFails: closeFails}
			s := New(plan.Plan{}, config.Defaults("").Worker)
			s.Backend = backend
			// When the assignment fails, then all obtained leases close, even when a close itself fails.
			err := s.Acquire(ctx, []int32{0, 1, 2})
			require.ErrorIs(t, err, errAcquire)
			if closeFails {
				require.ErrorIs(t, err, errClose)
			}
			for _, id := range []int32{0, 2} {
				require.Equal(t, 1, backend.leases[id].closed)
			}
			for id := range int32(3) {
				require.Nil(t, s.Partition(id))
			}
			// Then a new assignment can own all partitions and release them normally.
			backend.fail, backend.closeFails = false, false
			require.NoError(t, s.Acquire(ctx, []int32{0, 1, 2}))
			for id := range int32(3) {
				require.Same(t, backend.leases[id], s.Partition(id))
			}
			require.NoError(t, s.Close())
		})
	}
}

var errAcquire = errors.New("acquisition failed")
var errClose = errors.New("lease close failed")

type rollbackBackend struct {
	fail, closeFails bool
	first            chan struct{}
	leases           [3]*rollbackPartition
}

func (*rollbackBackend) Prepare(context.Context, []int32) error      { return nil }
func (*rollbackBackend) Read(context.Context, int32) (*Frame, error) { return nil, nil }
func (b *rollbackBackend) Acquire(ctx context.Context, id int32) (Partition, error) {
	if b.fail && id == 1 {
		select {
		case <-b.first:
			return nil, errAcquire
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	p := &rollbackPartition{closeFails: b.closeFails && id == 0}
	b.leases[id] = p
	if b.fail && id == 0 {
		close(b.first)
	}
	return p, nil
}

type rollbackPartition struct {
	closed     int
	closeFails bool
}

func (*rollbackPartition) Saved() *Frame                       { return nil }
func (*rollbackPartition) Commit(context.Context, Frame) error { return nil }
func (p *rollbackPartition) Close() error {
	p.closed++
	if p.closeFails {
		return errClose
	}
	return nil
}

func TestReaderRejectsFrameStoredUnderAnotherPartition(t *testing.T) {
	// Given a valid checksummed frame stored under a different source partition's log key.
	s := New(plan.Plan{Source: plan.Endpoint{Kind: "TOPIC"}}, config.Defaults("").Worker)
	f := Frame{Version: Version, Identity: s.Identity, Cursor: core.Cursor{Partition: 1, Position: 7}, Metadata: "source metadata"}
	data, err := Encode(f)
	require.NoError(t, err)
	require.NoError(t, s.Validate(f))
	log := &partitionReadLog{value: data}
	// When partition zero is read, then it refuses the misplaced authority rather than returning its cursor.
	got, err := (Reader{Session: s, Log: log}).Read(t.Context(), 0)
	require.ErrorContains(t, err, "partition disagrees")
	require.Nil(t, got)
	require.Equal(t, data, log.value)
}

type partitionReadLog struct {
	Log
	value []byte
}

func (l *partitionReadLog) ReadValue(context.Context, int32, string, bool) ([]byte, error) {
	return l.value, nil
}
