package archiveio

import (
	"context"
	"errors"
	"github.com/pagrus7/kaferry/internal/archive/replay"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
	"time"
)

type replayMock struct {
	state     replay.State
	err       error
	reads     int
	closed    bool
	confirmed int64
	stop      *core.Stop
}

func (m *replayMock) Next() (replay.Item, error) {
	m.reads++
	return replay.Item{Position: 10, Next: 11}, m.err
}
func (m *replayMock) CheckpointStop(position int64, stop *core.Stop) error {
	m.confirmed = position
	m.stop = stop
	return m.err
}
func (m *replayMock) Saved() replay.State { return m.state }
func (m *replayMock) Close() error        { m.closed = true; return nil }

type clockMock struct{ sleeps int }

func (m *clockMock) Now() time.Time      { return time.Unix(0, 0) }
func (m *clockMock) Sleep(time.Duration) { m.sleeps++ }

type lifecycleMock struct{ assignments []core.Assignment }

func (m *lifecycleMock) Assign(a []core.Assignment) error { m.assignments = a; return nil }
func (m *lifecycleMock) Revoke([]int32, bool) error       { return nil }
func TestReplaySourceBackpressureEOFAndConfirmedStop(t *testing.T) {
	// Given an initialized session whose progress is counted in input records.
	session := &replayMock{state: replay.State{Position: 10}}
	clock := &clockMock{}
	source := NewSource(session, clock, "")
	life := &lifecycleMock{}
	require.NoError(t, source.Start(life, nil))
	require.EqualValues(t, 10, life.assignments[0].Cursor.Position)
	// When paused or checkpoint capacity is full, then polling waits without inventing EOF or losing a record.
	require.NoError(t, source.Pause(0, true))
	event, err := source.Poll(time.Millisecond)
	require.NoError(t, err)
	require.Nil(t, event.Input)
	require.Zero(t, session.reads)
	require.NoError(t, source.Pause(0, false))
	session.err = replay.ErrPendingFull
	event, err = source.Poll(time.Millisecond)
	require.NoError(t, err)
	require.Nil(t, event.EOF)
	require.Equal(t, 2, clock.sleeps)
	session.err = nil
	event, err = source.Poll(time.Millisecond)
	require.NoError(t, err)
	require.EqualValues(t, 11, event.Input.Next)
	// When the destination confirms a stop, then only that prefix and its reason are saved.
	stop := &core.Stop{Reason: "worker_record_cap", At: 11}
	_, err = source.Save(t.Context(), []core.Cursor{{Position: 11, Stop: stop}})
	require.NoError(t, err)
	require.EqualValues(t, 11, session.confirmed)
	require.Equal(t, stop, session.stop)
	session.err = io.EOF
	event, err = source.Poll(time.Millisecond)
	require.NoError(t, err)
	require.EqualValues(t, 11, event.EOF.Position)
	require.NoError(t, source.Close())
	require.True(t, session.closed)
}
func TestReplaySourcePropagatesFailureAndRejectsInvalidCommit(t *testing.T) {
	// Given a failed source and a cancelled commit.
	failure := errors.New("corrupt archive")
	session := &replayMock{err: failure}
	source := NewSource(session, &clockMock{}, "")
	// When reading or saving, then the original error is retained and no invented prefix is committed.
	_, err := source.Poll(time.Millisecond)
	require.ErrorIs(t, err, failure)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = source.Save(ctx, []core.Cursor{{}})
	require.ErrorIs(t, err, context.Canceled)
	_, err = source.Save(t.Context(), []core.Cursor{{Partition: 1}})
	require.ErrorContains(t, err, "one virtual source cursor")
	_, err = source.Save(t.Context(), []core.Cursor{{Position: 3}})
	require.ErrorIs(t, err, failure)
}
