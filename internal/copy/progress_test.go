package copy

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfirmedPrefixIncludesNumericGaps(t *testing.T) {
	// Given
	tk := NewTracker()
	tk.Assign(Cursor{Partition: 0, Position: 100})

	// When
	a, err := tk.Observe(Cursor{Partition: 0, Position: 100}, 101, 10)
	require.NoError(t, err)
	b, err := tk.Observe(Cursor{Partition: 0, Position: 105}, 106, 20)
	require.NoError(t, err)
	require.True(t, tk.Settle(b, true))

	// Then
	require.Equal(t, int64(100), tk.Confirmed(0).Position, "unresolved record blocks progress")

	// When the missing earlier delivery succeeds.
	require.True(t, tk.Settle(a, true))

	// Then the consumed prefix includes numeric offset gaps.
	require.Equal(t, int64(106), tk.Confirmed(0).Position)
	records, bytes := tk.Total()
	require.Equal(t, 2, records, "confirmed work counts until commit")
	require.Equal(t, int64(30), bytes)

	// When only the first confirmed position is committed.
	tk.Committed(Cursor{Partition: 0, Position: 101})

	// Then later confirmed work still counts against the caps.
	records, bytes = tk.Total()
	require.Equal(t, 1, records)
	require.Equal(t, int64(20), bytes)

	// When the remaining confirmed position is committed.
	tk.Committed(Cursor{Partition: 0, Position: 106})

	// Then its work is released.
	records, bytes = tk.Total()
	require.Zero(t, records)
	require.Zero(t, bytes)
}

func TestOldGenerationDeliveryCannotAdvanceNewOwner(t *testing.T) {
	// Given a new assignment after the previous generation was revoked.
	tk := NewTracker()
	tk.Assign(Cursor{Partition: 0, Position: 0})
	old, err := tk.Observe(Cursor{Partition: 0, Position: 0}, 1, 1)
	require.NoError(t, err)
	tk.Revoke(0)
	tk.Assign(Cursor{Partition: 0, Position: 0})
	current, err := tk.Observe(Cursor{Partition: 0, Position: 0}, 1, 1)
	require.NoError(t, err)

	// When an old delivery arrives late.
	resolved := tk.Settle(old, true)

	// Then it cannot advance the new owner's confirmed prefix.
	require.False(t, resolved)
	require.Zero(t, tk.Confirmed(0).Position)

	// When the current generation's delivery arrives.
	resolved = tk.Settle(current, true)

	// Then its progress is confirmed.
	require.True(t, resolved)
	require.Equal(t, int64(1), tk.Confirmed(0).Position)
}

func TestControlRecordCursorAndTimestampStop(t *testing.T) {
	// Given
	tk := NewTracker()
	tk.Assign(Cursor{Partition: 0, Position: 0})

	// When a control cursor advances progress without a record payload.
	require.NoError(t, tk.Advance(Cursor{Partition: 0, Position: 2}))

	// Then one resolved entry occupies both budgets until saved.
	require.Equal(t, int64(2), tk.Confirmed(0).Position)
	records, bytes := tk.Total()
	require.Equal(t, 1, records)
	require.Zero(t, bytes)
	partitionRecords, partitionBytes := tk.Work(0)
	require.Equal(t, 1, partitionRecords)
	require.Zero(t, partitionBytes)

	// When an unchanged position marker follows the control cursor.
	require.NoError(t, tk.Advance(Cursor{Partition: 0, Position: 2}))

	// Then it retains exactly one entry, without adding work to either budget.
	records, bytes = tk.Total()
	require.Equal(t, 1, records)
	require.Zero(t, bytes)
	partitionRecords, partitionBytes = tk.Work(0)
	require.Equal(t, 1, partitionRecords)
	require.Zero(t, partitionBytes)

	// When the marker is committed, then both budgets clear.
	tk.Committed(Cursor{Partition: 0, Position: 2})
	records, bytes = tk.Total()
	require.Zero(t, records)
	require.Zero(t, bytes)
	partitionRecords, partitionBytes = tk.Work(0)
	require.Zero(t, partitionRecords)
	require.Zero(t, partitionBytes)
}

func TestBackwardsSourceCursorCannotChangeProgressOrWork(t *testing.T) {
	// Given saved progress followed by one outstanding write.
	tracker := NewTracker()
	tracker.Assign(Cursor{Partition: 0, Position: 10})
	first, err := tracker.Observe(Cursor{Partition: 0, Position: 10}, 11, 7)
	require.NoError(t, err)
	require.True(t, tracker.Settle(first, true))
	tracker.Committed(Cursor{Partition: 0, Position: 11})
	// When a source redelivers below the saved position with no outstanding work.
	_, err = tracker.Observe(Cursor{Partition: 0, Position: 10}, 11, 1)
	// Then rejection keeps the durable cursor and budget unchanged.
	require.Error(t, err)
	records, bytes := tracker.Total()
	require.Zero(t, records)
	require.Zero(t, bytes)
	require.Equal(t, int64(11), tracker.Saved(0))
	require.Equal(t, int64(11), tracker.Confirmed(0).Position)
	pending, err := tracker.Observe(Cursor{Partition: 0, Position: 11}, 12, 9)
	require.NoError(t, err)

	// When a source supplies a backwards record, next position, or EOF cursor.
	for _, operation := range []func() error{
		func() error { _, err := tracker.Observe(Cursor{Partition: 0, Position: 9}, 10, 100); return err },
		func() error { _, err := tracker.Observe(Cursor{Partition: 0, Position: 12}, 11, 100); return err },
		func() error { return tracker.Advance(Cursor{Partition: 0, Position: 5}) },
	} {
		err := operation()

		// Then rejection leaves both saved progress and the outstanding work unchanged.
		require.Error(t, err)
		records, bytes := tracker.Total()
		require.Equal(t, 1, records)
		require.Equal(t, int64(9), bytes)
		require.Equal(t, int64(11), tracker.Saved(0))
		require.Equal(t, int64(11), tracker.Confirmed(0).Position)
	}

	// When the legitimate outstanding write succeeds, then it still advances normally.
	require.True(t, tracker.Settle(pending, true))
	require.Equal(t, int64(12), tracker.Confirmed(0).Position)
	tracker.Committed(Cursor{Partition: 0, Position: 12})
	records, bytes = tracker.Total()
	require.Zero(t, records)
	require.Zero(t, bytes)
	require.Equal(t, int64(12), tracker.Saved(0))
}
