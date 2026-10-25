package kafkaport

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type lookupBackend struct {
	calls  []offsetCall
	values map[int32]int64
	stamps map[int32]int64
	err    error
}

func (b *lookupBackend) Offsets(_ context.Context, ids []int32, k OffsetKind, committed bool) (map[int32]int64, error) {
	b.calls = append(b.calls, offsetCall{k, committed})
	return b.values, b.err
}
func (b *lookupBackend) OffsetsForTimes(_ context.Context, q map[int32]int64) (map[int32]int64, error) {
	b.stamps = q
	return b.values, b.err
}
func TestOffsetLookupOperationsAndTimestampOutcomes(t *testing.T) {
	// Given one injected backend, when requesting named boundaries, then choose explicit isolation semantics.
	b := &lookupBackend{values: map[int32]int64{0: 12}}
	l := NewOffsetLookupFactory().New(b, b)
	for _, operation := range []func(context.Context, []int32) (map[int32]int64, error){l.Earliest, l.ReadableEnd, l.ReplicatedEnd} {
		found, err := operation(t.Context(), []int32{0})
		require.NoError(t, err)
		require.EqualValues(t, 12, found[0])
		_, err = operation(t.Context(), nil)
		require.NoError(t, err)
	}
	require.Equal(t, []offsetCall{{EarliestOffset, false}, {LatestOffset, true}, {LatestOffset, false}}, b.calls)
	// An old timestamp still returns a retained match; a no-match is a distinct successful result.
	b.values = map[int32]int64{0: 12, 2: -1}
	q := []TimestampOffsetQuery{{Partition: 0, TimestampMS: 1000}, {Partition: 2, TimestampMS: 2000}}
	found, err := l.AtOrAfterTimestamp(t.Context(), q)
	require.NoError(t, err)
	require.Equal(t, map[int32]int64{0: 1000, 2: 2000}, b.stamps)
	require.Equal(t, []TimestampOffsetMatch{{Partition: 0, Offset: 12, Found: true}, {Partition: 2, Offset: -1}}, found)
	_, err = l.AtOrAfterTimestamp(t.Context(), nil)
	require.NoError(t, err)
}
func TestOffsetLookupCannotTurnFailuresIntoNoMatch(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values map[int32]int64
		err    error
		stamp  int64
	}{
		{name: "partition missing", values: map[int32]int64{}},
		{name: "unknown partition", err: errors.New("unknown partition")},
		{name: "timeout", err: context.DeadlineExceeded},
		{name: "invalid offset", values: map[int32]int64{0: -2}},
		{name: "negative timestamp", values: map[int32]int64{0: 10}, stamp: -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given incomplete or failed metadata, when looking up, then return an error without matches.
			b := &lookupBackend{values: tc.values, err: tc.err}
			l := NewOffsetLookupFactory().New(b, b)
			found, err := l.AtOrAfterTimestamp(t.Context(), []TimestampOffsetQuery{{Partition: 0, TimestampMS: tc.stamp}})
			require.Error(t, err)
			require.Nil(t, found)
			if tc.stamp >= 0 {
				_, err = l.ReadableEnd(t.Context(), []int32{0})
				require.Error(t, err)
			}
		})
	}
	b := &lookupBackend{values: map[int32]int64{0: 1}}
	l := NewOffsetLookupFactory().New(b, b)
	_, err := l.AtOrAfterTimestamp(t.Context(), []TimestampOffsetQuery{{Partition: 0}, {Partition: 0}})
	require.ErrorContains(t, err, "repeats")
}
