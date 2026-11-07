package kafkaport

import (
	"context"
	"fmt"
)

type TimestampOffsetMatch struct {
	Offset int64
	Found  bool
}

// OffsetLookup reads positions without changing a consumer or saving progress.
// Successful boundary replies contain a nonnegative offset for every requested ID.
// Timestamp replies contain one result per query; only a genuine no-match has Found=false.
// A missing partition response is an error.
type OffsetLookup interface {
	// Earliest returns the earliest retained offset per partition.
	Earliest(context.Context, []int32) (map[int32]int64, error)
	// ReadableEnd returns the exclusive read-committed end per partition.
	ReadableEnd(context.Context, []int32) (map[int32]int64, error)
	// ReplicatedEnd returns the exclusive replicated end, including unresolved transactions.
	ReplicatedEnd(context.Context, []int32) (map[int32]int64, error)
	// AtOrAfterTimestamp accepts Unix milliseconds keyed by partition and returns
	// the earliest retained match for each partition, or Found=false.
	AtOrAfterTimestamp(context.Context, map[int32]int64) (map[int32]TimestampOffsetMatch, error)
}
type OffsetLookupFactory interface {
	New(OffsetReader, TimeReader) OffsetLookup
}
type offsetLookupFactory struct{}

func NewOffsetLookupFactory() OffsetLookupFactory { return offsetLookupFactory{} }
func (offsetLookupFactory) New(offsets OffsetReader, times TimeReader) OffsetLookup {
	return &offsetLookup{offsets: offsets, times: times}
}

type offsetLookup struct {
	offsets OffsetReader
	times   TimeReader
}

func (l *offsetLookup) read(ctx context.Context, ids []int32, kind OffsetKind, committed bool) (map[int32]int64, error) {
	if len(ids) == 0 {
		return map[int32]int64{}, nil
	}
	found, err := l.offsets.Offsets(ctx, ids, kind, committed)
	if err != nil {
		return nil, err
	}
	if err := validateOffsets(ids, found); err != nil {
		return nil, err
	}
	return found, nil
}
func (l *offsetLookup) Earliest(ctx context.Context, ids []int32) (map[int32]int64, error) {
	return l.read(ctx, ids, EarliestOffset, false)
}
func (l *offsetLookup) ReadableEnd(ctx context.Context, ids []int32) (map[int32]int64, error) {
	return l.read(ctx, ids, LatestOffset, true)
}
func (l *offsetLookup) ReplicatedEnd(ctx context.Context, ids []int32) (map[int32]int64, error) {
	return l.read(ctx, ids, LatestOffset, false)
}
func (l *offsetLookup) AtOrAfterTimestamp(ctx context.Context, queries map[int32]int64) (map[int32]TimestampOffsetMatch, error) {
	if len(queries) == 0 {
		return map[int32]TimestampOffsetMatch{}, nil
	}
	for _, stamp := range queries {
		if stamp < 0 {
			return nil, fmt.Errorf("timestamp lookup requires a non-negative timestamp")
		}
	}
	found, err := l.times.OffsetsForTimes(ctx, queries)
	if err != nil {
		return nil, err
	}
	matches := make(map[int32]TimestampOffsetMatch, len(queries))
	for id := range queries {
		offset, ok := found[id]
		if !ok || offset < -1 {
			return nil, fmt.Errorf("missing or invalid timestamp result for partition %d", id)
		}
		matches[id] = TimestampOffsetMatch{Offset: offset, Found: offset >= 0}
	}
	return matches, nil
}

func validateOffsets(ids []int32, offsets map[int32]int64) error {
	for _, id := range ids {
		if offset, present := offsets[id]; !present || offset < 0 {
			return fmt.Errorf("missing or invalid offset for partition %d", id)
		}
	}
	return nil
}
