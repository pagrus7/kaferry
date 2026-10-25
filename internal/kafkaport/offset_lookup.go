package kafkaport

import (
	"context"
	"fmt"
)

type TimestampOffsetQuery struct {
	Partition   int32
	TimestampMS int64
}
type TimestampOffsetMatch struct {
	Partition int32
	Offset    int64
	Found     bool
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
	// AtOrAfterTimestamp returns the earliest retained match per query, or Found=false.
	AtOrAfterTimestamp(context.Context, []TimestampOffsetQuery) ([]TimestampOffsetMatch, error)
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
func (l *offsetLookup) AtOrAfterTimestamp(ctx context.Context, queries []TimestampOffsetQuery) ([]TimestampOffsetMatch, error) {
	if len(queries) == 0 {
		return []TimestampOffsetMatch{}, nil
	}
	stamps := make(map[int32]int64, len(queries))
	for _, q := range queries {
		if q.TimestampMS < 0 {
			return nil, fmt.Errorf("timestamp lookup requires a non-negative timestamp")
		}
		if _, exists := stamps[q.Partition]; exists {
			return nil, fmt.Errorf("timestamp lookup repeats a partition")
		}
		stamps[q.Partition] = q.TimestampMS
	}
	found, err := l.times.OffsetsForTimes(ctx, stamps)
	if err != nil {
		return nil, err
	}
	matches := make([]TimestampOffsetMatch, 0, len(queries))
	for _, q := range queries {
		offset, ok := found[q.Partition]
		if !ok || offset < -1 {
			return nil, fmt.Errorf("missing or invalid timestamp result for partition %d", q.Partition)
		}
		matches = append(matches, TimestampOffsetMatch{Partition: q.Partition, Offset: offset, Found: offset >= 0})
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
