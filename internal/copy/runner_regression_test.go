package copy

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
)

func TestFatalDeliveryFailsDespiteSkipPolicies(t *testing.T) {
	// Given confirmed work followed by a fatal report with no record token.
	r, source, sink, store, clock := fixture(t)
	r.plan.Policies = plan.Policies{MissingTimestamp: "SKIP", RejectedTimestamp: "SKIP", OtherRecord: "SKIP"}
	first, second, unread := incoming(0, 0, 1), incoming(0, 1, 1), incoming(0, 2, 1)
	source.events = []Event{{Input: &first}, {Input: &second}, {Input: &unread}}
	fatal := errors.New("fatal producer failure")
	sink.submit = func(_ Record, token Token) error {
		if token.Sequence == 1 {
			sink.reports <- Delivery{Token: token}
		} else {
			sink.reports <- Delivery{Fatal: true, Err: fatal}
		}
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	deadline := clock.Now().Add(time.Second)
	clock.tick = func() {
		if !clock.Now().Before(deadline) {
			cancel()
		}
	}

	// When the copy receives the fatal report.
	err := r.Run(ctx)

	// Then it fails, stops intake, and saves only the confirmed prefix.
	if !errors.Is(err, fatal) || exitCode(err) != 1 || r.skipped != 0 || len(sink.writes) != 2 {
		t.Fatalf("err=%v skipped=%d writes=%v", err, r.skipped, sink.writes)
	}
	if len(store.saves) != 1 || len(store.saves[0]) != 1 || store.saves[0][0] != (Cursor{Partition: 0, Position: 1}) {
		t.Fatalf("unsafe progress: %v", store.saves)
	}
}

func TestQueueFullDeadlinePreservesConfirmedProgress(t *testing.T) {
	// Given a producer that confirms one record and remains full for the next.
	r, source, sink, store, clock := fixture(t)
	r.limits.DeliveryTimeout = "60ms"
	first, blocked, unread := incoming(0, 0, 1), incoming(0, 1, 1), incoming(0, 2, 1)
	source.events = []Event{{Input: &first}, {Input: &blocked}, {Input: &unread}}
	attempts := 0
	sink.submit = func(_ Record, token Token) error {
		if token.Sequence == 1 {
			sink.reports <- Delivery{Token: token}
			return nil
		}
		attempts++
		return &Busy{}
	}
	started := clock.Now()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clock.tick = func() {
		if clock.Now().Sub(started) >= time.Second {
			cancel()
		}
	}

	// When the configured queue-full budget expires.
	err := r.Run(ctx)

	// Then retries are bounded and neither blocked nor unread work is committed.
	if err == nil || !strings.Contains(err.Error(), "queue remained full for 60ms") || attempts < 2 || len(sink.writes) != 1 {
		t.Fatalf("err=%v attempts=%d writes=%v", err, attempts, sink.writes)
	}
	if elapsed := clock.Now().Sub(started); elapsed < 60*time.Millisecond || elapsed > 120*time.Millisecond {
		t.Fatalf("queue deadline elapsed=%s", elapsed)
	}
	if len(source.events) != 1 || len(store.saves) != 1 || store.saves[0][0] != (Cursor{Partition: 0, Position: 1}) {
		t.Fatalf("unread=%v progress=%v", source.events, store.saves)
	}
}

func TestRevokeDiscardsHeldWorkBeforeReassignment(t *testing.T) {
	for _, held := range []string{"queue-full submission", "deferred input and EOF"} {
		for _, lost := range []bool{false, true} {
			t.Run(held+map[bool]string{false: "/revoked", true: "/lost"}[lost], func(t *testing.T) {
				// Given held work for a partition that is about to change owners.
				r, _, sink, _, _ := fixture(t)
				require.NoError(t, r.Assign([]Assignment{{Cursor: Cursor{Partition: 0, Position: 0}}}))
				if held == "queue-full submission" {
					sink.submit = func(Record, Token) error { return &Busy{} }
				} else {
					r.limits.RecordCap = 1
				}
				require.NoError(t, r.input(incoming(0, 0, 1)))
				var old Token
				if r.pending != nil {
					old = r.pending.token
				} else {
					old = sink.writes[0]
					require.NoError(t, r.accept(Event{Input: ptrInput(incoming(0, 1, 1)), EOF: &Cursor{Partition: 0, Position: 2}}))
					if r.deferred == nil || r.deferredEOF == nil {
						t.Fatal("fixture did not hold input and EOF")
					}
				}
				before := len(sink.writes)

				// When ownership is revoked and the partition is reassigned for replay.
				require.NoError(t, r.Revoke([]int32{0}, lost))
				require.NoError(t, r.Assign([]Assignment{{Cursor: Cursor{Partition: 0, Position: 0}}}))
				sink.submit = nil
				require.NoError(t, r.advance())

				// Then held work and EOF cannot leak into the new assignment.
				if r.pending != nil || r.deferred != nil || r.deferredEOF != nil || len(sink.writes) != before || r.partitions[0].stop != nil {
					t.Fatal("revoked work survived reassignment")
				}
				require.NoError(t, r.input(incoming(0, 0, 1)))
				if len(sink.writes) != before+1 || sink.writes[before].Generation == old.Generation {
					t.Fatal("replay did not receive a fresh generation")
				}
			})
		}
	}
}

func ptrInput(input Input) *Input { return &input }

func TestRevokePreservesHeldWorkForRemainingPartition(t *testing.T) {
	// Given one partition filling the cap and another with deferred input and EOF.
	r, _, sink, _, _ := fixture(t)
	r.limits.RecordCap = 1
	require.NoError(t, r.Assign([]Assignment{{Cursor: Cursor{Partition: 0, Position: 0}}, {Cursor: Cursor{Partition: 1, Position: 0}}}))
	require.NoError(t, r.input(incoming(0, 0, 1)))
	require.NoError(t, r.accept(Event{Input: ptrInput(incoming(1, 0, 1)), EOF: &Cursor{Partition: 1, Position: 1}}))

	// When only the partition using the cap loses ownership.
	require.NoError(t, r.Revoke([]int32{0}, true))
	require.NoError(t, r.advance())

	// Then the remaining partition's held record and EOF are processed normally.
	if len(sink.writes) != 2 || sink.writes[1].Partition != 1 || r.partitions[1].stop == nil || r.partitions[1].position != 1 {
		t.Fatalf("remaining partition lost input or EOF: writes=%v state=%+v", sink.writes, r.partitions[1])
	}
}

func TestHalfCapFlushesAndCommitsBeforePeriodicDeadline(t *testing.T) {
	for _, cap := range []string{"total records", "total bytes", "partition records", "partition bytes"} {
		t.Run(cap, func(t *testing.T) {
			// Given one record below half the selected cap and no periodic commit due.
			r, _, sink, store, clock := fixture(t)
			require.NoError(t, r.Assign([]Assignment{{Cursor: Cursor{Partition: 0, Position: 0}}}))
			r.lastCommit = clock.Now()
			switch cap {
			case "total records":
				r.limits.RecordCap = 4
			case "total bytes":
				r.limits.ByteCap = incoming(0, 0, 1).Record.Size() * 4
			case "partition records":
				r.limits.PartitionRecordCap = 4
			case "partition bytes":
				r.limits.PartitionByteCap = incoming(0, 0, 1).Record.Size() * 4
			}
			flushes := 0
			sink.flush = func() error {
				flushes++
				for _, token := range sink.writes {
					sink.reports <- Delivery{Token: token}
				}
				return nil
			}
			require.NoError(t, r.input(incoming(0, 0, 1)))
			if err := r.checkpoint(); err != nil || flushes != 0 || len(store.saves) != 0 {
				t.Fatal("committed before half the cap", err)
			}

			// When a second record reaches half the cap without advancing time.
			require.NoError(t, r.input(incoming(0, 1, 1)))
			err := r.checkpoint()

			// Then the sink flush precedes a confirmed commit and releases accounting.
			if err != nil || flushes != 1 || len(store.saves) != 1 || store.saves[0][0] != (Cursor{Partition: 0, Position: 2}) {
				t.Fatalf("err=%v flushes=%d progress=%v", err, flushes, store.saves)
			}
			if records, bytes := r.tracker.Total(); records != 0 || bytes != 0 {
				t.Fatalf("committed work retained: %d/%d", records, bytes)
			}
		})
	}
}

func TestSignalDuringBoundedCopySavesProgressAndFails(t *testing.T) {
	for _, kind := range []string{"FILE", "TOPIC"} {
		t.Run(kind, func(t *testing.T) {
			// Given a bounded source with confirmed work and another record in Poll.
			r, source, sink, store, clock := fixture(t)
			r.plan.Source.Kind = kind
			upper := int64(200000)
			r.plan.Window.Upper = &plan.Expression{LiteralMS: upper}
			source.assignments[0].Window.Upper = &upper
			first, unread := incoming(0, 0, 1), incoming(0, 1, 1)
			stamp := int64(100000)
			first.Record.TimestampMS = &stamp
			source.events = []Event{{Input: &first}, {Input: &unread}}
			sink.submit = func(_ Record, token Token) error { sink.reports <- Delivery{Token: token}; return nil }
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			started := clock.Now()
			clock.tick = func() {
				if len(sink.writes) == 1 || clock.Now().Sub(started) >= time.Second {
					cancel()
				}
			}

			// When cancellation arrives during the next poll.
			err := r.Run(ctx)

			// Then the bounded copy fails and commits only the completed record.
			if err == nil || exitCode(err) != 1 || !strings.Contains(err.Error(), "bounded copy interrupted") || len(sink.writes) != 1 {
				t.Fatalf("err=%v writes=%v", err, sink.writes)
			}
			if len(store.saves) != 1 || store.saves[0][0] != (Cursor{Partition: 0, Position: 1}) {
				t.Fatalf("incorrect interrupted progress: %v", store.saves)
			}
		})
	}
}

func TestWriteRejectedDuringFollowShutdownFailsAndSavesOnlyConfirmedPrefix(t *testing.T) {
	// Given a following copy whose second write remains outstanding at the signal.
	r, source, sink, store, _ := fixture(t)
	r.plan.Source.Kind = "TOPIC"
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	first, second := incoming(0, 0, 1), incoming(0, 1, 1)
	source.events = []Event{{Input: &first}, {Input: &second}}
	rejected := errors.New("destination rejected outstanding write")
	sink.submit = func(_ Record, token Token) error {
		if len(sink.writes) == 1 {
			cancel()
			sink.reports <- Delivery{Token: sink.writes[0]}
			sink.reports <- Delivery{Token: token, Err: rejected}
		}
		return nil
	}

	// When shutdown drains the writes and learns that the second write failed.
	err := r.Run(ctx)

	// Then a following copy cannot report clean shutdown or commit the rejected input.
	if !errors.Is(err, rejected) || exitCode(err) != 1 || len(sink.writes) != 2 || !reflect.DeepEqual(store.saves, [][]Cursor{{{Partition: 0, Position: 1}}}) {
		t.Fatal("shutdown hid a write failure or advanced past it", err, store.saves)
	}
}
