package copy

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
)

func TestRecordPoliciesCommitSkippedInputsOnly(t *testing.T) {
	for _, tc := range []struct {
		name, class string
		policies    plan.Policies
		skip        bool
	}{
		{"missing default", "missing_timestamp", plan.Policies{}, false},
		{"missing skip", "missing_timestamp", plan.Policies{MissingTimestamp: "SKIP"}, true},
		{"missing ignores other policy", "missing_timestamp", plan.Policies{OtherRecord: "SKIP"}, false},
		{"rejected default", "rejected_timestamp", plan.Policies{}, false},
		{"rejected skip", "rejected_timestamp", plan.Policies{RejectedTimestamp: "SKIP"}, true},
		{"rejected ignores other policy", "rejected_timestamp", plan.Policies{OtherRecord: "SKIP"}, false},
		{"validation default", "validation", plan.Policies{}, false},
		{"validation skip", "validation", plan.Policies{OtherRecord: "SKIP"}, true},
		{"unknown failure cannot skip", "unclassified", plan.Policies{OtherRecord: "SKIP"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given good records surrounding one input rejected before submission.
			r, source, sink, store, _ := fixture(t)
			r.plan.Policies = tc.policies
			failure := errors.New("invalid input")
			sink.submit = func(record Record, token Token) error {
				if record.Value[0] == 1 {
					if tc.class == "unclassified" {
						return failure
					}
					return fmt.Errorf("wrapped: %w", &RecordError{Class: tc.class, Err: failure})
				}
				sink.reports <- Delivery{Token: token}
				return nil
			}
			lo, hi, stamp := int64(0), int64(10), int64(5)
			if tc.class == "missing_timestamp" {
				source.assignments[0].Window = plan.Window{Lower: &lo, Upper: &hi}
			}
			for i := range 3 {
				input := incoming(0, int64(i), 1)
				input.Record.Value[0] = byte(i)
				input.Record.TimestampMS = &stamp
				if i == 1 && tc.class == "missing_timestamp" {
					input.Record.TimestampMS = nil
				}
				source.events = append(source.events, Event{Input: &input})
			}
			source.events = append(source.events, Event{EOF: &Cursor{Partition: 0, Position: 3}})

			// When the copy applies the selected record policy.
			err := r.Run(context.Background())

			// Then only a safe skip may advance the committed prefix beyond the bad input.
			wantPosition, wantWrites, wantSkipped := int64(1), 1, uint64(0)
			if tc.skip {
				wantPosition, wantWrites, wantSkipped = 3, 2, 1
			}
			if (err == nil) != tc.skip || len(sink.writes) != wantWrites || r.skipped != wantSkipped {
				t.Fatalf("error=%v writes=%d skipped=%d", err, len(sink.writes), r.skipped)
			}
			if len(store.saves) == 0 || store.saves[len(store.saves)-1][0].Position != wantPosition {
				t.Fatalf("unsafe committed prefix: %+v", store.saves)
			}
			if !tc.skip && tc.class != "unclassified" && !strings.Contains(err.Error(), "partition 0") {
				t.Fatal("record failure lost its partition", err)
			}
		})
	}
}

func TestAsynchronousDeliveryFailureCannotBeSkipped(t *testing.T) {
	// Given ON_ERROR=SKIP and a batch failure reported after publication.
	r, source, sink, store, _ := fixture(t)
	r.plan.Policies = plan.Policies{MissingTimestamp: "SKIP", RejectedTimestamp: "SKIP", OtherRecord: "SKIP"}
	failure := errors.New("unattributed batch rejection")
	sink.submit = func(record Record, token Token) error {
		var err error
		if record.Value[0] == 1 {
			err = failure
		}
		sink.reports <- Delivery{Token: token, Err: err}
		return nil
	}
	for i := range 3 {
		input := incoming(0, int64(i), 1)
		input.Record.Value[0] = byte(i)
		source.events = append(source.events, Event{Input: &input})
	}

	// When the producer reports the second submission as failed.
	err := r.Run(context.Background())

	// Then the worker fails and commits only the successfully delivered prefix.
	if !errors.Is(err, failure) || r.skipped != 0 || len(sink.writes) != 2 {
		t.Fatal("asynchronous failure was skipped", err, r.skipped, sink.writes)
	}
	if len(store.saves) != 1 || store.saves[0][0].Position != 1 {
		t.Fatal("failed delivery advanced progress", store.saves)
	}
}

func TestTimeFilterKeepsInclusiveStartAndExclusiveEnd(t *testing.T) {
	// Given records on both sides of and exactly at each bound.
	r, source, sink, store, _ := fixture(t)
	lo, hi := int64(10), int64(20)
	source.assignments[0].Window = plan.Window{Lower: &lo, Upper: &hi}
	for i, stamp := range []int64{9, 10, 19, 20, 25} {
		input := incoming(0, int64(i), 1)
		input.Record.TimestampMS = &stamp
		source.events = append(source.events, Event{Input: &input})
	}
	source.events = append(source.events, Event{EOF: &Cursor{Partition: 0, Position: 5}})
	sink.submit = func(_ Record, token Token) error { sink.reports <- Delivery{Token: token}; return nil }

	// When the full source is processed through the timestamp filter.
	err := r.Run(context.Background())

	// Then only the two in-range records publish, while filtered inputs advance progress.
	if err != nil || len(sink.writes) != 2 || r.filtered != 3 || store.saves[len(store.saves)-1][0].Position != 5 {
		t.Fatal("incorrect time bounds", err, sink.writes, r.filtered, store.saves)
	}
}

func TestDeferredRecordRetainsFollowingEOFUntilBudgetIsReleased(t *testing.T) {
	// Given one uncommitted record and a second record held by the byte cap.
	r, _, sink, store, _ := fixture(t)
	r.limits.ByteCap, r.limits.PartitionByteCap = 150, 150
	require.NoError(t, r.Assign([]Assignment{{Cursor: Cursor{Partition: 0, Position: 0}}}))
	require.NoError(t, r.input(incoming(0, 0, 30)))
	require.NoError(t, r.input(incoming(0, 1, 200)))

	// When an already queued EOF arrives behind the held record.
	err := r.eof(Cursor{Partition: 0, Position: 2})

	// Then the partition stays open and keeps the EOF without observing it ahead of input.
	if err != nil || r.deferredEOF == nil || r.partitions[0].stop != nil || r.partitions[0].position != 1 {
		t.Fatal("EOF overtook deferred input", err, r.partitions[0])
	}

	// When the first record becomes durable and the held record can be admitted.
	sink.reports <- Delivery{Token: sink.writes[0]}
	require.NoError(t, r.deliveries())
	require.NoError(t, r.commit(r.ids()))
	err = r.admitDeferred()

	// Then both input and EOF are consumed in order, with the second delivery still pending.
	if err != nil || len(sink.writes) != 2 || r.deferred != nil || r.deferredEOF != nil || r.partitions[0].stop == nil || r.readyToExit() {
		t.Fatal("held EOF was lost or completed unresolved work", err)
	}

	// When the final submission succeeds.
	sink.reports <- Delivery{Token: sink.writes[1]}
	require.NoError(t, r.deliveries())
	err = r.commit(r.ids())

	// Then completion and saved progress include the whole source.
	if err != nil || !r.readyToExit() || store.saves[len(store.saves)-1][0].Position != 2 {
		t.Fatal("deferred source did not finish", err, store.saves)
	}
}

func TestDeferredRecordIsValidatedAfterBudgetIsReleased(t *testing.T) {
	// Given a valid write and a timestamp-less input too large for the remaining byte budget.
	r, source, sink, store, _ := fixture(t)
	r.limits.ByteCap, r.limits.PartitionByteCap = 150, 150
	lower := int64(1000)
	source.assignments[0].Window.Lower = &lower
	first, second := incoming(0, 0, 30), incoming(0, 1, 200)
	first.Record.TimestampMS = &lower
	eof := Cursor{Partition: 0, Position: 2}
	source.events = []Event{{Input: &first}, {Input: &second}, {EOF: &eof}}
	acknowledged := false
	sink.flush = func() error {
		if r.deferred != nil && !acknowledged {
			acknowledged = true
			sink.reports <- Delivery{Token: sink.writes[0]}
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// When the first write is confirmed, committed, and the held input is admitted.
	err := r.Run(ctx)

	// Then deferred admission keeps FAIL semantics and cannot publish or save the invalid record.
	var invalid *RecordError
	if !acknowledged || !errors.As(err, &invalid) || invalid.Class != "missing_timestamp" || exitCode(err) != 1 || len(sink.writes) != 1 || !reflect.DeepEqual(store.saves, [][]Cursor{{{Partition: 0, Position: 1}}}) {
		t.Fatal("held input bypassed validation or failed to stop", err, store.saves, sink.writes)
	}
}
