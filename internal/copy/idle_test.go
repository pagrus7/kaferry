package copy

import (
	"bytes"
	"context"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
)

func TestUnassignedTimeoutHasDistinctOutcomeBeforeAndAfterIntake(t *testing.T) {
	for _, afterIntake := range []bool{false, true} {
		t.Run(map[bool]string{false: "before intake", true: "after intake"}[afterIntake], func(t *testing.T) {
			// Given a bounded worker that becomes unassigned, optionally after copying.
			r, source, sink, store, clock := fixture(t)
			upper, stamp := int64(1000), int64(900)
			r.plan.Source.Kind = "TOPIC"
			r.plan.Window.Upper = &plan.Expression{LiteralMS: upper}
			r.limits.IdleExit = "1s"
			var logs bytes.Buffer
			r.log = slog.New(slog.NewJSONHandler(&logs, nil))
			source.assignments = nil
			if afterIntake {
				source.assignments = []Assignment{{Cursor: Cursor{Partition: 0, Position: 0}, Window: plan.Window{Upper: &upper}}}
				input := incoming(0, 0, 1)
				input.Record.TimestampMS = &stamp
				source.events = []Event{{Input: &input}}
				sink.submit = func(_ Record, token Token) error {
					sink.reports <- Delivery{Token: token}
					return nil
				}
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			deadline := clock.Now().Add(5 * time.Second)
			revoked := false
			clock.tick = func() {
				if afterIntake && !revoked && len(sink.writes) > 0 {
					require.NoError(t, r.Revoke([]int32{0}, false))
					require.NoError(t, r.Assign(nil))
					revoked = true
				}
				if !clock.Now().Before(deadline) {
					cancel()
				}
			}

			// When the unassigned interval reaches the configured timeout.
			err := r.Run(ctx)

			// Then report only the local idle outcome, preserving any saved progress.
			if err == nil || exitCode(err) != 3 || err.Error() != "worker held no partitions for 1s" {
				t.Fatalf("exit=%d err=%v", exitCode(err), err)
			}
			require.Contains(t, logs.String(), `"msg":"local idle exit","idle_exit":"1s","exit_code":3`, "idle outcome is missing its structured context")
			if afterIntake {
				if !revoked || len(sink.writes) != 1 || !reflect.DeepEqual(store.saves, [][]Cursor{{{Partition: 0, Position: 1}}}) {
					t.Fatal("confirmed progress was lost or advanced after release", sink.writes, store.saves)
				}
			} else if len(sink.writes) != 0 || len(store.saves) != 0 {
				t.Fatal("unassigned worker wrote or committed progress", sink.writes, store.saves)
			}
		})
	}
}

func TestAssignmentBeforeIdleDeadlineCompletesNormally(t *testing.T) {
	// Given a bounded worker with an empty assignment and one-second idle timeout.
	r, source, sink, store, clock := fixture(t)
	upper, stamp := int64(1000), int64(1001)
	r.plan.Source.Kind = "TOPIC"
	r.plan.Window.Upper = &plan.Expression{LiteralMS: upper}
	r.limits.IdleExit = "1s"
	source.assignments = nil
	started := clock.Now()
	assigned := false
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	clock.tick = func() {
		if !assigned && clock.Now().Sub(started) >= 500*time.Millisecond {
			require.NoError(t, r.Assign([]Assignment{{Cursor: Cursor{Partition: 0, Position: 0}, Window: plan.Window{Upper: &upper}}}))
			input := incoming(0, 0, 1)
			input.Record.TimestampMS = &stamp
			source.events = []Event{{Input: &input}}
			assigned = true
		}
		if clock.Now().Sub(started) >= 5*time.Second {
			cancel()
		}
	}

	// When a partition arrives before the deadline and reaches its timestamp stop.
	err := r.Run(ctx)

	// Then complete successfully without submitting or committing past the trigger.
	if err != nil || !assigned || r.timestampStops != 1 || len(sink.writes) != 0 || len(store.saves) != 1 || store.saves[0][0].Position != 0 || store.saves[0][0].Stop == nil {
		t.Fatal("assignment did not supersede idle termination", err, sink.writes, store.saves)
	}
}

func TestDelayedHandoffDistinguishesPartitionCompletionFromIdleExit(t *testing.T) {
	// Given A owns completed P0 and unfinished P1, and B is joining the group.
	a, _, _, _, _ := fixture(t)
	b, _, _, _, clockB := fixture(t)
	upper, stamp := int64(1000), int64(1001)
	for _, runner := range []*Runner{a, b} {
		runner.plan.Source.Kind = "TOPIC"
		runner.plan.Window.Upper = &plan.Expression{LiteralMS: upper}
		runner.limits.IdleExit = "1s"
	}
	if err := a.Assign([]Assignment{
		{Cursor: Cursor{Partition: 0, Position: 0}, Window: plan.Window{Upper: &upper}},
		{Cursor: Cursor{Partition: 1, Position: 0}, Window: plan.Window{Upper: &upper}},
	}); err != nil {
		t.Fatal(err)
	}
	trigger := incoming(0, 0, 1)
	trigger.Record.TimestampMS = &stamp
	require.NoError(t, a.input(trigger))
	require.Nil(t, a.partitions[1].stop, "fixture completed the partition intended for handoff")

	// When P1 is revoked from A and its assignment to B is delayed past idle_exit.
	require.NoError(t, a.Revoke([]int32{1}, false))
	require.NoError(t, b.Assign(nil))
	doneA, errA := a.step(t.Context())
	clockB.Sleep(2 * time.Second)
	doneB, errB := b.step(t.Context())

	// Then A completes its current assignment and B reports being unassigned.
	if !doneA || errA != nil || !doneB || errB == nil || exitCode(errB) != 3 {
		t.Fatalf("A: done=%v err=%v; B: done=%v err=%v", doneA, errA, doneB, errB)
	}
}
