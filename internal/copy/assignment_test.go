package copy

import (
	"testing"
)

func TestReassignedPartitionResumesWithFreshRunnerState(t *testing.T) {
	// Given a partition paused by its uncommitted record cap.
	r, source, sink, _, _ := fixture(t)
	r.limits.RecordCap, r.limits.PartitionRecordCap = 1, 1
	if err := source.Start(r, nil); err != nil {
		t.Fatal(err)
	}
	if err := r.input(incoming(0, 0, 1)); err != nil {
		t.Fatal(err)
	}
	if err := r.pause(); err != nil {
		t.Fatal(err)
	}
	if !source.paused[0] {
		t.Fatal("fixture did not reach the pause cap")
	}

	// When ownership is revoked and the source clears pause before reassigning.
	err := r.Revoke([]int32{0}, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Pause(0, false); err != nil {
		t.Fatal(err)
	}
	err = r.Assign([]Assignment{{Cursor: Cursor{Partition: 0, Position: 0}}})
	if err != nil {
		t.Fatal(err)
	}
	err = r.input(incoming(0, 0, 1))

	// Then the new generation publishes again and late old delivery is fenced.
	if err != nil || source.paused[0] || len(sink.writes) != 2 || sink.writes[0].Generation == sink.writes[1].Generation {
		t.Fatal("reassigned partition stalled or reused ownership", err)
	}
	if r.tracker.Settle(sink.writes[0], true) {
		t.Fatal("revoked delivery resolved new work")
	}
}
