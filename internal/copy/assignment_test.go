package copy

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReassignedPartitionResumesWithFreshRunnerState(t *testing.T) {
	// Given a partition paused by its uncommitted record cap.
	r, source, sink, _, _ := fixture(t)
	r.limits.RecordCap, r.limits.PartitionRecordCap = 1, 1
	require.NoError(t, source.Start(r, nil))
	require.NoError(t, r.input(incoming(0, 0, 1)))
	require.NoError(t, r.pause())
	if !source.paused[0] {
		t.Fatal("fixture did not reach the pause cap")
	}

	// When ownership is revoked and the source clears pause before reassigning.
	err := r.Revoke([]int32{0}, false)
	require.NoError(t, err)
	require.NoError(t, source.Pause(0, false))
	err = r.Assign([]Assignment{{Cursor: Cursor{Partition: 0, Position: 0}}})
	require.NoError(t, err)
	err = r.input(incoming(0, 0, 1))

	// Then the new generation publishes again and late old delivery is fenced.
	if err != nil || source.paused[0] || len(sink.writes) != 2 || sink.writes[0].Generation == sink.writes[1].Generation {
		t.Fatal("reassigned partition stalled or reused ownership", err)
	}
	if r.tracker.Settle(sink.writes[0], true) {
		t.Fatal("revoked delivery resolved new work")
	}
}
