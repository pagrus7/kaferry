//go:build integration && fault && load

package compat

import (
	"time"

	"github.com/pagrus7/kaferry/src/tests/system/faults"
)

func networkMeasurementState(start time.Time, elapsed time.Duration, count int64) brokerObservation {
	state := brokerObservation{At: start.Add(elapsed), Topology: make(map[string][]faults.Partition),
		Saved: make([]int64, replicatedPartitions), Ends: make([]int64, replicatedPartitions),
		Publishing: replicatedPublishing{Started: start, Observed: start.Add(elapsed),
			Acknowledged: make([]int64, replicatedPartitions), Offered: make([]int64, replicatedPartitions)}}
	state.Pending.Submitted, state.Pending.Confirmed = 100, 100
	for partition := range replicatedPartitions {
		state.Saved[partition], state.Ends[partition] = count, count
		state.Publishing.Acknowledged[partition], state.Publishing.Offered[partition] = count, count
		for _, side := range []string{"source", "destination"} {
			state.Topology[side] = append(state.Topology[side], faults.Partition{Leader: 1, ISR: []int32{1, 2, 3}})
		}
	}
	return state
}
