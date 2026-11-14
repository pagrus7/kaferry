//go:build integration && fault && load

package compat

import (
	"fmt"
	"slices"

	"github.com/pagrus7/kaferry/src/tests/system/faults"
)

// A disconnected follower with the same LEO remains in Kafka's ISR. Select an
// actually lagging partition on the originally resolved broker, retaining the
// injection role and route rather than assuming partition zero must shrink.
func brokerISRPartition(event faults.Event, before, current brokerObservation) (int, bool) {
	role := event.Role
	if role.Kind != "leader" || role.Cluster != "destination" || role.Listener != "internal" || role.Topic == "" ||
		event.Fault.Kind != "cut" || !event.Heal.IsZero() || !event.Start.Before(current.At) || !before.At.Before(event.Start) ||
		len(event.Targets) != 1 || !brokerHealthyCopy(before) || len(current.Topology["destination"]) != replicatedPartitions ||
		len(current.Saved) != replicatedPartitions || len(current.Ends) != replicatedPartitions || len(current.Publishing.Acknowledged) != replicatedPartitions ||
		current.Pending.Submitted <= current.Pending.Confirmed || role.Partition < 0 || role.Partition >= replicatedPartitions {
		return -1, false
	}
	target := event.Targets[0]
	if target.Cluster != role.Cluster || target.Listener != role.Listener || target.Container == "" ||
		target.Proxy != fmt.Sprintf("destination-%d-internal", target.Broker) ||
		before.Topology["destination"][role.Partition].Leader != target.Broker {
		return -1, false
	}
	for partition, part := range current.Topology["destination"] {
		if before.Topology["destination"][partition].Leader == target.Broker && part.Leader == target.Broker &&
			slices.Equal(part.ISR, []int32{target.Broker}) && current.Saved[partition] <= current.Ends[partition] &&
			current.Publishing.Acknowledged[partition] > max(current.Ends[partition], current.Saved[partition])+3 {
			return partition, true
		}
	}
	return -1, false
}
