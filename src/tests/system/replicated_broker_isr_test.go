//go:build integration && fault && load

package compat

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/src/tests/system/faults"
	"github.com/stretchr/testify/require"
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

func TestBrokerISRSelectionRequiresActiveSameLeaderBacklog(t *testing.T) {
	// Given an idle target partition and another partition on the same broker,
	// when only the latter has observed ISR1 and new unacknowledged input, then
	// select that actual exposure and reject missing or retargeted proof.
	for _, scenario := range []string{"valid", "wrong route", "wrong leader", "wrong pre-fault leader", "no ISR1", "no backlog", "save past acknowledgements", "no unresolved work", "after heal", "before fault"} {
		t.Run(scenario, func(t *testing.T) {
			proof := brokerEvidenceProof("destination")
			before := proof.Before
			current := brokerEvidenceProof("destination").Before
			current.At = proof.Start.Add(time.Second)
			current.Ends = make([]int64, replicatedPartitions)
			for partition := range current.Ends {
				current.Ends[partition] = 1
			}
			current.Topology["destination"][6].ISR = []int32{1}
			current.Publishing.Acknowledged[6] = 5
			event := faults.Event{Role: faults.Role{Kind: "leader", Cluster: "destination", Listener: "internal", Topic: "destination"},
				Fault: faults.Fault{Kind: "cut"}, Start: proof.Start,
				Targets: []faults.Target{{Cluster: "destination", Broker: 1, Listener: "internal", Container: "owned", Proxy: "destination-1-internal"}}}
			switch scenario {
			case "wrong route":
				event.Targets[0].Proxy = "destination-2-internal"
			case "wrong leader":
				current.Topology["destination"][6].Leader = 2
			case "wrong pre-fault leader":
				before.Topology["destination"][6].Leader = 2
			case "no ISR1":
				current.Topology["destination"][6].ISR = []int32{1, 2}
			case "no backlog":
				current.Publishing.Acknowledged[6] = 4
			case "save past acknowledgements":
				current.Saved[6] = 2
			case "no unresolved work":
				current.Pending.Confirmed = current.Pending.Submitted
			case "after heal":
				event.Heal = proof.Start.Add(time.Second)
			case "before fault":
				current.At = event.Start.Add(-time.Second)
			}
			partition, selected := brokerISRPartition(event, before, current)
			require.Equal(t, scenario == "valid", selected)
			if selected {
				require.Equal(t, 6, partition, "partition zero did not lose ISR")
			}
		})
	}
}
