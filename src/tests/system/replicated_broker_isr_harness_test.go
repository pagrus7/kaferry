//go:build integration && fault && load && harness

package compat

import (
	"testing"
	"time"

	"github.com/pagrus7/kaferry/src/tests/system/faults"
	"github.com/stretchr/testify/require"
)

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
