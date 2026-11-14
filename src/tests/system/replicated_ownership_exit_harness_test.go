//go:build integration && fault && load && harness

package compat

import (
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/src/tests/system/faults"
	"github.com/stretchr/testify/require"
)

func TestOwnershipCoordinatorRestartRequiresRecordedScope(t *testing.T) {
	for _, route := range []int{1, 2} {
		t.Run(fmt.Sprint(route), func(t *testing.T) {
			// Given an actual selected worker and the recorded coordinator kill.
			process := brokerLogChild(t, brokerWorkSummary+"save progress: transport diagnostic changed")
			copy := brokerFaultChildCopy(t, process)
			copy.active = nil
			started := copy.identities[0].Started
			worker := &ownershipCaseWorker{route: route, process: process, started: started, lifetime: copy.identities[0]}
			s := &ownershipCase{t: t, id: "ownership/coordinator-kill-rebalance", group: "group", lifecycle: copy,
				states: make(map[string]ownershipCaseObservation),
				events: []map[string]any{{"reason": "OS confirms held worker", "observed_at": started.Add(-time.Second)}},
				coordinatorFault: &faults.Injection{Event: faults.Event{Role: faults.Role{Kind: "coordinator", Cluster: "source", Group: "group"},
					Fault: faults.Fault{Kind: "kill"}, Targets: []faults.Target{{Broker: 1}}}}}
			if route == 1 {
				s.signal(worker, syscall.SIGSTOP, "hold old member while a selected-binary peer joins")
			}
			s.states["preparing_rebalance"] = ownershipCaseObservation{Started: time.Now().UTC()}
			atKill := time.Now().UTC()
			s.states["at_kill"] = ownershipCaseObservation{State: "PreparingRebalance", Coordinator: 1, Finished: atKill, Members: make([]ownershipMember, 2)}
			s.failedAfter = atKill.Add(time.Nanosecond)
			s.events = append(s.events, map[string]any{"coordinator_kill_finished": s.failedAfter})
			if route == 1 {
				s.signal(worker, syscall.SIGCONT, "resume held worker after coordinator kill")
			}
			require.NoError(t, process.stdin.Close())
			require.Error(t, process.wait())
			exit, err := copy.retainExit(process)
			require.NoError(t, err)
			// When its own operation label reports the failure, then either
			// route can restart with only its recorded hold/resume intentions.
			_, known := s.coordinatorFailure(worker, exit.Witness, process.logs())
			require.True(t, known)
			intents := exit.Witness.DriverIntent
			exit.Witness.DriverIntent = append(append([]cliSignalIntent(nil), intents...), cliSignalIntent{At: atKill, Signal: "interrupt", Reason: "driver stop"})
			_, known = s.coordinatorFailure(worker, exit.Witness, process.logs())
			require.False(t, known, "an intentional stop cannot borrow a fault restart")
			exit.Witness.DriverIntent = intents
			s.states["broker_restored"] = ownershipCaseObservation{Finished: process.stopped.Add(-time.Millisecond)}
			_, known = s.coordinatorFailure(worker, exit.Witness, process.logs())
			require.False(t, known, "configured recovery ends before the replacement boundary")
		})
	}
}
