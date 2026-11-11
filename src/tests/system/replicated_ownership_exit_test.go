//go:build integration && fault && load

package compat

import (
	"fmt"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/src/tests/system/faults"
	"github.com/stretchr/testify/require"
)

// The actual coordinator kill and current worker lifetime authorize this one
// restart. Dependency diagnostic text and error ordering are not a gate.
func (s *ownershipCase) coordinatorFailure(worker *ownershipCaseWorker, exit *brokerExitWitness, logs string) (string, bool) {
	terminal := strings.Join(brokerTerminals(logs), "\n")
	fault := s.coordinatorFault
	atKill := s.states["at_kill"]
	restored := s.states["broker_restored"].Finished
	if s.id != "ownership/coordinator-kill-rebalance" || worker == nil || exit == nil || fault == nil ||
		fault.Event.Role != (faults.Role{Kind: "coordinator", Cluster: "source", Group: s.group}) || fault.Event.Fault.Kind != "kill" ||
		len(fault.Event.Targets) == 0 || fault.Event.Targets[0].Broker != atKill.Coordinator ||
		atKill.State != "PreparingRebalance" || len(atKill.Members) != 2 || atKill.Finished.After(s.failedAfter) ||
		s.failedAfter.IsZero() || s.coordinatorKillFinished().IsZero() || s.heldWorkerObserved().IsZero() ||
		exit.Lifetime != worker.lifetime || exit.ExitCode != 1 || exit.WaitError != "exit status 1" || exit.Signal != "" ||
		!worker.started.Before(exit.Stopped) || !s.failedAfter.Before(exit.Stopped) ||
		(!restored.IsZero() && exit.Stopped.After(restored)) || exit.Recorded.Before(exit.Stopped) ||
		exit.Log == nil || exit.LogError != "" || exit.LogBytes != len(logs) || exit.Log.SHA256 != brokerDigest(logs) {
		return terminal, false
	}
	if worker.route == 1 {
		if len(exit.DriverIntent) != 2 || exit.DriverIntent[0].Signal != syscall.SIGSTOP.String() ||
			exit.DriverIntent[0].Reason != "hold old member while a selected-binary peer joins" ||
			!exit.DriverIntent[0].At.Before(s.states["preparing_rebalance"].Started) ||
			exit.DriverIntent[1].Signal != syscall.SIGCONT.String() ||
			exit.DriverIntent[1].Reason != "resume held worker after coordinator kill" ||
			exit.DriverIntent[1].At.Before(s.coordinatorKillFinished()) || !s.coordinatorOwnedIntents(worker, exit) {
			return terminal, false
		}
	} else if worker.route != 2 || len(exit.DriverIntent) != 0 || !worker.started.After(s.heldWorkerObserved()) {
		return terminal, false
	}
	work, ready, err := ownershipCaseWork(logs)
	if err != nil || !ready || work.Summary == nil {
		return terminal, false
	}
	return terminal, strings.Contains("\n"+terminal, "\npoll source: ") || strings.Contains("\n"+terminal, "\nsave progress: ")
}

func (s *ownershipCase) coordinatorOwnedIntents(worker *ownershipCaseWorker, exit *brokerExitWitness) bool {
	numbers := map[string]int{syscall.SIGSTOP.String(): int(syscall.SIGSTOP), syscall.SIGCONT.String(): int(syscall.SIGCONT)}
	for _, intent := range exit.DriverIntent {
		matched := 0
		for _, event := range s.events {
			start, started := event["started"].(time.Time)
			end, finished := event["finished"].(time.Time)
			if started && finished && event["pid"] == worker.lifetime.PID && event["route"] == worker.route &&
				event["signal_number"] == numbers[intent.Signal] &&
				event["signal"] == intent.Signal && event["reason"] == intent.Reason &&
				!intent.At.Before(start) && !intent.At.After(end) && !end.After(exit.Stopped) {
				matched++
			}
		}
		if matched != 1 {
			return false
		}
	}
	return true
}

func (s *ownershipCase) heldWorkerObserved() time.Time {
	for _, event := range s.events {
		if event["reason"] == "OS confirms held worker" {
			at, _ := event["observed_at"].(time.Time)
			return at
		}
	}
	return time.Time{}
}

func (s *ownershipCase) coordinatorKillFinished() time.Time {
	for _, event := range s.events {
		if at, ok := event["coordinator_kill_finished"].(time.Time); ok {
			return at
		}
	}
	return time.Time{}
}

// Recheck the complete observed recovery window before starting replacements.
func (s *ownershipCase) validateCoordinatorExits() {
	require.False(s.t, s.states["broker_restored"].Finished.IsZero())
	for _, worker := range s.workers {
		if worker.reason != "coordinator fault fail-fast exit" {
			continue
		}
		exit, err := s.lifecycle.retainExit(worker.process)
		require.NoError(s.t, err)
		_, valid := s.coordinatorFailure(worker, exit.Witness, worker.process.logs())
		require.True(s.t, valid, "coordinator exit is outside its recorded process or fault/recovery scope")
	}
}

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
