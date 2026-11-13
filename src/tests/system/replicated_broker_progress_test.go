//go:build integration && fault && load && harness

package compat

import (
	"testing"
	"time"

	"github.com/pagrus7/kaferry/src/tests/system/faults"
	"github.com/stretchr/testify/require"
)

func TestBrokerRestartRechecksFailureAfterHealthObservation(t *testing.T) {
	for _, scenario := range []struct {
		name, terminal        string
		allow, scope, restart bool
	}{
		{name: "unsupported", terminal: "unexpected invariant failure", allow: true},
		{name: "permitted", terminal: "poll source: transport diagnostic changed", allow: true, scope: true, restart: true},
		{name: "unscoped", terminal: "poll source: transport diagnostic changed", allow: true},
		{name: "unknown failure inside fault", terminal: "unexpected invariant failure", allow: true, scope: true},
		{name: "ISR forbids restart", terminal: "context deadline exceeded"},
		{name: "successful exit", allow: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			// Given a worker still alive at the pre-query liveness check.
			process := brokerLogChild(t, scenario.terminal)
			copy := brokerFaultChildCopy(t, process)
			if !scenario.scope {
				copy.active = nil
			}
			require.False(t, process.finished())
			copy.live(scenario.allow)
			// When a successful health observation completes after that worker exits.
			healthy := func() bool {
				require.NoError(t, process.stdin.Close())
				_ = process.wait()
				return true
			}()
			require.True(t, healthy)
			restarts := 0
			proof := copy.failureProof(process)
			err := restartBrokerWorker(process, scenario.allow, func() { restarts++ }, proof)
			// Then unsupported exits fail before either measurement or replacement.
			if scenario.restart {
				require.NoError(t, err)
				require.Equal(t, 1, restarts)
			} else {
				require.Error(t, err)
				require.Zero(t, restarts)
			}
		})
	}
}

func TestBrokerRestartRequiresCurrentLifetimeInsideRecoveryWindow(t *testing.T) {
	// Given an actual healthy child and recorded broker fault, when it exits,
	// then restart belongs only to this lifetime and the configured recovery.
	process := brokerLogChild(t, "save progress: transport unavailable")
	s := brokerFaultChildCopy(t, process)
	require.NoError(t, process.stdin.Close())
	require.Error(t, process.wait())
	proof := s.failureProof(process)
	_, err := completedBrokerFailure(process, true, proof)
	require.NoError(t, err)
	proof.Before.Worker.Lifetime.Number++
	_, err = completedBrokerFailure(process, true, proof)
	require.Error(t, err, "an earlier worker cannot lend its progress to a restart")
	proof.Before.Worker.Lifetime.Number--
	proof.Start = process.stopped.Add(time.Second)
	_, err = completedBrokerFailure(process, true, proof)
	require.Error(t, err, "a pre-injection exit is not an observed fault outcome")
	proof.Start = s.active.Event.Start
	proof.Heal = process.stopped.Add(-time.Duration(proof.RecoveryMS+1) * time.Millisecond)
	_, err = completedBrokerFailure(process, true, proof)
	require.Error(t, err, "configured recovery cannot be extended by a late exit")
}

func TestNetworkOutageRestartRequiresFirstWorkerRSSAndActiveCut(t *testing.T) {
	// Given the healthy first worker under a recorded destination route cut.
	process := brokerLogChild(t, "save progress: changed connection diagnostic")
	s := brokerFaultChildCopy(t, process)
	s.caseID = "network/destination-outage"
	s.active.Event.Role = faults.Role{Kind: "worker-destination", Cluster: "destination", Worker: 1}
	s.active.Event.Fault.Kind = "cut"
	s.memories = []func() rssPeak{func() rssPeak { return rssPeak{peak: 4096, samples: 2} }}
	require.NoError(t, process.stdin.Close())
	require.Error(t, process.wait())
	// When it exits 1 inside the cut, then retained live RSS and the actual
	// lifetime permit restart without a dependency-specific diagnostic.
	proof := s.failureProof(process)
	_, err := completedBrokerFailure(process, true, proof)
	require.NoError(t, err)
	rss := proof.Exit.RSS
	proof.Exit.RSS = nil
	_, err = completedBrokerFailure(process, true, proof)
	require.Error(t, err, "unavailable live RSS cannot pass")
	proof.Exit.RSS = rss
	proof.Heal = process.stopped.Add(-time.Millisecond)
	_, err = completedBrokerFailure(process, true, proof)
	require.Error(t, err, "whole-outage exit must occur before healing")
}
