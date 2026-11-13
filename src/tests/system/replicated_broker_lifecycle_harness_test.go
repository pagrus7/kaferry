//go:build integration && fault && load && harness

package compat

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBrokerExitWitnessRetainsUnsupportedAndIntentionalExitsBeforeAssertions(t *testing.T) {
	t.Run("original cancellation contract", brokerCancellationWitness)
	for _, terminal := range []string{
		"shutdown timed out while saving progress; exiting without further cleanup",
		"poll source: transport changed its diagnostic\nsave progress: another cause",
		brokerWorkSummary + "opaque destination delivery failure",
	} {
		t.Run(terminal, func(t *testing.T) {
			// Given complete progress from this actual live lifetime, when its
			// operation failure follows injection, then restart uses the
			// immutable witness rather than a prior worker's healthy counters.
			process := brokerLogChild(t, terminal)
			s := brokerFaultChildCopy(t, process)
			require.NoError(t, process.stdin.Close())
			require.Error(t, process.wait())
			proof := s.failureProof(process)
			logs, err := process.currentLogs()
			require.NoError(t, err)
			require.True(t, proof.Exit.binds(logs, proof.Before), "raw lifecycle must bind the observed current progress: %+v", proof.Exit)
			require.True(t, proof.restartScope(logs), "registered active fault must bind: %+v", proof)
			failure, err := completedBrokerFailure(process, true, proof)
			require.NoError(t, err)
			require.Equal(t, strings.Join(brokerTerminals(terminal), "\n"), failure)
			_, err = completedBrokerFailure(process, false, proof)
			require.Error(t, err, "ISR keeps its no-restart gate")
			require.NotNil(t, s.exits[process], "exit witness is retained before resource checks")
		})
	}
	t.Run("scoped driver intent", func(t *testing.T) {
		// Given an accepted current-lifetime exit 1 inside its recorded fault.
		process := brokerLogChild(t, "save progress: transport unavailable")
		s := brokerFaultChildCopy(t, process)
		require.NoError(t, process.stdin.Close())
		require.Error(t, process.wait())
		proof := s.failureProof(process)
		_, err := completedBrokerFailure(process, true, proof)
		require.NoError(t, err)
		// When the driver requested a stop before that exit, then this otherwise
		// valid fault proof cannot authorize a replacement worker.
		proof.Exit.DriverIntent = []cliSignalIntent{{At: proof.Before.At, Signal: "interrupt", Reason: "driver requested stop"}}
		restarts := 0
		err = restartBrokerWorker(process, true, func() { restarts++ }, proof)
		require.Error(t, err)
		require.Zero(t, restarts)
	})
	for _, intentional := range []bool{false, true} {
		t.Run(fmt.Sprint(intentional), func(t *testing.T) {
			// Given a real live worker, when it exits with an unsupported terminal
			// or a driver signal, then the original logs remain available.
			process := brokerLogChild(t, "unexpected invariant failure")
			s := &brokerCopy{t: t, directory: t.TempDir(), workers: []*cliProcess{process}, identities: []brokerLifetime{{Number: 1, PID: process.pid(), Started: time.Now().UTC()}}}
			if intentional {
				require.NoError(t, process.signal(os.Interrupt, "regression stop"))
			} else {
				require.NoError(t, process.stdin.Close())
			}
			require.Error(t, process.wait())
			exit, err := s.retainExit(process)
			require.NoError(t, err)
			require.Equal(t, process.stopped, exit.Witness.Stopped)
			if intentional {
				require.Len(t, exit.Witness.DriverIntent, 1)
				require.NotEmpty(t, exit.Witness.Signal)
			} else {
				require.Equal(t, []string{"unexpected invariant failure"}, exit.Witness.Terminal)
			}
			_, err = completedBrokerFailure(process, true)
			require.Error(t, err, "unscoped failure cannot reach replacement")
			original, err := os.ReadFile(filepath.Join(s.directory, exit.Reference.Path))
			require.NoError(t, err)
			require.Equal(t, exit.Reference.SHA256, brokerDigest(string(original)))
			raw, err := os.ReadFile(filepath.Join(s.directory, exit.Witness.Log.Path))
			require.NoError(t, err)
			require.Equal(t, exit.Witness.Log.SHA256, brokerDigest(string(raw)))
			// Later cleanup/read failures cannot rewrite the original witness.
			process.output += "-missing"
			again, err := s.retainExit(process)
			require.NoError(t, err)
			require.Same(t, exit, again)
			require.ErrorIs(t, writeBrokerImmutable(filepath.Join(s.directory, exit.Reference.Path), []byte("replacement")), os.ErrExist)
			retained, err := os.ReadFile(filepath.Join(s.directory, exit.Reference.Path))
			require.NoError(t, err)
			require.Equal(t, original, retained)
		})
	}
	// An unreadable final log still leaves an explicit immutable failure record.
	process := brokerLogChild(t, "unexpected invariant failure")
	require.NoError(t, process.stdin.Close())
	require.Error(t, process.wait())
	process.output += "-missing"
	s := &brokerCopy{directory: t.TempDir(), workers: []*cliProcess{process}, identities: []brokerLifetime{{Number: 1, PID: process.pid(), Started: process.stopped.Add(-time.Second)}}}
	exit, err := s.retainExit(process)
	require.NoError(t, err)
	require.NotEmpty(t, exit.Witness.LogError)
	require.Nil(t, exit.Witness.Log)
}
