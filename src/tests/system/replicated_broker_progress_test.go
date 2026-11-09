//go:build integration && fault && load

package compat

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBrokerRestartRechecksFailureAfterHealthObservation(t *testing.T) {
	for _, scenario := range []struct {
		name, terminal string
		allow, restart bool
	}{
		{name: "unsupported", terminal: "unexpected invariant failure", allow: true},
		{name: "permitted", terminal: "context deadline exceeded", allow: true, restart: true},
		{name: "ISR forbids restart", terminal: "context deadline exceeded"},
		{name: "successful exit", allow: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			// Given a worker still alive at the pre-query liveness check.
			process := brokerLogChild(t, scenario.terminal)
			copy := &brokerCopy{t: t, workers: []*cliProcess{process}}
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
			err := restartBrokerWorker(process, scenario.allow, func() { restarts++ })
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
