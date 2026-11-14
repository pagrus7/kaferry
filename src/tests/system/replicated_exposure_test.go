//go:build integration && fault && load && harness

package compat

import (
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/src/internal/config"
	"github.com/pagrus7/kaferry/src/internal/plan"
	"github.com/pagrus7/kaferry/src/internal/sql"
	"github.com/pagrus7/kaferry/src/tests/system/faults"
	"github.com/pelletier/go-toml/v2"
	"github.com/stretchr/testify/require"
)

func TestReplicatedZombieRSSStopsAtProcessExit(t *testing.T) {
	const helper = "KAFERRY_ZOMBIE_RSS_HELPER"
	if os.Getenv(helper) == "1" {
		_, err := io.Copy(io.Discard, os.Stdin)
		require.NoError(t, err)
		return
	}
	// Given a live selected-process analogue whose SQL release is still held.
	t.Setenv(helper, "1")
	process := launchCLI(t, t.Context(), os.Args[0], "-test.run=^TestReplicatedZombieRSSStopsAtProcessExit$")
	calls, sampled := 0, make(chan struct{}, 1)
	finish := watchReplicatedSamples(t, process, func() (int64, error) {
		calls++
		if process.finished() {
			return 0, errors.New("PID is gone")
		}
		if calls == 2 {
			sampled <- struct{}{}
		}
		return int64(42 + calls), nil
	})
	exit := observeReplicatedExit(process)
	// When it exits before the surviving peer's publication interval ends,
	// then RSS covers its live interval without polling the missing PID.
	select {
	case <-sampled:
	case <-time.After(time.Second):
		t.Fatal("live process was not sampled")
	}
	require.NoError(t, process.stdin.Close())
	require.NoError(t, process.wait(), process.logs())
	require.False(t, (<-exit).IsZero())
	measured := finish()
	require.GreaterOrEqual(t, measured.samples, 2)
	require.GreaterOrEqual(t, measured.peak, int64(44))
	require.Zero(t, measured.failures)
	require.Equal(t, measured, finish())

	for _, test := range []struct {
		name     string
		err      error
		failures int
	}{
		{"task gone before Wait publication", fmt.Errorf("native RSS: %w", syscall.ESRCH), 0},
		{"live sampling error", errors.New("RSS unavailable"), 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Given a sampler whose process completion has not been published.
			process := &cliProcess{done: make(chan struct{})}
			calls, sampled := 0, make(chan struct{})
			finish := watchReplicatedSamples(t, process, func() (int64, error) {
				calls++
				if calls == 1 {
					return 42, nil
				}
				if calls == 2 {
					close(sampled)
				}
				return 0, test.err
			})
			// When the OS reports a missing task or another sampling error.
			select {
			case <-sampled:
			case <-time.After(time.Second):
				t.Fatal("RSS error was not sampled")
			}
			measured := finish()
			// Then only independent task disappearance ends the live interval.
			require.False(t, process.finished())
			if test.failures == 0 {
				require.Zero(t, measured.failures)
			} else {
				require.Positive(t, measured.failures)
			}
			require.Equal(t, 1, measured.samples)
			require.EqualValues(t, 42, measured.peak)
		})
	}
}

func TestReplicatedZombieExposureSpansSessionWithinSaveDeadline(t *testing.T) {
	// Given the actual registered RPC/session/save bounds.
	timeouts := faultTimeouts(t, "ownership/zombie-worker")
	require.Equal(t, map[string]int64{"session.timeout.ms": 6000, "heartbeat.interval.ms": 1000, "socket.timeout.ms": 8000}, zombieWorkerConnection(timeouts))
	// When constructing the stimulus, then bytes upstream are dropped without
	// an immediate close and the RPC straddles session expiry within save10s.
	fault, err := zombieCoordinatorFault(timeouts)
	require.NoError(t, err)
	require.Equal(t, faults.Fault{Kind: "timeout", Stream: "upstream", Value: 0}, fault)
	require.Greater(t, timeouts["socket.timeout.ms"], timeouts["session.timeout.ms"])
	require.Less(t, timeouts["socket.timeout.ms"], timeouts["progress_save_timeout"])
	recovery := time.Duration(0)
	for _, timeout := range timeouts {
		recovery += timeout
	}
	require.Equal(t, 31*time.Second, recovery)
	// The statement and config actually released to both CLIs must use the
	// catalog bounds, not merely declare them in the independent receipt.
	for _, worker := range []int{1, 2} {
		statement, err := sql.NewParser().Parse(zombieWorkerSQL(timeouts, worker, "worker", "src", "dst", "group"))
		require.NoError(t, err)
		planned, err := plan.NewPlanner(nil).Build(statement, plan.Profiles{})
		require.NoError(t, err)
		require.Equal(t, "6000", planned.Source.Properties.Get("session.timeout.ms"))
		require.Equal(t, "1000", planned.Source.Properties.Get("heartbeat.interval.ms"))
		require.Equal(t, "8000", planned.Source.Properties.Get("socket.timeout.ms"))
	}
	var configured config.Config
	require.NoError(t, toml.Unmarshal([]byte("[worker]\n"+zombieWorkerSettings(timeouts)), &configured))
	require.Equal(t, 2*time.Second, configured.Worker.DeliveryTimeout.Duration)
	require.Equal(t, 5*time.Second, configured.Worker.ShutdownTimeout.Duration)
	require.Equal(t, 1024, configured.Worker.RecordCap)
	require.EqualValues(t, 134217728, configured.Worker.ByteCap)
	// A short RPC or a separately extended save must not silently redefine the
	// required live exposure or evade the product's existing fail-fast bound.
	for name, value := range map[string]time.Duration{"socket.timeout.ms": 3 * time.Second, "progress_save_timeout": 20 * time.Second} {
		changed := maps.Clone(timeouts)
		changed[name] = value
		_, err := zombieCoordinatorFault(changed)
		require.Error(t, err)
	}
}
