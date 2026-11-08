//go:build integration

package compat

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOwnedBrokerRequiresFixtureOwnership(t *testing.T) {
	// Given labels from an inspected container, including foreign projects/services.
	for _, tc := range []struct {
		name, container, labels, service string
		err                              error
		valid                            bool
	}{
		{name: "project", container: "broker", labels: "assigned\n", valid: true},
		{name: "service", container: "broker", labels: "assigned kafka-dr\n", service: "kafka-dr", valid: true},
		{name: "foreign project", container: "broker", labels: "foreign"},
		{name: "wrong service", container: "broker", labels: "assigned kafka", service: "kafka-dr"},
		{name: "inspect failure", container: "broker", labels: "assigned", err: errors.New("inspect failed")},
		{name: "missing container"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// When claiming it, then only a matching disposable fixture is accepted.
			broker, err := inspectBroker(tc.container, "assigned", tc.service, func(args ...string) ([]byte, error) {
				require.Equal(t, "inspect", args[0])
				return []byte(tc.labels), tc.err
			})
			if tc.valid {
				require.NoError(t, err)
				require.NotNil(t, broker)
			} else {
				require.Error(t, err)
				require.Nil(t, broker)
			}
		})
	}
}

func TestOwnedBrokerRestoresAfterEarlyReturnAndRetriesFailure(t *testing.T) {
	for _, stopped := range []bool{false, true} {
		t.Run(fmt.Sprintf("stop=%t", stopped), func(t *testing.T) {
			// Given a broker whose first recovery attempt fails.
			var commands []string
			var observations []string
			failure := errors.New("temporary restore failure")
			restore := "unpause"
			interrupt := "pause broker"
			if stopped {
				restore, interrupt = "start", "stop -t 1 broker"
			}
			failed := false
			t.Run("scenario", func(t *testing.T) {
				broker, err := inspectBroker("broker", "assigned", "", func(args ...string) ([]byte, error) {
					if args[0] == "inspect" {
						return []byte("assigned"), nil
					}
					commands = append(commands, strings.Join(args, " "))
					if args[0] == restore && !failed {
						failed = true
						return nil, failure
					}
					return nil, nil
				})
				require.NoError(t, err)
				broker.observe = func(operation string, started, finished time.Time, err error) {
					require.False(t, started.IsZero())
					if finished.IsZero() {
						observations = append(observations, operation+":unknown")
						return
					}
					require.False(t, finished.Before(started))
					observations = append(observations, fmt.Sprintf("%s:%t", operation, err == nil))
				}
				apply := broker.pause
				if stopped {
					apply = broker.stop
				}
				// When explicit recovery is retried, then it is safe to repeat after success.
				require.NoError(t, apply(t))
				require.ErrorIs(t, broker.recover(), failure)
				require.NoError(t, broker.recover())
				require.NoError(t, broker.recover())
				require.NoError(t, apply(t))
				// Return with the broker interrupted; registered cleanup must restore it.
			})
			require.Equal(t, []string{interrupt, restore + " broker", restore + " broker", interrupt, restore + " broker"}, commands)
			action := strings.Fields(interrupt)[0]
			require.Equal(t, []string{action + ":unknown", action + ":true", restore + ":unknown", restore + ":false",
				restore + ":unknown", restore + ":true", action + ":unknown", action + ":true", restore + ":unknown", restore + ":true"}, observations)
		})
	}
}

func TestOwnedBrokerKeepsCleanupCorrectWhenObservationAborts(t *testing.T) {
	// Given command observations that abort after a successful Docker operation.
	var commands []string
	t.Run("scenario", func(t *testing.T) {
		broker, err := inspectBroker("broker", "assigned", "", func(args ...string) ([]byte, error) {
			if args[0] == "inspect" {
				return []byte("assigned"), nil
			}
			commands = append(commands, strings.Join(args, " "))
			return nil, nil
		})
		require.NoError(t, err)
		broker.observe = func(_ string, _, finished time.Time, _ error) {
			if !finished.IsZero() {
				panic("observation failed")
			}
		}
		// When observing pause and unpause aborts, then cleanup still knows their results.
		require.Panics(t, func() { _ = broker.pause(t) })
		require.True(t, broker.paused())
		require.Panics(t, func() { _ = broker.recover() })
		require.False(t, broker.paused())
	})
	// Then cleanup neither strands a paused broker nor repeats successful unpause.
	require.Equal(t, []string{"pause broker", "unpause broker"}, commands)
}
