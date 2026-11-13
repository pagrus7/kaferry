//go:build integration && harness

package compat

import (
	"errors"
	"fmt"
	"strings"
	"testing"

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

		})
	}
}
