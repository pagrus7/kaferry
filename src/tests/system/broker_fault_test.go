//go:build integration && harness

package compat

import (
	"errors"
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
