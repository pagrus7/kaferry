//go:build integration

package compat

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type ownedBroker struct {
	name       string
	run        func(...string) ([]byte, error)
	restore    string
	cleanupSet bool
}

func fixtureProject(fallback string) string {
	if project := os.Getenv("KAFERRY_FIXTURE_PROJECT"); project != "" {
		return project
	}
	return fallback
}

func ownBroker(t *testing.T, name, project, service string) *ownedBroker {
	t.Helper()
	broker, err := inspectBroker(name, project, service, func(args ...string) ([]byte, error) {
		return exec.Command("docker", args...).CombinedOutput()
	})
	require.NoError(t, err)
	return broker
}

func inspectBroker(name, project, service string, run func(...string) ([]byte, error)) (*ownedBroker, error) {
	if name == "" {
		return nil, fmt.Errorf("fault requires an owned broker container")
	}
	format := `{{index .Config.Labels "com.docker.compose.project"}}`
	want := project
	if service != "" {
		format += ` {{index .Config.Labels "com.docker.compose.service"}}`
		want += " " + service
	}
	data, err := run("inspect", "--format", format, name)
	if err != nil || strings.TrimSpace(string(data)) != want {
		return nil, fmt.Errorf("fault target %s must belong to disposable fixture %s: %v", name, want, err)
	}
	return &ownedBroker{name: name, run: run}, nil
}

func (b *ownedBroker) pause(t *testing.T) error { return b.interrupt(t, "unpause", "pause") }
func (b *ownedBroker) stop(t *testing.T) error  { return b.interrupt(t, "start", "stop", "-t", "1") }
func (b *ownedBroker) paused() bool             { return b != nil && b.restore == "unpause" }

func (b *ownedBroker) interrupt(t *testing.T, restore string, args ...string) error {
	data, err := b.run(append(args, b.name)...)
	if err != nil {
		return fmt.Errorf("broker %s: %w: %s", args[0], err, data)
	}
	b.restore = restore
	if !b.cleanupSet {
		b.cleanupSet = true
		t.Cleanup(func() {
			if err := b.recover(); err != nil {
				t.Errorf("restore owned broker: %v", err)
			}
		})
	}
	return nil
}

func (b *ownedBroker) recover() error {
	if b.restore == "" {
		return nil
	}
	data, err := b.run(b.restore, b.name)
	if err != nil {
		return fmt.Errorf("broker %s: %w: %s", b.restore, err, data)
	}
	b.restore = ""
	return nil
}

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
