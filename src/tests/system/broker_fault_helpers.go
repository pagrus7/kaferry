//go:build integration

package compat

import (
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
