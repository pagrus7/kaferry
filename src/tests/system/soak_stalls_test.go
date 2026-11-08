//go:build integration && load

package compat

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStallTraceRetainsFailedCommandWithoutClaimingPausedState(t *testing.T) {
	// Given an owned fixture whose Docker pause command fails.
	directory := t.TempDir()
	script := "#!/bin/sh\ncase \"$1\" in\ninspect) printf '%s\\n' 'assigned kafka-dr';;\npause) exit 5;;\n*) exit 9;;\nesac\n"
	require.NoError(t, os.WriteFile(filepath.Join(directory, "docker"), []byte(script), 0700))
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Run("record", func(t *testing.T) {
		t.Setenv("KAFERRY_REPORT_DIR", directory)
		broker := ownStallBroker(t, "fixture", "assigned", "kafka-dr", stallTrace(t))
		// When recording the existing command, then failure does not establish state.
		require.Error(t, broker.pause(t))
		require.False(t, broker.paused())
	})
	data, err := os.ReadFile(filepath.Join(directory, "soak.stalls.jsonl"))
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	require.Len(t, lines, 4)
	for index, line := range lines {
		var event stallObservation
		require.NoError(t, json.Unmarshal([]byte(line), &event))
		require.Equal(t, "kafka-dr", event.Role)
		require.False(t, event.Started.IsZero())
		if index%2 == 0 {
			require.Nil(t, event.Finished)
			require.Equal(t, "unknown", event.Result)
		} else {
			require.NotNil(t, event.Finished)
			require.False(t, event.Finished.Before(event.Started))
		}
		require.Nil(t, event.Paused, "a command result is not a state query")
		require.Nil(t, event.NotBefore)
		if index < 2 {
			require.Equal(t, "inspect ownership", event.Operation)
		} else {
			require.Equal(t, "pause", event.Operation)
		}
		if index == 1 {
			require.Equal(t, "completed", event.Result)
		}
		if index == 3 {
			require.Equal(t, "failed", event.Result)
		}
	}
}
