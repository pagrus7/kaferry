package file

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
)

func TestCheckpointDirectoryMustAlreadyExist(t *testing.T) {
	for _, scenario := range []string{"missing", "not a directory", "existing"} {
		t.Run(scenario, func(t *testing.T) {
			// Given an existing directory, a missing path, or a regular file.
			directory := t.TempDir()
			if scenario != "existing" {
				directory = filepath.Join(directory, "checkpoints")
			}
			if scenario == "not a directory" {
				require.NoError(t, os.WriteFile(directory, []byte("keep"), 0600))
			}

			// When a file-source group opens its checkpoint store.
			store, err := NewCheckpoints(directory, "restore", DurableWriter{})

			// Then only an existing directory permits ownership; other paths stay untouched.
			if scenario == "existing" {
				require.NoError(t, err)
				defer store.Close()
				if second, err := NewCheckpoints(directory, "restore", DurableWriter{}); err == nil {
					second.Close()
					t.Fatal("second owner accepted")
				}
				return
			}
			var stateConflict *Conflict
			if store != nil || !errors.As(err, &stateConflict) || !strings.Contains(err.Error(), "worker.checkpoint_dir") {
				t.Fatal("missing actionable conflict", err)
			}
			if scenario == "missing" {
				if _, err := os.Stat(directory); !os.IsNotExist(err) {
					t.Fatal("missing checkpoint directory was created", err)
				}
			} else if data, err := os.ReadFile(directory); err != nil || string(data) != "keep" {
				t.Fatal("non-directory path changed", err)
			}
		})
	}
}

func TestSourceFactoryRequiresCheckpointDirectoryBeforeOpeningArchive(t *testing.T) {
	// Given a fresh default directory and a missing archive.
	home := t.TempDir()
	limits := config.Defaults(home).Worker
	p := plan.Plan{Source: plan.Endpoint{Kind: "FILE", Name: filepath.Join(home, "missing.cbor")}, Destination: plan.Endpoint{Kind: "TOPIC"}, Group: "restore"}
	factory := NewFactory(core.RealClock{}, logger(), DurableWriter{}, NewPublisher())

	// When a file-source job starts without provisioning its checkpoint directory.
	source, err := factory.OpenSource(p, "fp", limits)

	// Then startup requests an existing progress directory before reading or creating state.
	var stateConflict *Conflict
	if source != nil || !errors.As(err, &stateConflict) || !strings.Contains(err.Error(), "to resume") {
		t.Fatal("directory gate did not fail first", err)
	}
	if _, err := os.Stat(*limits.CheckpointDir); !os.IsNotExist(err) {
		t.Fatal("startup created a fresh checkpoint directory", err)
	}
}

func TestSourceFactoryResolvesDefaultCheckpointDirectoryLazily(t *testing.T) {
	for _, available := range []bool{false, true} {
		t.Run(map[bool]string{false: "no home", true: "home available"}[available], func(t *testing.T) {
			// Given a file-source job with an omitted checkpoint directory.
			home := ""
			if available {
				home = t.TempDir()
			}
			t.Setenv("HOME", home)
			limits := config.Defaults("").Worker
			p := plan.Plan{Source: plan.Endpoint{Kind: "FILE", Name: filepath.Join(t.TempDir(), "missing.cbor")}, Group: "restore"}
			factory := NewFactory(core.RealClock{}, logger(), DurableWriter{}, NewPublisher())

			// When opening the file source without provisioning checkpoint storage.
			source, err := factory.OpenSource(p, "fp", limits)

			// Then startup reports the unresolved default or the actual default path.
			var stateConflict *Conflict
			if source != nil || !errors.As(err, &stateConflict) || !strings.Contains(err.Error(), "worker.checkpoint_dir") {
				t.Fatal("missing actionable checkpoint diagnostic", err)
			}
			if available {
				if !strings.Contains(err.Error(), filepath.Join(home, ".kaferry", "checkpoints")) {
					t.Fatal("default directory resolved incorrectly", err)
				}
			} else if !strings.Contains(err.Error(), "set HOME") {
				t.Fatal("missing home guidance", err)
			}
		})
	}
}
