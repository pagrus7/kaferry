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
)

func TestSourceFactoryReleasesCheckpointLockOnOpenFailure(t *testing.T) {
	// Given
	dir := t.TempDir()
	limits := config.Defaults(dir).Worker
	limits.CheckpointDir = filepath.Join(dir, "checkpoints")
	p := plan.Plan{Source: plan.Endpoint{Kind: "FILE", Name: filepath.Join(dir, "missing.cbor")}, Destination: plan.Endpoint{Kind: "TOPIC"}, Group: "group"}
	factory := NewFactory(core.RealClock{}, logger(), DurableWriter{}, NewPublisher())

	// When
	_, openSourceErr := factory.OpenSource(p, "fp", limits)

	// Then
	if openSourceErr == nil {
		t.Fatal("missing source was accepted")
	}
	checkpoints, err := NewCheckpoints(limits.CheckpointDir, p.Group, DurableWriter{})
	if err != nil {
		t.Fatal("failed source open leaked checkpoint lock", err)
	}
	checkpoints.Close()
}

func TestSourceFactoryRejectsFileAliasesBeforeCheckpointCreation(t *testing.T) {
	// Given
	dir := t.TempDir()
	source, destination := filepath.Join(dir, "source"), filepath.Join(dir, "alias")
	if err := os.WriteFile(source, []byte("source"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(source, destination); err != nil {
		t.Fatal(err)
	}
	limits := config.Defaults(dir).Worker
	limits.CheckpointDir = filepath.Join(dir, "checkpoints")
	p := plan.Plan{Source: plan.Endpoint{Kind: "FILE", Name: source}, Destination: plan.Endpoint{Kind: "FILE", Name: destination}, Group: "group"}
	factory := NewFactory(core.RealClock{}, logger(), DurableWriter{}, NewPublisher())

	// When
	_, openSourceErr := factory.OpenSource(p, "fp", limits)

	// Then
	if openSourceErr == nil {
		t.Fatal("source/destination inode alias was accepted")
	}
	if _, err := os.Stat(limits.CheckpointDir); !os.IsNotExist(err) {
		t.Fatal("self-copy created checkpoint state", err)
	}
}

func TestOutputFactoryRejectsMissingIdentityBeforeCreatingFile(t *testing.T) {
	// Given
	dir := t.TempDir()
	p := plan.Plan{Destination: plan.Endpoint{Name: filepath.Join(dir, "output")}, Group: "group"}
	factory := NewFactory(core.RealClock{}, logger(), DurableWriter{}, NewPublisher())

	// When
	_, openOutputErr := factory.OpenOutput(p, "fp", DestinationProgress{Exists: true}, config.Defaults(dir).Worker)

	// Then
	if openOutputErr == nil {
		t.Fatal("existing progress without file identity was accepted")
	}
	if _, err := os.Stat(p.Destination.Name); !os.IsNotExist(err) {
		t.Fatal("invalid progress created output", err)
	}
}

func TestFileUpgradeGuard(t *testing.T) {
	for _, tc := range []struct {
		name, oldSuffix, oldGroup, destination string
		custom, existing, trailingSlash        bool
		blocked                                bool
	}{
		{name: "saved checkpoint", oldSuffix: ".json", blocked: true},
		{name: "stable lock", oldSuffix: ".json.lock", blocked: true},
		{name: "normalized default", oldSuffix: ".json", trailingSlash: true, blocked: true},
		{name: "file output", oldSuffix: ".json", destination: "FILE", blocked: true},
		{name: "different group", oldSuffix: ".json", oldGroup: "other"},
		{name: "custom directory", oldSuffix: ".json", custom: true},
		{name: "existing new progress", oldSuffix: ".json", existing: true},
		{name: "fresh group"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given a temporary home and optional state from before the rename.
			homeDir := t.TempDir()
			limits := config.Defaults(homeDir).Worker
			legacyDir := filepath.Join(homeDir, ".kafka-fwd2", "checkpoints")
			group := "restore"
			if tc.oldSuffix != "" {
				if err := os.MkdirAll(legacyDir, 0700); err != nil {
					t.Fatal(err)
				}
				oldGroup := group
				if tc.oldGroup != "" {
					oldGroup = tc.oldGroup
				}
				name := strings.TrimSuffix(checkpointPath(legacyDir, oldGroup), ".json") + tc.oldSuffix
				if err := os.WriteFile(name, []byte("old state"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if tc.custom {
				limits.CheckpointDir = filepath.Join(homeDir, "custom")
			}
			if tc.trailingSlash {
				limits.CheckpointDir += string(filepath.Separator)
			}
			if tc.existing {
				if err := os.MkdirAll(limits.CheckpointDir, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(checkpointPath(limits.CheckpointDir, group), []byte("new state"), 0600); err != nil {
					t.Fatal(err)
				}
			}

			// When checking the default location before creating new progress.
			err := rejectLegacyCheckpoint(limits.CheckpointDir, limits.LegacyCheckpointDir(), group)

			// Then only an unmatched legacy checkpoint or lock requires operator action.
			var stateConflict *Conflict
			if errors.As(err, &stateConflict) != tc.blocked || (!tc.blocked && err != nil) {
				t.Fatal("incorrect upgrade decision", err)
			}
			if !tc.blocked {
				return
			}
			p := plan.Plan{Source: plan.Endpoint{Kind: "FILE", Name: filepath.Join(homeDir, "missing.cbor")}, Destination: plan.Endpoint{Kind: "TOPIC"}, Group: group}
			if tc.destination != "" {
				p.Destination.Kind = tc.destination
			}
			factory := NewFactory(core.RealClock{}, logger(), DurableWriter{}, NewPublisher())
			// When opening the source through its component boundary.
			_, err = factory.OpenSource(p, "fp", limits)
			// Then the guard fails before opening the archive or creating checkpoint state.
			if !errors.As(err, &stateConflict) || !strings.Contains(err.Error(), legacyDir) || !strings.Contains(err.Error(), "worker.checkpoint_dir") {
				t.Fatal("missing actionable upgrade conflict", err)
			}
			if _, err := os.Stat(limits.CheckpointDir); !os.IsNotExist(err) {
				t.Fatal("upgrade failure created new checkpoint state", err)
			}
		})
	}
}

func TestExplicitLegacyDirectoryResumesProgressAndKeepsItsLock(t *testing.T) {
	// Given an archive and checkpoint retained in the old directory.
	homeDir := t.TempDir()
	archive := filepath.Join(homeDir, "input.cbor")
	output, err := NewOutput(NewPublisher(), archive, "archive", "archive-fp", "", 0, codec(t), 10, logger())
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"first", "second"} {
		if err := output.Submit(core.Record{Value: []byte(value), TimestampType: "unknown"}, core.Token{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := output.Flush(); err != nil {
		t.Fatal(err)
	}
	output.Close()
	legacyDir := filepath.Join(homeDir, ".kafka-fwd2", "checkpoints")
	checkpoints, err := NewCheckpoints(legacyDir, "restore", DurableWriter{})
	if err != nil {
		t.Fatal(err)
	}
	oldSource, err := NewSource(archive, "fp", plan.WindowSpec{}, codec(t), checkpoints, core.RealClock{})
	if err != nil {
		t.Fatal(err)
	}
	first, err := oldSource.Poll(0)
	if err != nil || first.Input == nil || string(first.Input.Record.Value) != "first" {
		t.Fatal("cannot establish original progress", first, err)
	}
	if _, err := checkpoints.Save(t.Context(), []core.Cursor{{Partition: 0, Position: first.Input.Next}}); err != nil {
		t.Fatal(err)
	}
	oldSource.Close()
	checkpoints.Close()
	cfg, err := config.Decode(strings.NewReader("[worker]\ncheckpoint_dir='"+legacyDir+"'\n"), homeDir)
	if err != nil {
		t.Fatal(err)
	}
	limits := cfg.Worker
	p := plan.Plan{Source: plan.Endpoint{Kind: "FILE", Name: archive}, Destination: plan.Endpoint{Kind: "TOPIC"}, Group: "restore"}
	factory := NewFactory(core.RealClock{}, logger(), DurableWriter{}, NewPublisher())

	// When the newer build uses the explicitly configured legacy directory.
	source, err := factory.OpenSource(p, "fp", limits)

	// Then it retains saved ownership and excludes a second reader of that group.
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if !source.Progress().Exists {
		t.Fatal("legacy checkpoint was not resumed")
	}
	secondRecord, err := source.Poll(0)
	if err != nil || secondRecord.Input == nil || string(secondRecord.Input.Record.Value) != "second" {
		t.Fatal("legacy resume replayed or lost records", secondRecord, err)
	}
	if second, err := factory.OpenSource(p, "fp", limits); err == nil {
		second.Close()
		t.Fatal("legacy checkpoint lock was bypassed")
	}
}
