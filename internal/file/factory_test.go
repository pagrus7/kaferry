package file

import (
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
	checkpointDir := t.TempDir()
	limits.CheckpointDir = &checkpointDir
	p := plan.Plan{Source: plan.Endpoint{Kind: "FILE", Name: filepath.Join(dir, "missing.cbor")}, Destination: plan.Endpoint{Kind: "TOPIC"}, Group: "group"}
	factory := NewFactory(core.RealClock{}, logger(), DurableWriter{}, NewPublisher())

	// When
	_, openSourceErr := factory.OpenSource(p, "fp", limits)

	// Then
	if openSourceErr == nil {
		t.Fatal("missing source was accepted")
	}
	checkpoints, err := NewCheckpoints(*limits.CheckpointDir, p.Group, DurableWriter{})
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
	checkpointDir := filepath.Join(dir, "checkpoints")
	limits.CheckpointDir = &checkpointDir
	p := plan.Plan{Source: plan.Endpoint{Kind: "FILE", Name: source}, Destination: plan.Endpoint{Kind: "FILE", Name: destination}, Group: "group"}
	factory := NewFactory(core.RealClock{}, logger(), DurableWriter{}, NewPublisher())

	// When
	_, openSourceErr := factory.OpenSource(p, "fp", limits)

	// Then
	if openSourceErr == nil {
		t.Fatal("source/destination inode alias was accepted")
	}
	if _, err := os.Stat(*limits.CheckpointDir); !os.IsNotExist(err) {
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

func TestConfiguredDirectoryResumesProgressAndKeepsItsLock(t *testing.T) {
	// Given an archive and checkpoint retained in a configured directory.
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
	checkpointDir := t.TempDir()
	checkpoints, err := NewCheckpoints(checkpointDir, "restore", DurableWriter{})
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
	cfg, err := config.Decode(strings.NewReader("[worker]\ncheckpoint_dir='"+checkpointDir+"'\n"), homeDir)
	if err != nil {
		t.Fatal(err)
	}
	limits := cfg.Worker
	p := plan.Plan{Source: plan.Endpoint{Kind: "FILE", Name: archive}, Destination: plan.Endpoint{Kind: "TOPIC"}, Group: "restore"}
	factory := NewFactory(core.RealClock{}, logger(), DurableWriter{}, NewPublisher())

	// When a worker uses the explicitly configured checkpoint directory.
	source, err := factory.OpenSource(p, "fp", limits)

	// Then it retains saved ownership and excludes a second reader of that group.
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if !source.Progress().Exists {
		t.Fatal("configured checkpoint was not resumed")
	}
	secondRecord, err := source.Poll(0)
	if err != nil || secondRecord.Input == nil || string(secondRecord.Input.Record.Value) != "second" {
		t.Fatal("configured resume replayed or lost records", secondRecord, err)
	}
	if second, err := factory.OpenSource(p, "fp", limits); err == nil {
		second.Close()
		t.Fatal("configured checkpoint lock was bypassed")
	}
}
