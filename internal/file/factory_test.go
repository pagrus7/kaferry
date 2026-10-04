package file

import (
	"os"
	"path/filepath"
	"testing"

	"kafka-fwd2/internal/config"
	core "kafka-fwd2/internal/copy"
	"kafka-fwd2/internal/plan"
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
