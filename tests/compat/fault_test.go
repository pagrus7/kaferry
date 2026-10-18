//go:build integration && fault

package compat

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	core "github.com/pagrus7/kaferry/internal/copy"
	adapter "github.com/pagrus7/kaferry/internal/franz"
)

func ownedContainer(t *testing.T) string {
	t.Helper()
	name := os.Getenv("KAFKA_FAULT_CONTAINER")
	if name == "" {
		t.Skip("set KAFKA_FAULT_CONTAINER for broker pause tests")
	}
	data, err := exec.Command("docker", "inspect", "--format", "{{index .Config.Labels \"com.docker.compose.project\"}}", name).Output()
	if err != nil || strings.TrimSpace(string(data)) != "kaferry-compat" {
		t.Fatal("fault tests require the project's disposable compose broker")
	}
	return name
}
func TestBlockingCommitAcrossBrokerOutage(t *testing.T) {
	// Given an assigned production source with confirmed initial progress.
	container := ownedContainer(t)
	admin := admin(t)
	name := topic(t, admin, 1)
	source, store := probeSourceProgress(t, name, name+"-fault")
	ownership := &probeOwnership{owned: make(map[int32]bool)}
	if err := source.Start(ownership, nil); err != nil {
		t.Fatal(err)
	}
	pollOwnership(t, []*adapter.Source{source}, func() bool { return len(ownership.owned) == 1 })
	if data, err := exec.Command("docker", "pause", container).CombinedOutput(); err != nil {
		t.Fatalf("pause: %v %s", err, data)
	}
	paused := true
	defer func() {
		if paused {
			_ = exec.Command("docker", "unpause", container).Run()
		}
	}()
	deadline, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	defer cancel()
	// When the member commits while the coordinator is unavailable.
	started := time.Now()
	_, err := store.Save(deadline, []core.Cursor{{Partition: 0, Position: 0}})
	// Then the actual adapter returns its deadline without waiting for recovery.
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > 2*time.Second {
		t.Fatal("member commit ignored deadline", err, time.Since(started))
	}
	if data, err := exec.Command("docker", "unpause", container).CombinedOutput(); err != nil {
		t.Fatalf("unpause: %v %s", err, data)
	}
	paused = false
	// When the coordinator recovers, then the same owned position can be saved.
	recovered, stop := context.WithTimeout(t.Context(), 10*time.Second)
	defer stop()
	results, err := store.Save(recovered, []core.Cursor{{Partition: 0, Position: 0}})
	failure, present := results[0]
	if err != nil || !present || failure != nil {
		t.Fatal("member commit did not recover", results, err)
	}
}
