//go:build integration && fault

package compat

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	core "github.com/pagrus7/kaferry/src/internal/copy"
	adapter "github.com/pagrus7/kaferry/src/internal/franz"
	"github.com/stretchr/testify/require"
)

func TestBlockingCommitAcrossBrokerOutage(t *testing.T) {
	// Given an assigned production source with confirmed initial progress.
	name := os.Getenv("KAFKA_FAULT_CONTAINER")
	if name == "" {
		t.Skip("set KAFKA_FAULT_CONTAINER for broker pause tests")
	}
	container := ownBroker(t, name, fixtureProject("kaferry-compat"), "")
	admin := admin(t)
	name = topic(t, admin, 1)
	source, store := probeSourceProgress(t, name, name+"-fault")
	ownership := &probeOwnership{owned: make(map[int32]bool)}
	require.NoError(t, source.Start(ownership, nil))
	pollOwnership(t, []*adapter.Source{source}, func() bool { return len(ownership.owned) == 1 })
	require.NoError(t, container.pause(t))
	deadline, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	defer cancel()
	// When the member commits while the coordinator is unavailable.
	started := time.Now()
	_, err := store.Save(deadline, []core.Cursor{{Partition: 0, Position: 0}})
	// Then the actual adapter returns its deadline without waiting for recovery.
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > 2*time.Second {
		t.Fatal("member commit ignored deadline", err, time.Since(started))
	}
	require.NoError(t, container.recover())
	// When the coordinator recovers, then the same owned position can be saved.
	recovered, stop := context.WithTimeout(t.Context(), 10*time.Second)
	defer stop()
	results, err := store.Save(recovered, []core.Cursor{{Partition: 0, Position: 0}})
	failure, present := results[0]
	if err != nil || !present || failure != nil {
		t.Fatal("member commit did not recover", results, err)
	}
}
