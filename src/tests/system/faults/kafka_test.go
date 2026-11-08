package faults

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestMetadataAdapterUsesSelectedClusterAndRejectsAbsentState(t *testing.T) {
	// Given independent in-process protocol fixtures with different partitions.
	metadata := make(KafkaMetadata)
	for side, count := range map[string]int32{"source": 2, "destination": 5} {
		cluster := kfake.MustCluster(kfake.NumBrokers(3), kfake.SeedTopics(count, "records"))
		t.Cleanup(cluster.Close)
		client, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
		require.NoError(t, err)
		metadata[side] = kadm.NewClient(client)
	}
	t.Cleanup(metadata.Close)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	// When asking for partition 4, then only the destination contains it.
	_, err := metadata.Partition(ctx, "source", "records", 4)
	require.ErrorContains(t, err, "partition metadata is unavailable")
	part, err := metadata.Partition(ctx, "destination", "records", 4)
	require.NoError(t, err)
	require.NotEmpty(t, part.ISR)
	_, err = metadata.Coordinator(ctx, "source", "copy")
	require.NoError(t, err)
	_, err = metadata.Partition(ctx, "unknown", "records", 0)
	require.ErrorContains(t, err, "not configured")
	_, err = metadata.Coordinator(ctx, "unknown", "copy")
	require.ErrorContains(t, err, "not configured")
	cancelled, done := context.WithCancel(ctx)
	done()
	_, err = metadata.Partition(cancelled, "source", "records", 0)
	require.ErrorIs(t, err, context.Canceled)
}
