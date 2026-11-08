//go:build integration && load

package compat

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
)

func soakTopic(t *testing.T, admin *kadm.Client, partitions int, replicated bool) string {
	t.Helper()
	if !replicated {
		return topic(t, admin, partitions)
	}
	name := configuredTopic(t, admin, partitions, 3, map[string]*string{"min.insync.replicas": kadm.StringPtr("2")})
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		metadata, err := admin.ListTopics(ctx, name)
		if err == nil && fullyReplicatedTopic(metadata[name], partitions) {
			configs, err := admin.DescribeTopicConfigs(ctx, name)
			if err == nil && len(configs) == 1 && configs[0].Err == nil {
				for _, config := range configs[0].Configs {
					if config.Key == "min.insync.replicas" && config.Value != nil {
						require.Equal(t, "2", *config.Value, "replicated endurance requires min ISR=2")
						return name
					}
				}
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("replicated endurance topic %s did not reach RF=3/ISR=3/min ISR=2: %v %+v", name, err, metadata)
		case <-tick.C:
		}
	}
}

func fullyReplicatedTopic(topic kadm.TopicDetail, partitions int) bool {
	if topic.Err != nil || len(topic.Partitions) != partitions {
		return false
	}
	for partition := range partitions {
		part, exists := topic.Partitions[int32(partition)]
		if !exists || part.Err != nil || part.Leader < 0 || len(part.Replicas) != 3 || len(part.ISR) != 3 || len(part.OfflineReplicas) != 0 {
			return false
		}
		inSync := make(map[int32]bool, 3)
		for _, replica := range part.ISR {
			inSync[replica] = true
		}
		if len(inSync) != 3 || !inSync[part.Leader] {
			return false
		}
		for _, replica := range part.Replicas {
			if !inSync[replica] {
				return false
			}
			delete(inSync, replica)
		}
	}
	return true
}
