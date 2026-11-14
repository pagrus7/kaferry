//go:build integration && load && harness

package compat

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
)

func TestReplicatedEnduranceRequiresEveryPartitionFullyInSync(t *testing.T) {
	// Given metadata for replicated topics, including incomplete setup states.
	for _, test := range []struct {
		name   string
		change func(*kadm.TopicDetail)
		ready  bool
	}{
		{name: "ready", ready: true},
		{name: "missing partition", change: func(topic *kadm.TopicDetail) { delete(topic.Partitions, 1) }},
		{name: "unknown topic", change: func(topic *kadm.TopicDetail) { topic.Err = errors.New("unknown topic") }},
		{name: "leader unavailable", change: func(topic *kadm.TopicDetail) {
			part := topic.Partitions[1]
			part.Leader = -1
			topic.Partitions[1] = part
		}},
		{name: "ISR below replication factor", change: func(topic *kadm.TopicDetail) {
			part := topic.Partitions[1]
			part.ISR = []int32{1, 2}
			topic.Partitions[1] = part
		}},
		{name: "wrong replicas", change: func(topic *kadm.TopicDetail) {
			part := topic.Partitions[1]
			part.Replicas = []int32{1}
			topic.Partitions[1] = part
		}},
		{name: "offline replica", change: func(topic *kadm.TopicDetail) {
			part := topic.Partitions[1]
			part.OfflineReplicas = []int32{3}
			topic.Partitions[1] = part
		}},
		{name: "wrong ISR identities", change: func(topic *kadm.TopicDetail) {
			part := topic.Partitions[1]
			part.ISR = []int32{1, 2, 4}
			topic.Partitions[1] = part
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			topic := kadm.TopicDetail{Partitions: make(kadm.PartitionDetails)}
			for partition := range int32(2) {
				topic.Partitions[partition] = kadm.PartitionDetail{Partition: partition, Leader: 1,
					Replicas: []int32{1, 2, 3}, ISR: []int32{1, 2, 3}}
			}
			if test.change != nil {
				test.change(&topic)
			}
			// When deciding readiness, then every partition must satisfy RF=3/ISR=3.
			require.Equal(t, test.ready, fullyReplicatedTopic(topic, 2))
		})
	}
}

func TestReplicatedEnduranceStallsAndRestoresEveryDestinationBroker(t *testing.T) {
	// Given three verified destination brokers and the existing stall schedule.
	var commands []string
	t.Run("stream", func(t *testing.T) {
		stalls := destinationStalls{next: time.Now()}
		for node := 1; node <= 3; node++ {
			name := fmt.Sprintf("destination-%d", node)
			broker, err := inspectBroker(name, "assigned", name, func(args ...string) ([]byte, error) {
				if args[0] == "inspect" {
					return []byte("assigned " + name), nil
				}
				commands = append(commands, strings.Join(args, " "))
				return nil, nil
			})
			require.NoError(t, err)
			stalls.brokers = append(stalls.brokers, broker)
		}
		stalls.step(t, stalls.next.Add(-time.Nanosecond), true)
		require.Empty(t, commands, "do not stall before the scheduled time")
		// When the scheduled stall starts, then all destination brokers are paused.
		started := time.Now()
		stalls.step(t, stalls.next, true)
		require.Equal(t, []string{"pause destination-1", "pause destination-2", "pause destination-3"}, commands)
		require.True(t, stalls.paused())
		require.Equal(t, 1, stalls.count, "one cluster stall counts once")
		require.False(t, stalls.until.Before(started.Add(time.Second)), "retain the one-second hold")
		stalls.step(t, stalls.until.Add(-time.Nanosecond), false)
		require.Len(t, commands, 3, "do not heal before the one-second hold")
		// Then healing restores every broker, even after publishing ends.
		stalls.step(t, stalls.until, false)
		require.False(t, stalls.paused())
		require.Equal(t, []string{"unpause destination-1", "unpause destination-2", "unpause destination-3"}, commands[3:])
		stalls.step(t, stalls.next, false)
		require.Len(t, commands, 6, "do not start another stall after publishing")
		stalls.next = time.Now()
		stalls.step(t, stalls.next, true)
		require.Equal(t, 2, stalls.count)
		// Registered cleanup must restore all three after an early return.
	})
	require.Equal(t, []string{"pause destination-1", "pause destination-2", "pause destination-3",
		"unpause destination-3", "unpause destination-2", "unpause destination-1"}, commands[6:])
}
