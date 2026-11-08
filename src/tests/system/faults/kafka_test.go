package faults

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
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
	_, err = metadata.Coordinator(cancelled, "source", "copy")
	require.ErrorIs(t, err, context.Canceled)
}

func TestNativeEndpointsKeepAllWorkerPathsIndependent(t *testing.T) {
	// Given the published fixture ports, native clients must retain their own metadata path.
	require.Equal(t, []string{"127.0.0.1:46092", "127.0.0.1:46093", "127.0.0.1:46094"}, Endpoints("source", 0))
	require.Equal(t, []string{"127.0.0.1:47392", "127.0.0.1:47393", "127.0.0.1:47394"}, Endpoints("destination", 3))
	seen := make(map[string]bool)
	for _, cluster := range []string{"source", "destination"} {
		for worker := 0; worker <= 3; worker++ {
			for _, endpoint := range Endpoints(cluster, worker) {
				// Then no worker, observer or cluster can share another client's endpoint.
				require.False(t, seen[endpoint], endpoint)
				seen[endpoint] = true
			}
		}
	}
	require.Len(t, seen, 24)
}

func TestMetadataAdapterRejectsOmittedErroredAndUnavailableBrokerResponses(t *testing.T) {
	for _, mode := range []string{"omitted-topic", "topic-error", "partition-error", "no-response", "wrong-coordinator-key"} {
		t.Run(mode, func(t *testing.T) {
			// Given a broker response that cannot establish a current role.
			cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "records"))
			t.Cleanup(cluster.Close)
			client, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
			require.NoError(t, err)
			metadata := KafkaMetadata{"source": kadm.NewClient(client)}
			t.Cleanup(metadata.Close)
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			if mode == "wrong-coordinator-key" {
				cluster.ControlKey(int16(kmsg.FindCoordinator), func(request kmsg.Request) (kmsg.Response, error, bool) {
					response := kmsg.NewPtrFindCoordinatorResponse()
					response.Version = request.GetVersion()
					coordinator := kmsg.NewFindCoordinatorResponseCoordinator()
					coordinator.Key = "another-group"
					response.Coordinators = append(response.Coordinators, coordinator)
					return response, nil, true
				})
				_, err = metadata.Coordinator(ctx, "source", "copy")
				require.ErrorContains(t, err, "coordinator metadata is unavailable")
				return
			}
			cluster.ControlKey(int16(kmsg.Metadata), func(request kmsg.Request) (kmsg.Response, error, bool) {
				return failedMetadataResponse(cluster, mode, request), nil, true
			})
			// When resolving a partition, then return the failure instead of a usable broker ID.
			part, err := metadata.Partition(ctx, "source", "records", 0)
			require.Error(t, err)
			require.Empty(t, part.ISR)
			if mode == "no-response" {
				require.ErrorIs(t, err, context.DeadlineExceeded)
			}
		})
	}
}

func failedMetadataResponse(cluster *kfake.Cluster, mode string, request kmsg.Request) kmsg.Response {
	cluster.KeepControl()
	if mode == "no-response" {
		return nil
	}
	response := kmsg.NewPtrMetadataResponse()
	response.Version = request.GetVersion()
	if mode == "omitted-topic" {
		return response
	}
	topic := kmsg.NewMetadataResponseTopic()
	topic.Topic = kmsg.StringPtr("records")
	if mode == "topic-error" {
		topic.ErrorCode = kerr.UnknownTopicOrPartition.Code
	} else {
		part := kmsg.NewMetadataResponseTopicPartition()
		part.ErrorCode = kerr.NotLeaderForPartition.Code
		topic.Partitions = append(topic.Partitions, part)
	}
	response.Topics = append(response.Topics, topic)
	return response
}
