package faults

import (
	"context"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"
)

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

func testPinnedMetadata(t *testing.T, mode string) {
	t.Helper()
	cluster := kfake.MustCluster(kfake.NumBrokers(4), kfake.ClusterID("source-id"))
	t.Cleanup(cluster.Close)
	routes := cluster.ListenAddrs()[1:]
	var blocked atomic.Bool
	var requests [4]atomic.Int32
	cluster.ControlKey(int16(kmsg.Metadata), func(request kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		metadata := request.(*kmsg.MetadataRequest)
		node := cluster.CurrentNode()
		if metadata.Topics == nil {
			requests[node].Add(1)
		}
		if blocked.Load() && node == 2 && metadata.Topics == nil {
			return nil, nil, true
		}
		response := pinnedMetadataFixture(metadata.Version, routes)
		if mode == "wrong route" {
			response.Brokers[1].Port = response.Brokers[0].Port
		}
		if mode == "unselected live broker" {
			host, port, _ := net.SplitHostPort(cluster.ListenAddrs()[0])
			number, _ := strconv.Atoi(port)
			response.Brokers = append(response.Brokers, kmsg.MetadataResponseBroker{NodeID: 0, Host: host, Port: int32(number)})
		}
		return response, nil, true
	})
	client, err := NewMetadataClient("source", routes...)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	target := Target{Cluster: "source", Broker: 2, Service: "source-2"}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if mode != "wrong route" {
		_, initial, probeErr := client.ProbeMetadata(ctx, target)
		require.NoError(t, probeErr)
		require.Equal(t, routes[1], initial.RemoteAddress)
		require.Equal(t, int32(2), initial.TransportBroker)
		require.True(t, initial.AllTopics)
	}
	if mode == "unselected live broker" {
		// Given a live broker learned from metadata but outside the selected
		// ADMIN routes, prove that the raw client can reach its real endpoint.
		_, rawErr := client.raw.Broker(0).Request(ctx, kmsg.NewPtrMetadataRequest())
		require.NoError(t, rawErr)
		before := requests[0].Load()
		require.Positive(t, before)
		require.NotContains(t, client.AdminRoutes(), int32(0))
		// When the observer requests that broker, then reject it before sending
		// another request, even though cached metadata permits a direct request.
		_, rejected := client.RequestBroker(ctx, 0, kmsg.NewPtrMetadataRequest())
		require.ErrorContains(t, rejected, "outside the selected ADMIN routes")
		require.Equal(t, before, requests[0].Load())
		return
	}
	switch mode {
	case "blocked target":
		blocked.Store(true)
	case "cancelled":
		cancel()
	case "wrong cluster":
		target.Cluster = "destination"
	case "wrong broker":
		target.Broker = 4
	}
	_, proof, err := client.ProbeMetadata(ctx, target)
	if mode == "reused connection" {
		require.NoError(t, err)
		require.GreaterOrEqual(t, requests[2].Load(), int32(2))
		require.Equal(t, routes[1], proof.TransportAddress)
		require.False(t, proof.ConnectedAt.After(proof.ResponseAt))
	} else {
		require.Error(t, err)
		require.NotEmpty(t, proof.Error)
	}
}

func pinnedMetadataFixture(version int16, routes []string) *kmsg.MetadataResponse {
	response := kmsg.NewPtrMetadataResponse()
	response.Version = version
	response.ClusterID = kmsg.StringPtr("source-id")
	response.ControllerID = 1
	for index, address := range routes {
		host, port, _ := net.SplitHostPort(address)
		number, _ := strconv.Atoi(port)
		response.Brokers = append(response.Brokers, kmsg.MetadataResponseBroker{NodeID: int32(index + 1), Host: host, Port: int32(number)})
	}
	return response
}
