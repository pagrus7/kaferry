//go:build integration && fault && load

package compat

import (
	"context"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func checkOwnershipObserverFailure(t *testing.T, s *ownershipCase, mode string) {
	t.Helper()
	query, done := context.WithTimeout(t.Context(), 2*time.Second)
	defer done()
	var err error
	switch mode {
	case "expired":
		done()
		observed := s.observe(query)
		require.NotEmpty(t, observed.Error)
		require.Nil(t, observed.Saved)
		return
	case "wrong group", "group error", "malformed assignment":
		_, _, err = s.groupMembers(query, 3)
	default:
		var saved []int64
		saved, err = s.savedOffsets(query, 3)
		require.Nil(t, saved)
		if mode == "missing partition" {
			require.NoError(t, err)
			return
		}
	}
	require.Error(t, err)
	require.NotErrorIs(t, err, context.DeadlineExceeded, "the explicit malformed response, not a timeout, must reject the observation")
}

func ownershipObserverMetadata(version int16, routes []string) *kmsg.MetadataResponse {
	response := kmsg.NewPtrMetadataResponse()
	response.Version = version
	for index, address := range routes {
		host, port, _ := net.SplitHostPort(address)
		number, _ := strconv.Atoi(port)
		response.Brokers = append(response.Brokers, kmsg.MetadataResponseBroker{NodeID: int32(index + 1), Host: host, Port: int32(number)})
	}
	return response
}

type ownershipBrokerProxy struct {
	listener net.Listener
	mu       sync.Mutex
	blocked  bool
	clients  []net.Conn
	rejected atomic.Int32
	accepted chan struct{}
	joined   sync.WaitGroup
}

func newOwnershipBrokerProxy(t *testing.T, backend string) *ownershipBrokerProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	proxy := &ownershipBrokerProxy{listener: listener, accepted: make(chan struct{})}
	go func() {
		defer close(proxy.accepted)
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			proxy.mu.Lock()
			if proxy.blocked {
				proxy.rejected.Add(1)
				_ = connection.Close()
				proxy.mu.Unlock()
				continue
			}
			proxy.clients = append(proxy.clients, connection)
			proxy.joined.Go(func() {
				defer func() { _ = connection.Close() }()
				upstream, err := net.DialTimeout("tcp", backend, time.Second)
				if err != nil {
					return
				}
				defer func() { _ = upstream.Close() }()
				finished := make(chan struct{})
				go func() {
					_, _ = io.Copy(upstream, connection)
					_ = upstream.Close()
					close(finished)
				}()
				_, _ = io.Copy(connection, upstream)
				_ = connection.Close()
				<-finished
			})
			proxy.mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-proxy.accepted
		proxy.disconnect()
		proxy.joined.Wait()
	})
	return proxy
}

func (p *ownershipBrokerProxy) disconnect() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blocked = true
	for _, client := range p.clients {
		_ = client.Close()
	}
}

func ownershipObserverGroup(version int16, source, group, mode string) *kmsg.DescribeGroupsResponse {
	assignment := kmsg.ConsumerMemberAssignment{Topics: []kmsg.ConsumerMemberAssignmentTopic{{Topic: source, Partitions: []int32{0}}}}
	response := kmsg.NewPtrDescribeGroupsResponse()
	response.Version = version
	response.Groups = []kmsg.DescribeGroupsResponseGroup{{Group: group, State: "Stable", ProtocolType: "consumer",
		Members: []kmsg.DescribeGroupsResponseGroupMember{{MemberID: "member", ClientID: "worker", MemberAssignment: assignment.AppendTo(nil)}}}}
	switch mode {
	case "wrong group":
		response.Groups[0].Group = "another-group"
	case "group error":
		response.Groups[0].ErrorCode = 16
	case "malformed assignment":
		response.Groups[0].Members[0].MemberAssignment = []byte{0}
	}
	return response
}

func ownershipObserverOffsets(version int16, source, group string, sourceID [16]byte, mode string) *kmsg.OffsetFetchResponse {
	response := kmsg.NewPtrOffsetFetchResponse()
	response.Version = version
	topic := kmsg.OffsetFetchResponseGroupTopic{Topic: source, TopicID: sourceID}
	for partition := range replicatedPartitions {
		topic.Partitions = append(topic.Partitions, kmsg.OffsetFetchResponseGroupTopicPartition{Partition: int32(partition), Offset: 17})
	}
	response.Groups = []kmsg.OffsetFetchResponseGroup{{Group: group, Topics: []kmsg.OffsetFetchResponseGroupTopic{topic}}}
	switch mode {
	case "wrong offset group":
		response.Groups[0].Group = "another-group"
	case "offset group error":
		response.Groups[0].ErrorCode = 16
	case "wrong topic identity":
		response.Groups[0].Topics[0].TopicID = [16]byte{2}
	case "missing partition":
		response.Groups[0].Topics[0].Partitions = topic.Partitions[1:]
	case "repeated partition":
		response.Groups[0].Topics[0].Partitions[1].Partition = 0
	case "partition error":
		response.Groups[0].Topics[0].Partitions[0].ErrorCode = 16
	}
	return response
}
