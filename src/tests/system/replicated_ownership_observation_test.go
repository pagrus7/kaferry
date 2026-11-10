//go:build integration && fault && load

package compat

import (
	"context"
	"fmt"
	"io"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/src/tests/system/faults"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestOwnershipObservationFollowsFreshCoordinatorBehindAcceptingProxy(t *testing.T) {
	// Given cached group reads on broker1 behind a real forwarding proxy.
	cluster := kfake.MustCluster(kfake.NumBrokers(4))
	t.Cleanup(cluster.Close)
	var coordinator atomic.Int32
	coordinator.Store(1)
	var reply atomic.Value
	reply.Store("healthy")
	routes := make([]string, 3)
	proxies := make([]*ownershipBrokerProxy, 3)
	for index, backend := range cluster.ListenAddrs()[1:] {
		proxies[index] = newOwnershipBrokerProxy(t, backend)
		routes[index] = proxies[index].listener.Addr().String()
	}
	s := &ownershipCase{group: "group", src: "source", sourceID: [16]byte{1}, workers: nil,
		publisher: &replicatedPublisher{state: replicatedPublishing{Started: time.Now().UTC()}}}
	cluster.ControlKey(int16(kmsg.Metadata), func(request kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		return ownershipObserverMetadata(request.GetVersion(), routes), nil, true
	})
	cluster.ControlKey(int16(kmsg.FindCoordinator), func(request kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		node := coordinator.Load()
		host, port, _ := net.SplitHostPort(routes[node-1])
		number, _ := strconv.Atoi(port)
		response := kmsg.NewPtrFindCoordinatorResponse()
		response.Version = request.GetVersion()
		response.Coordinators = []kmsg.FindCoordinatorResponseCoordinator{{Key: s.group, NodeID: node, Host: host, Port: int32(number)}}
		return response, nil, true
	})
	cluster.ControlKey(int16(kmsg.DescribeGroups), func(request kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		if cluster.CurrentNode() != coordinator.Load() {
			return nil, fmt.Errorf("group read reached stale coordinator %d", cluster.CurrentNode()), true
		}
		response := ownershipObserverGroup(request.GetVersion(), s.src, s.group, reply.Load().(string))
		return response, nil, true
	})
	cluster.ControlKey(int16(kmsg.OffsetFetch), func(request kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		if cluster.CurrentNode() != coordinator.Load() {
			return nil, fmt.Errorf("offset read reached stale coordinator %d", cluster.CurrentNode()), true
		}
		response := ownershipObserverOffsets(request.GetVersion(), s.src, s.group, s.sourceID, reply.Load().(string))
		return response, nil, true
	})
	client, err := faults.NewMetadataClient("source", routes...)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	s.metadata = faults.KafkaMetadata{"source": client}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	_, err = client.DescribeGroups(ctx, s.group)
	require.NoError(t, err)
	_, err = client.FetchOffsets(ctx, s.group)
	require.NoError(t, err)

	// When the old proxy still accepts TCP but closes every backend path, the
	// SDK's cached group reads fail even after the actual coordinator changes.
	proxies[0].disconnect()
	coordinator.Store(3)
	_, err = client.DescribeGroups(ctx, s.group)
	require.Error(t, err, "the regression requires the stale cached group route")
	_, err = client.FetchOffsets(ctx, s.group)
	require.Error(t, err, "saved offsets must not keep using the old coordinator either")
	cancel()
	recovery, recovered := context.WithTimeout(t.Context(), 2*time.Second)
	defer recovered()
	var state ownershipCaseObservation
	for recovery.Err() == nil {
		state = s.observe(recovery)
		if state.Error == "" {
			break
		}
	}
	// Then the unchanged bounded observer resolves and pins both real reads
	// to the new coordinator, without changing the product client's policy.
	require.NoError(t, recovery.Err(), "%+v", state)
	require.Empty(t, state.Error)
	require.EqualValues(t, 3, state.Coordinator)
	require.Equal(t, "Stable", state.State)
	require.Equal(t, []ownershipMember{{ClientID: "worker", MemberID: "member", Partitions: []int32{0}}}, state.Members)
	require.Equal(t, slices.Repeat([]int64{17}, replicatedPartitions), state.Saved)
	require.Positive(t, proxies[0].rejected.Load(), "the old route must accept and close, not fail TCP dialing")
	for _, mode := range []string{"wrong group", "group error", "malformed assignment", "wrong offset group", "offset group error", "wrong topic identity", "missing partition", "repeated partition", "partition error", "expired"} {
		t.Run(mode, func(t *testing.T) {
			// Given a freshly resolved coordinator with an invalid observation,
			// when read directly, then no complete saved/membership proof survives.
			reply.Store(mode)
			checkOwnershipObserverFailure(t, s, mode)
		})
	}
}

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

func TestReplicatedAuditCohortUsesOneCompleteParsedSnapshot(t *testing.T) {
	// Given thousands of irrelevant completed requests surrounding all three
	// members' current joins and accepted commits, retain every decoded row.
	members := ownershipGenerationMembers()
	junk := `Completed request:{"requestHeader":{"clientId":"observer","requestApiKeyName":"FETCH"},"request":{"padding":"` + strings.Repeat("x", 2048) + `"},"response":{}}` + "\n"
	data := []byte(strings.Repeat(junk, 3000) + ownershipGenerationRows(members, 0, 3))
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	// When the shared snapshot is decoded once, then every member uses those
	// same validated rows while preserving its generation and partition proof.
	rows, err := decodeOwnershipAudit(ctx, data)
	require.NoError(t, err)
	require.Len(t, rows, 3000+3+replicatedPartitions)
	for _, member := range members {
		generation, ready, proofErr := currentGenerationSavedRows(rows, member.ClientID, "group", member.MemberID, member.Partitions, nil, time.Time{})
		require.NoError(t, proofErr)
		require.EqualValues(t, 3, generation)
		require.True(t, ready)
	}
	// Then malformed irrelevant rows still reject the whole snapshot, even
	// when every selected member's complete accepted proof precedes them.
	_, err = decodeOwnershipAudit(ctx, append(data, []byte("Completed request:{\n")...))
	require.Error(t, err)
	cancel()
	rows, err = decodeOwnershipAudit(ctx, data)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, rows, "expired parsing cannot expose a partial passing cohort")
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
