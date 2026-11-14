//go:build integration && fault && load && harness

package compat

import (
	"context"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
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
