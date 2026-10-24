package franz

import (
	"slices"
	"testing"

	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type memberClaim struct {
	generation int32
	partitions []int32
}

func selectedBalance(t *testing.T, balancer kgo.GroupBalancer, count int32, owners ...memberClaim) map[string][]int32 {
	t.Helper()
	members := make([]kmsg.JoinGroupResponseMember, len(owners))
	for i, claim := range owners {
		members[i] = kmsg.JoinGroupResponseMember{MemberID: string(rune('a' + i)), ProtocolMetadata: balancer.JoinGroupMetadata([]string{"input"}, map[string][]int32{"input": claim.partitions}, claim.generation)}
	}
	member, _, err := balancer.MemberBalancer(members)
	require.NoError(t, err)
	assignments, err := member.(kgo.GroupMemberBalancerOrError).BalanceOrError(map[string]int32{"input": count})
	require.NoError(t, err)
	result := map[string][]int32{}
	for _, assignment := range assignments.IntoSyncAssignment() {
		decoded, err := balancer.ParseSyncAssignment(assignment.MemberAssignment)
		require.NoError(t, err)
		result[assignment.MemberID] = decoded["input"]
	}
	return result
}

func TestSelectedBalancerDistributesSparseIDsAndCooperativelyTransfersOwnership(t *testing.T) {
	// Given two selected partitions separated by many excluded IDs.
	p := plan.Plan{Source: plan.Endpoint{Name: "input"}, Partitions: &plan.PartitionFilter{IDs: []int32{1, 8}}}
	b := sourceBalancer(p, "fp")
	require.True(t, b.IsCooperative())
	initial := selectedBalance(t, b, 10, memberClaim{}, memberClaim{})
	require.Len(t, initial["a"], 1)
	require.Len(t, initial["b"], 1)
	all := append(slices.Clone(initial["a"]), initial["b"]...)
	slices.Sort(all)
	require.Equal(t, []int32{1, 8}, all)
	// When a new member joins while the existing member owns both IDs.
	rebalance := selectedBalance(t, b, 10, memberClaim{3, []int32{1, 8}}, memberClaim{})
	// Then the recipient waits for revocation before receiving the moved partition.
	require.Len(t, rebalance["a"], 1)
	require.Empty(t, rebalance["b"])
	settled := selectedBalance(t, b, 10, memberClaim{4, rebalance["a"]}, memberClaim{})
	require.Len(t, settled["a"], 1)
	require.Len(t, settled["b"], 1)
	require.NotEqual(t, settled["a"], settled["b"])
}

func TestComplementBalancerAdmitsAdditionsAndCanAssignNothing(t *testing.T) {
	// Given every current source partition excluded.
	p := plan.Plan{Source: plan.Endpoint{Name: "input"}, Partitions: &plan.PartitionFilter{Exclude: true, IDs: []int32{0, 1}}}
	b := sourceBalancer(p, "fp")
	// When the topic grows, then the same predicate admits only the new partition.
	initial := selectedBalance(t, b, 2, memberClaim{})
	require.Empty(t, initial["a"])
	grown := selectedBalance(t, b, 3, memberClaim{})
	require.Equal(t, []int32{2}, grown["a"])
}

func TestSparseGenerationClaimsCannotSkipRevocation(t *testing.T) {
	// Given a stale claim on partition 1 and a newer member still consuming all three selected IDs.
	p := plan.Plan{Source: plan.Endpoint{Name: "input"}, Partitions: &plan.PartitionFilter{IDs: []int32{1, 8, 9}}}
	b := sourceBalancer(p, "fp")
	// When sticky balancing moves partition 1 back toward the stale claimant.
	first := selectedBalance(t, b, 10, memberClaim{1, []int32{1}}, memberClaim{2, []int32{1, 8, 9}})
	// Then neither member receives partition 1 until its current owner revokes it.
	require.Empty(t, first["a"])
	require.Equal(t, []int32{8, 9}, first["b"])
	second := selectedBalance(t, b, 10, memberClaim{3, first["a"]}, memberClaim{3, first["b"]})
	require.Equal(t, []int32{1}, second["a"])
	require.Equal(t, []int32{8, 9}, second["b"])
}

func TestSelectedProtocolCannotMatchAnOlderOrDifferentQuery(t *testing.T) {
	// Given one old query and two distinct selected query identities.
	p := plan.Plan{Partitions: &plan.PartitionFilter{IDs: []int32{0}}}
	// When advertising group protocols, then there is no fallback shared with incompatible workers.
	require.NotEqual(t, sourceBalancer(p, "first").ProtocolName(), sourceBalancer(p, "second").ProtocolName())
	require.NotEqual(t, sourceBalancer(p, "first").ProtocolName(), sourceBalancer(plan.Plan{}, "first").ProtocolName())
}

func TestSelectedBalancerRejectsIncompatibleMetadataBeforeAssigning(t *testing.T) {
	for _, mode := range []string{"missing ID", "wrong subscription", "wrong owned topic", "excluded owner", "malformed metadata"} {
		t.Run(mode, func(t *testing.T) {
			// Given a selected query with incompatible topology or member ownership.
			p := plan.Plan{Source: plan.Endpoint{Name: "input"}, Partitions: &plan.PartitionFilter{IDs: []int32{1, 8}}}
			b := sourceBalancer(p, "fp")
			topics, owned := []string{"input"}, map[string][]int32{}
			count := int32(10)
			switch mode {
			case "missing ID":
				count = 8
			case "wrong subscription":
				topics = []string{"other"}
			case "wrong owned topic":
				owned["other"] = []int32{1}
			case "excluded owner":
				owned["input"] = []int32{0}
			}
			encoded := b.JoinGroupMetadata(topics, owned, 1)
			if mode == "malformed metadata" {
				encoded = []byte{0}
			}
			// When balancing, then no assignment is usable after the incompatibility.
			member, _, err := b.MemberBalancer([]kmsg.JoinGroupResponseMember{{MemberID: "a", ProtocolMetadata: encoded}})
			if mode == "malformed metadata" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			_, err = member.(kgo.GroupMemberBalancerOrError).BalanceOrError(map[string]int32{"input": count})
			require.Error(t, err)
		})
	}
}
