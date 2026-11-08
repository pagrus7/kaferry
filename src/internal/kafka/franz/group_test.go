package franz

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"github.com/pagrus7/kaferry/src/internal/kafka"
	"github.com/pagrus7/kaferry/src/internal/kafka/commitmeta"
	"github.com/pagrus7/kaferry/src/internal/plan"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestGroupInspectionAllowsCompatibleWorkersAndRejectsConflicts(t *testing.T) {
	assignment := func(topic string) []byte {
		a := kmsg.NewConsumerMemberAssignment()
		a.Topics = []kmsg.ConsumerMemberAssignmentTopic{{Topic: topic, Partitions: []int32{0}}}
		return a.AppendTo(nil)
	}
	member := func(clientID string, assignment []byte) []kmsg.DescribeGroupsResponseGroupMember {
		return []kmsg.DescribeGroupsResponseGroupMember{{ClientID: clientID, MemberAssignment: assignment}}
	}
	unavailable := errors.New("coordinator unavailable")
	for _, tc := range []struct {
		name     string
		members  []kmsg.DescribeGroupsResponseGroupMember
		code     int16
		failure  error
		conflict bool
	}{
		{name: "compatible worker", members: member("kaferry-worker", assignment("input"))},
		{name: "worker awaiting assignment", members: member("kaferry-worker", nil)},
		{name: "absent group", code: kerr.GroupIDNotFound.Code},
		{name: "foreign worker", members: member("other-consumer", nil), conflict: true},
		{name: "waiting worker before foreign member", members: []kmsg.DescribeGroupsResponseGroupMember{
			{ClientID: "kaferry-waiting"}, {ClientID: "other-consumer"},
		}, conflict: true},
		{name: "other topic", members: member("kaferry-worker", assignment("other")), conflict: true},
		{name: "unreadable assignment", members: member("kaferry-worker", []byte{0}), conflict: true},
		{name: "group access denied", code: kerr.GroupAuthorizationFailed.Code, failure: kerr.GroupAuthorizationFailed},
		{name: "group unavailable", failure: unavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given the requested group's members or a failed inspection.
			requests := 0
			client := &metadataMock{request: func(_ context.Context, r kmsg.Request) (kmsg.Response, error) {
				requests++
				require.Equal(t, []string{"group"}, r.(*kmsg.DescribeGroupsRequest).Groups, "inspected the wrong group")
				if tc.failure != nil && tc.code == 0 {
					return nil, tc.failure
				}
				return &kmsg.DescribeGroupsResponse{Groups: []kmsg.DescribeGroupsResponseGroup{{
					Group: "group", Protocol: "cooperative-sticky", ErrorCode: tc.code,
					Members: tc.members,
				}}}, nil
			}}
			inspector := &Inspector{codec: commitmeta.NewCodec(), client: client, topic: "input"}
			// When deciding whether this query may join the group.
			err := inspector.CheckMembers(t.Context(), "group", "cooperative-sticky")
			// Then only an absent group or compatible members pass; inspection failures remain errors.
			var conflict *kafka.Conflict
			if tc.conflict {
				require.ErrorAs(t, err, &conflict, "incompatible member accepted")
			} else if !errors.Is(err, tc.failure) {
				t.Fatal("incorrect group admission", err)
			}
			require.Equal(t, 1, requests, "non-coordinator failures must not be retried")
		})
	}
}

func TestGroupInspectionRetriesCoordinatorErrors(t *testing.T) {
	for _, failure := range []*kerr.Error{kerr.CoordinatorNotAvailable, kerr.CoordinatorLoadInProgress, kerr.NotCoordinator} {
		for _, responseError := range []bool{false, true} {
			name := failure.Message + "/request"
			if responseError {
				name = failure.Message + "/response"
			}
			t.Run(name, func(t *testing.T) {
				coordinatorRetry(t, failure, responseError)
			})
		}
	}
}

func TestGroupInspectionRetriesShareOneDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Given a coordinator that stays unavailable for the whole request budget.
		var deadline time.Time
		requests := 0
		client := &metadataMock{request: func(ctx context.Context, _ kmsg.Request) (kmsg.Response, error) {
			requests++
			current, ok := ctx.Deadline()
			require.True(t, ok)
			if requests == 1 {
				deadline = current
			}
			require.Equal(t, deadline, current, "a retry must not extend the request deadline")
			return nil, kerr.CoordinatorNotAvailable
		}}
		inspector := &Inspector{client: client, topic: "input"}
		started := time.Now()
		// When the inspection exhausts its shared budget.
		err := inspector.CheckMembers(t.Context(), "group", "cooperative-sticky")
		// Then it fails after ten seconds, without treating the group as absent.
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Greater(t, requests, 1)
		require.Equal(t, 10*time.Second, time.Since(started))
	})
}

func TestGroupInspectionCancellationStopsRequests(t *testing.T) {
	for _, before := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			// Given cancellation before inspection or immediately after a coordinator error.
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if before {
				cancel()
			}
			requests := 0
			client := &metadataMock{request: func(context.Context, kmsg.Request) (kmsg.Response, error) {
				requests++
				cancel()
				return nil, kerr.CoordinatorNotAvailable
			}}
			inspector := &Inspector{client: client, topic: "input"}
			// When checking group membership.
			err := inspector.CheckMembers(ctx, "group", "cooperative-sticky")
			// Then cancellation fails inspection and no further request is sent.
			require.ErrorIs(t, err, context.Canceled)
			want := 1
			if before {
				want = 0
			}
			require.Equal(t, want, requests)
		})
	}
}

func TestGroupInspectionValidatesTheResponseAfterRetry(t *testing.T) {
	for _, tc := range []struct {
		name   string
		groups []kmsg.DescribeGroupsResponseGroup
		want   string
	}{
		{"foreign member", []kmsg.DescribeGroupsResponseGroup{{Group: "group", Members: []kmsg.DescribeGroupsResponseGroupMember{{ClientID: "other-consumer"}}}}, "foreign active member"},
		{"access denied", []kmsg.DescribeGroupsResponseGroup{{Group: "group", ErrorCode: kerr.GroupAuthorizationFailed.Code}}, "GROUP_AUTHORIZATION_FAILED"},
		{"missing group", nil, "missing group description"},
		{"wrong group", []kmsg.DescribeGroupsResponseGroup{{Group: "other"}}, "missing group description"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				// Given an unavailable coordinator followed by an inadmissible response.
				requests := 0
				client := &metadataMock{request: func(context.Context, kmsg.Request) (kmsg.Response, error) {
					requests++
					if requests == 1 {
						return nil, kerr.CoordinatorNotAvailable
					}
					return &kmsg.DescribeGroupsResponse{Groups: tc.groups}, nil
				}}
				inspector := &Inspector{client: client, topic: "input"}
				// When the coordinator recovers.
				err := inspector.CheckMembers(t.Context(), "group", "cooperative-sticky")
				// Then retrying never bypasses membership, authorization or response checks.
				require.ErrorContains(t, err, tc.want)
				require.Equal(t, 2, requests)
			})
		})
	}
}

func TestSavedOffsetsDistinguishAbsentGroupsFromInspectionFailures(t *testing.T) {
	for _, tc := range []struct {
		name    string
		grouped bool
		code    int16
		failure error
	}{
		{name: "legacy offsets"},
		{name: "topic ID offsets", grouped: true},
		{name: "absent legacy group", code: kerr.GroupIDNotFound.Code},
		{name: "absent grouped response", grouped: true, code: kerr.GroupIDNotFound.Code},
		{name: "legacy group access denied", code: kerr.GroupAuthorizationFailed.Code, failure: kerr.GroupAuthorizationFailed},
		{name: "grouped access denied", grouped: true, code: kerr.GroupAuthorizationFailed.Code, failure: kerr.GroupAuthorizationFailed},
		{name: "coordinator unavailable", failure: kerr.CoordinatorNotAvailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			savedOffsetInspection(t, tc.grouped, tc.code, tc.failure)
		})
	}
}

func TestMembershipObservesActivityAndCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name, state, client string
		assignment          []byte
		idle, foreign       bool
	}{
		{name: "empty", state: "Empty", idle: true},
		{name: "dead", state: "Dead", idle: true},
		{name: "rebalance without members", state: "PreparingRebalance"},
		{name: "unrecognized state", state: "unknown"},
		{name: "active", state: "Stable", client: "kaferry-worker"},
		{name: "foreign", state: "Stable", client: "other", foreign: true},
		{name: "invalid assignment", state: "Stable", client: "kaferry-worker", assignment: []byte{0}, foreign: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given an independently mocked broker group description.
			members := []kmsg.DescribeGroupsResponseGroupMember{}
			if tc.client != "" {
				members = append(members, kmsg.DescribeGroupsResponseGroupMember{ClientID: tc.client, MemberAssignment: tc.assignment})
			}
			client := &metadataMock{request: func(_ context.Context, r kmsg.Request) (kmsg.Response, error) {
				if req := r.(*kmsg.DescribeGroupsRequest); !reflect.DeepEqual(req.Groups, []string{"group"}) {
					t.Fatal(req)
				}
				return &kmsg.DescribeGroupsResponse{Groups: []kmsg.DescribeGroupsResponseGroup{{Group: "group", Protocol: "cooperative-sticky", State: tc.state, Members: members}}}, nil
			}}
			inspector := &Inspector{client: client, topic: "input"}
			// When deriving an observation for the status service.
			got, err := inspector.Membership(t.Context(), "group", "cooperative-sticky")
			// Then active, foreign and unsettled empty groups cannot be mistaken for idle groups.
			if err != nil || got.State != tc.state || got.Idle != tc.idle || got.Foreign != tc.foreign {
				t.Fatal(got, err)
			}
		})
	}
}

func TestGroupInspectionRejectsDifferentSelectionsEvenBeforeAssignment(t *testing.T) {
	for _, actual := range []string{"cooperative-sticky", "kaferry-partitions-v1-other"} {
		// Given a Kaferry worker waiting for assignment under another protocol.
		client := &metadataMock{request: func(context.Context, kmsg.Request) (kmsg.Response, error) {
			return &kmsg.DescribeGroupsResponse{Groups: []kmsg.DescribeGroupsResponseGroup{{Group: "group", State: "Stable", Protocol: actual, Members: []kmsg.DescribeGroupsResponseGroupMember{{ClientID: "kaferry-worker"}}}}}, nil
		}}
		i := &Inspector{client: client, topic: "input"}
		// When checking run admission or status, then protocol mismatch is incompatible without needing assigned partitions.
		require.ErrorContains(t, i.CheckMembers(t.Context(), "group", "kaferry-partitions-v1-expected"), "incompatible partition selection")
		membership, err := i.Membership(t.Context(), "group", "kaferry-partitions-v1-expected")
		require.NoError(t, err)
		require.True(t, membership.Foreign)
	}
}

func TestPreparingGroupCanAdmitWorkersBeforeItsProtocolIsElected(t *testing.T) {
	// Given a new group with waiting Kaferry members and no elected protocol yet.
	client := &metadataMock{request: func(context.Context, kmsg.Request) (kmsg.Response, error) {
		return &kmsg.DescribeGroupsResponse{Groups: []kmsg.DescribeGroupsResponseGroup{{Group: "group", State: "PreparingRebalance", Members: []kmsg.DescribeGroupsResponseGroupMember{{ClientID: "kaferry-worker"}}}}}, nil
	}}
	i := &Inspector{client: client, topic: "input"}
	// When inspecting, then defer protocol compatibility to the coordinator's fenced join and report activity.
	require.NoError(t, i.CheckMembers(t.Context(), "group", "kaferry-partitions-v1-expected"))
	m, err := i.Membership(t.Context(), "group", "kaferry-partitions-v1-expected")
	require.NoError(t, err)
	require.False(t, m.Foreign)
	require.False(t, m.Idle)
}

func TestSavedOffsetsRejectFailedMetadataWithoutChangingCachedIdentity(t *testing.T) {
	for _, mode := range []string{"recreated", "topic authorization", "partition error"} {
		t.Run(mode, func(t *testing.T) {
			// Given a cached source identity and partition set from an earlier successful refresh.
			id := [16]byte{1}
			response := metadataResponse(id)
			requests := 0
			client := &metadataMock{request: func(_ context.Context, request kmsg.Request) (kmsg.Response, error) {
				requests++
				if _, ok := request.(*kmsg.MetadataRequest); !ok {
					return nil, errors.New("offset fetch must not follow failed metadata")
				}
				return response, nil
			}}
			inspector := &Inspector{codec: commitmeta.NewCodec(), client: client, topic: "input", log: quietLogger()}
			require.NoError(t, inspector.Refresh(t.Context()))
			switch mode {
			case "recreated":
				response.Topics[0].TopicID = [16]byte{2}
			case "topic authorization":
				response.Topics[0].ErrorCode = kerr.TopicAuthorizationFailed.Code
			case "partition error":
				response.Topics[0].Partitions[1].ErrorCode = kerr.NotLeaderForPartition.Code
			}
			// When saved progress refreshes the source metadata before requesting offsets.
			saved, err := inspector.Saved(t.Context(), "group", "fp", plan.WindowSpec{}, 0)
			// Then failure preserves the cache and exposes no progress or offset-fetch request.
			require.Error(t, err)
			require.Nil(t, saved)
			require.Equal(t, id, inspector.topicID)
			require.Equal(t, "cluster", inspector.clusterID)
			require.Equal(t, []int32{0, 2}, inspector.parts)
			require.Equal(t, 2, requests)
		})
	}
}

func coordinatorRetry(t *testing.T, failure *kerr.Error, responseError bool) {
	synctest.Test(t, func(t *testing.T) {
		// Given a coordinator error followed by compatible group members.
		requests := 0
		client := &metadataMock{request: func(_ context.Context, request kmsg.Request) (kmsg.Response, error) {
			requests++
			require.Equal(t, []string{"group"}, request.(*kmsg.DescribeGroupsRequest).Groups)
			group := kmsg.DescribeGroupsResponseGroup{Group: "group", Protocol: "cooperative-sticky", Members: []kmsg.DescribeGroupsResponseGroupMember{{ClientID: "kaferry-worker"}}}
			if requests == 1 {
				if !responseError {
					return nil, failure
				}
				group.ErrorCode = failure.Code
			}
			return &kmsg.DescribeGroupsResponse{Groups: []kmsg.DescribeGroupsResponseGroup{group}}, nil
		}}
		inspector := &Inspector{client: client, topic: "input"}
		// When inspecting the group before joining.
		err := inspector.CheckMembers(t.Context(), "group", "cooperative-sticky")
		// Then retry the request and admit only after the successful description.
		require.NoError(t, err)
		require.Equal(t, 2, requests)
	})
}

func savedOffsetInspection(t *testing.T, grouped bool, code int16, failure error) {
	// Given a retained commit and a legacy or grouped Kafka offset response.
	id := [16]byte{1}
	encoded := encodeMetadata(kafka.Progress{Fingerprint: "fp", TopicID: topicID(id)})
	client := &metadataMock{request: func(_ context.Context, r kmsg.Request) (kmsg.Response, error) {
		if _, ok := r.(*kmsg.MetadataRequest); ok {
			return metadataResponse(id), nil
		}
		req := r.(*kmsg.OffsetFetchRequest)
		require.Equal(t, "group", req.Group)
		require.Len(t, req.Groups, 1)
		require.Equal(t, "group", req.Groups[0].Group)
		require.True(t, req.RequireStable)

		group := req.Groups[0]
		require.EqualValues(t, -1, group.MemberEpoch)
		require.Len(t, group.Topics, 1)
		require.Equal(t, id, group.Topics[0].TopicID)
		require.Equal(t, []int32{0, 2}, group.Topics[0].Partitions)

		if failure != nil && code == 0 {
			return nil, failure
		}
		if grouped {
			return &kmsg.OffsetFetchResponse{Groups: []kmsg.OffsetFetchResponseGroup{{Group: "group", ErrorCode: code,
				Topics: []kmsg.OffsetFetchResponseGroupTopic{{TopicID: id, Partitions: []kmsg.OffsetFetchResponseGroupTopicPartition{
					{Partition: 0, Offset: 42, Metadata: &encoded}, {Partition: 2, Offset: -1},
				}}},
			}}}, nil
		}
		return &kmsg.OffsetFetchResponse{ErrorCode: code, Topics: []kmsg.OffsetFetchResponseTopic{{Topic: "input",
			Partitions: []kmsg.OffsetFetchResponseTopicPartition{{Partition: 0, Offset: 42, Metadata: &encoded}, {Partition: 2, Offset: -1}},
		}}}, nil
	}}
	inspector := &Inspector{codec: commitmeta.NewCodec(), client: client, topic: "input", log: quietLogger()}
	// When loading progress before positioning a source.
	saved, err := inspector.Saved(t.Context(), "group", "fp", plan.WindowSpec{}, 0)
	// Then unavailable or unauthorized progress cannot become a fresh start.
	require.ErrorIs(t, err, failure, "incorrect saved-offset outcome")
	if failure != nil || code == kerr.GroupIDNotFound.Code {
		require.Empty(t, saved)

	} else if len(saved) != 1 || saved[0].Position != 42 || encodeMetadata(saved[0].Progress) != encoded {
		t.Fatal("saved progress changed", saved)
	}
}
