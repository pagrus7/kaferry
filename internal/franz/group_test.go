package franz

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/pagrus7/kaferry/internal/commitmeta"
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/plan"
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
			client := &metadataMock{request: func(_ context.Context, r kmsg.Request) (kmsg.Response, error) {
				if !reflect.DeepEqual(r.(*kmsg.DescribeGroupsRequest).Groups, []string{"group"}) {
					t.Fatal("inspected the wrong group")
				}
				if tc.failure != nil && tc.code == 0 {
					return nil, tc.failure
				}
				return &kmsg.DescribeGroupsResponse{Groups: []kmsg.DescribeGroupsResponseGroup{{
					Group: "group", ErrorCode: tc.code,
					Members: tc.members,
				}}}, nil
			}}
			inspector := &Inspector{codec: commitmeta.NewCodec(), client: client, topic: "input"}
			// When deciding whether this query may join the group.
			err := inspector.CheckMembers(t.Context(), "group")
			// Then only an absent group or compatible members pass; inspection failures remain errors.
			var conflict *kafkaport.Conflict
			if tc.conflict {
				if !errors.As(err, &conflict) {
					t.Fatal("incompatible member accepted", err)
				}
			} else if !errors.Is(err, tc.failure) {
				t.Fatal("incorrect group admission", err)
			}
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
			// Given a retained commit and a legacy or grouped Kafka offset response.
			id := [16]byte{1}
			encoded := encodeMetadata(kafkaport.Progress{Fingerprint: "fp", TopicID: topicID(id)})
			client := &metadataMock{request: func(_ context.Context, r kmsg.Request) (kmsg.Response, error) {
				if _, ok := r.(*kmsg.MetadataRequest); ok {
					return metadataResponse(id), nil
				}
				req := r.(*kmsg.OffsetFetchRequest)
				if req.Group != "group" || len(req.Groups) != 1 || req.Groups[0].Group != "group" || !req.RequireStable {
					t.Fatal("saved offsets did not request the group's stable commits")
				}
				group := req.Groups[0]
				if group.MemberEpoch != -1 || len(group.Topics) != 1 || group.Topics[0].TopicID != id ||
					!reflect.DeepEqual(group.Topics[0].Partitions, []int32{0, 2}) {
					t.Fatal("saved offsets did not identify the topic and partitions for a non-member")
				}
				if tc.failure != nil && tc.code == 0 {
					return nil, tc.failure
				}
				if tc.grouped {
					return &kmsg.OffsetFetchResponse{Groups: []kmsg.OffsetFetchResponseGroup{{Group: "group", ErrorCode: tc.code,
						Topics: []kmsg.OffsetFetchResponseGroupTopic{{TopicID: id, Partitions: []kmsg.OffsetFetchResponseGroupTopicPartition{
							{Partition: 0, Offset: 42, Metadata: &encoded}, {Partition: 2, Offset: -1},
						}}},
					}}}, nil
				}
				return &kmsg.OffsetFetchResponse{ErrorCode: tc.code, Topics: []kmsg.OffsetFetchResponseTopic{{Topic: "input",
					Partitions: []kmsg.OffsetFetchResponseTopicPartition{{Partition: 0, Offset: 42, Metadata: &encoded}, {Partition: 2, Offset: -1}},
				}}}, nil
			}}
			inspector := &Inspector{codec: commitmeta.NewCodec(), client: client, topic: "input", log: quietLogger()}
			// When loading progress before positioning a source.
			positions, progress, err := inspector.Saved(t.Context(), "group", "fp", plan.WindowSpec{}, 0)
			// Then unavailable or unauthorized progress cannot become a fresh start.
			if !errors.Is(err, tc.failure) {
				t.Fatal("incorrect saved-offset outcome", err)
			}
			if tc.failure != nil || tc.code == kerr.GroupIDNotFound.Code {
				if len(positions) != 0 || len(progress) != 0 {
					t.Fatal("unexpected usable progress", positions, progress)
				}
			} else if !reflect.DeepEqual(positions, map[int32]int64{0: 42}) || len(progress) != 1 || encodeMetadata(progress[0]) != encoded {
				t.Fatal("saved progress changed", positions, progress)
			}
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
				return &kmsg.DescribeGroupsResponse{Groups: []kmsg.DescribeGroupsResponseGroup{{Group: "group", State: tc.state, Members: members}}}, nil
			}}
			inspector := &Inspector{client: client, topic: "input"}
			// When deriving an observation for the status service.
			got, err := inspector.Membership(t.Context(), "group")
			// Then active, foreign and unsettled empty groups cannot be mistaken for idle groups.
			if err != nil || got.State != tc.state || got.Members != len(members) || got.Idle != tc.idle || got.Foreign != tc.foreign {
				t.Fatal(got, err)
			}
		})
	}
}
