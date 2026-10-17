package franz

import (
	"context"
	"errors"
	"reflect"
	"testing"

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
	unavailable := errors.New("coordinator unavailable")
	for _, tc := range []struct {
		name       string
		clientID   string
		assignment []byte
		code       int16
		failure    error
		conflict   bool
	}{
		{name: "compatible worker", clientID: "kaferry-worker", assignment: assignment("input")},
		{name: "worker awaiting assignment", clientID: "kaferry-worker"},
		{name: "absent group", code: kerr.GroupIDNotFound.Code},
		{name: "foreign worker", clientID: "other-consumer", conflict: true},
		{name: "other topic", clientID: "kaferry-worker", assignment: assignment("other"), conflict: true},
		{name: "unreadable assignment", clientID: "kaferry-worker", assignment: []byte{0}, conflict: true},
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
					Members: []kmsg.DescribeGroupsResponseGroupMember{{ClientID: tc.clientID, MemberAssignment: tc.assignment}},
				}}}, nil
			}}
			inspector := &Inspector{client: client, topic: "input"}
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
			encoded := (kafkaport.Progress{Fingerprint: "fp", TopicID: topicID(id)}).Encode()
			client := &metadataMock{request: func(_ context.Context, r kmsg.Request) (kmsg.Response, error) {
				if _, ok := r.(*kmsg.MetadataRequest); ok {
					return metadataResponse(id), nil
				}
				req := r.(*kmsg.OffsetFetchRequest)
				if req.Group != "group" || len(req.Groups) != 1 || req.Groups[0].Group != "group" || !req.RequireStable {
					t.Fatal("saved offsets did not request the group's stable commits")
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
			inspector := &Inspector{client: client, topic: "input", log: quietLogger()}
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
			} else if !reflect.DeepEqual(positions, map[int32]int64{0: 42}) || len(progress) != 1 || progress[0].Encode() != encoded {
				t.Fatal("saved progress changed", positions, progress)
			}
		})
	}
}
