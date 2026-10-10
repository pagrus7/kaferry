package kafka

import (
	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pagrus7/kaferry/internal/plan"
	"testing"
)

func TestOffsetsMatchTopicIdentityNotPointerAddress(t *testing.T) {
	// Given
	requested, returned := "orders", "orders"
	result := k.ListOffsetsResult{ResultInfos: map[k.TopicPartition]k.ListOffsetsResultInfo{{Topic: &returned, Partition: 3}: {Offset: 42}}}

	// When
	info, ok := partitionInfos(result, requested)[3]

	// Then
	if !ok || info.Offset != 42 {
		t.Fatal("returned topic pointer prevented matching Kafka partition")
	}
}

func TestInspectorClosesAdminWhenPreflightFails(t *testing.T) {
	// Given
	for _, result := range []k.DescribeTopicsResult{{}, {TopicDescriptions: []k.TopicDescription{{Name: "orders"}}}} {
		admin := &adminMock{described: result}

		// When
		_, newInspectorErr := NewInspector(&mockClients{admin: admin}, plan.Properties{}, "orders", quietLogger())

		// Then
		if newInspectorErr == nil || !admin.closed {
			t.Fatal("invalid topic identity accepted or admin leaked", newInspectorErr)
		}
	}
}

func TestInspectorRequestsCurrentPartitions(t *testing.T) {
	// Given
	description := k.TopicDescription{Name: "orders", Partitions: []k.TopicPartitionInfo{{Partition: 0}, {Partition: 2}}}
	admin := &adminMock{described: k.DescribeTopicsResult{TopicDescriptions: []k.TopicDescription{description}}, groupOffsets: k.ListConsumerGroupOffsetsResult{ConsumerGroupsTopicPartitions: []k.ConsumerGroupTopicPartitions{{Partitions: []k.TopicPartition{{Partition: 0, Offset: 10}, {Partition: 2, Offset: k.OffsetInvalid}}}}}}
	inspector := &Inspector{admin: admin, topic: "orders", description: description, log: quietLogger()}

	// When
	parts, err := inspector.Progress("group")

	// Then
	if err != nil || len(parts) != 2 || len(admin.requested) != 1 || admin.requested[0].Group != "group" || len(admin.requested[0].Partitions) != 2 || admin.requested[0].Partitions[1].Partition != 2 {
		t.Fatal("current partition discovery failed", parts, admin.requested, err)
	}
}

func TestInspectorRejectsMissingOffsetResultsAndForeignMembers(t *testing.T) {
	// Given
	admin := &adminMock{offsetResult: k.ListOffsetsResult{ResultInfos: map[k.TopicPartition]k.ListOffsetsResultInfo{}}}
	inspector := &Inspector{admin: admin, topic: "orders", log: quietLogger()}

	// When
	_, offsetsErr := inspector.Offsets([]int32{0}, k.LatestOffsetSpec, true)

	// Then
	if offsetsErr == nil {
		t.Fatal("missing offset accepted")
	}
	admin.groups = k.DescribeConsumerGroupsResult{ConsumerGroupDescriptions: []k.ConsumerGroupDescription{{Members: []k.MemberDescription{{ClientID: "another-app"}}}}}
	if err := inspector.CheckMembers("group"); err == nil {
		t.Fatal("foreign active member accepted")
	}
}

func TestInspectorRecognizesCurrentAndLegacyWorkers(t *testing.T) {
	for _, tc := range []struct {
		client, topic string
		allowed       bool
	}{
		{"kaferry-worker", "orders", true},
		{"kafka-fwd2-worker", "orders", true},
		{"kaferry-worker", "other", false},
		{"kafka-fwd2-worker", "other", false},
		{"another-app", "orders", false},
		{"kaferryx-worker", "orders", false},
	} {
		t.Run(tc.client+"/"+tc.topic, func(t *testing.T) {
			// Given an active member from before or after the rename.
			member := k.MemberDescription{ClientID: tc.client, Assignment: k.MemberAssignment{TopicPartitions: []k.TopicPartition{{Topic: &tc.topic, Partition: 0}}}}
			admin := &adminMock{groups: k.DescribeConsumerGroupsResult{ConsumerGroupDescriptions: []k.ConsumerGroupDescription{{Members: []k.MemberDescription{member}}}}}
			inspector := &Inspector{admin: admin, topic: "orders", log: quietLogger()}

			// When startup checks the group's members.
			err := inspector.CheckMembers("group")

			// Then the rename preserves compatible workers without admitting foreign ones.
			if (err == nil) != tc.allowed {
				t.Fatal("incorrect member compatibility", err)
			}
		})
	}
}
