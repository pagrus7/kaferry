package kafka

import (
	"bytes"
	"errors"
	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pagrus7/kaferry/internal/plan"
	"log/slog"
	"reflect"
	"strings"
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

func TestInspectorRefreshesPartitionsWithoutReplacingTopicIdentity(t *testing.T) {
	for _, scenario := range []string{"growth", "identity changed", "missing topic", "multiple topics", "denied", "RPC failed"} {
		t.Run(scenario, func(t *testing.T) {
			// Given cached metadata and an independently controlled admin response.
			original := k.TopicDescription{Name: "orders", Partitions: []k.TopicPartitionInfo{{Partition: 0}}}
			current := original
			current.Partitions = []k.TopicPartitionInfo{{Partition: 0}, {Partition: 2}}
			admin := &adminMock{described: k.DescribeTopicsResult{TopicDescriptions: []k.TopicDescription{current}}}
			inspector := &Inspector{admin: admin, topic: "orders", description: original, log: quietLogger()}
			rpcFailure := errors.New("describe request failed")
			switch scenario {
			case "identity changed":
				// Kafka's UUID has no exported constructor; simulate losing a cached ID.
				inspector.topicID = "original-topic-id"
			case "missing topic":
				admin.described.TopicDescriptions = nil
			case "multiple topics":
				admin.described.TopicDescriptions = append(admin.described.TopicDescriptions, current)
			case "denied":
				admin.described.TopicDescriptions[0].Error = k.NewError(k.ErrTopicAuthorizationFailed, "denied", false)
			case "RPC failed":
				admin.describeErr = rpcFailure
			}
			identity := inspector.TopicID()

			// When current partitions are requested through the metadata interface.
			var metadata SourceMetadata = inspector
			parts, err := metadata.CurrentPartitions()

			// Then growth is visible; failed checks preserve the cached description and ID.
			if scenario == "growth" {
				if err != nil || len(parts) != 2 || parts[1].Partition != 2 || parts[1].Topic == nil || *parts[1].Topic != "orders" || inspector.PartitionCount() != 2 {
					t.Fatal("partition growth lost", parts, err)
				}
			} else {
				var conflict *Conflict
				if err == nil || parts != nil || !reflect.DeepEqual(inspector.description, original) {
					t.Fatal("failed refresh replaced cached metadata", parts, err)
				}
				if scenario == "RPC failed" {
					if !errors.Is(err, rpcFailure) {
						t.Fatal("RPC failure lost", err)
					}
				} else if !errors.As(err, &conflict) {
					t.Fatal("invalid metadata was not a conflict", err)
				}
			}
			if inspector.TopicID() != identity {
				t.Fatal("source identity changed during refresh")
			}
		})
	}
}

func TestInspectorOptionalConfigurationFallback(t *testing.T) {
	for _, scenario := range []string{"success", "denied", "RPC failed", "missing", "multiple", "wrong topic", "wrong resource type"} {
		t.Run(scenario, func(t *testing.T) {
			// Given an optional configuration response that may be unavailable or malformed.
			resource := k.ConfigResourceResult{Type: k.ResourceTopic, Name: "orders", Config: map[string]k.ConfigEntryResult{"message.timestamp.type": {Value: "CreateTime"}}}
			admin := &adminMock{configs: []k.ConfigResourceResult{resource}}
			switch scenario {
			case "denied":
				admin.configs[0].Error = k.NewError(k.ErrTopicAuthorizationFailed, "unique-secret", false)
			case "RPC failed":
				admin.configErr = errors.New("unique-secret")
			case "missing":
				admin.configs = nil
			case "multiple":
				admin.configs = append(admin.configs, resource)
			case "wrong topic":
				admin.configs[0].Name = "another-topic"
			case "wrong resource type":
				admin.configs[0].Type = k.ResourceBroker
			}
			var output bytes.Buffer
			inspector := &Inspector{admin: admin, topic: "orders", log: slog.New(slog.NewTextHandler(&output, nil))}

			// When a destination asks for optional preflight settings.
			var metadata DestinationMetadata = inspector
			values := metadata.Config()

			// Then valid settings are returned, otherwise preflight falls back with a safe warning.
			if len(admin.configRequest) != 1 || admin.configRequest[0].Type != k.ResourceTopic || admin.configRequest[0].Name != "orders" {
				t.Fatal("wrong configuration request", admin.configRequest)
			}
			if scenario == "success" {
				if !reflect.DeepEqual(values, map[string]string{"message.timestamp.type": "CreateTime"}) || output.Len() != 0 {
					t.Fatal("valid configuration lost", values, output.String())
				}
			} else if values != nil || !strings.Contains(output.String(), "dependent preflight checks disabled") || strings.Contains(output.String(), "unique-secret") {
				t.Fatal("configuration denial did not fall back safely", values, output.String())
			}
		})
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

func TestInspectorRecognizesKaferryWorkers(t *testing.T) {
	for _, tc := range []struct {
		client, topic string
		allowed       bool
	}{
		{"kaferry-worker", "orders", true},
		{"kaferry-worker", "other", false},
		{"another-app", "orders", false},
		{"kaferryx-worker", "orders", false},
	} {
		t.Run(tc.client+"/"+tc.topic, func(t *testing.T) {
			// Given an active member with a protected or foreign client ID.
			member := k.MemberDescription{ClientID: tc.client, Assignment: k.MemberAssignment{TopicPartitions: []k.TopicPartition{{Topic: &tc.topic, Partition: 0}}}}
			admin := &adminMock{groups: k.DescribeConsumerGroupsResult{ConsumerGroupDescriptions: []k.ConsumerGroupDescription{{Members: []k.MemberDescription{member}}}}}
			inspector := &Inspector{admin: admin, topic: "orders", log: quietLogger()}

			// When startup checks the group's members.
			err := inspector.CheckMembers("group")

			// Then only Kaferry workers consuming this topic are accepted.
			if (err == nil) != tc.allowed {
				t.Fatal("incorrect member compatibility", err)
			}
		})
	}
}
