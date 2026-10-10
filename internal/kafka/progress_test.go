package kafka

import (
	"errors"
	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pagrus7/kaferry/internal/plan"
	"math"
	"reflect"
	"testing"
)

func TestAssignmentReadsCommittedMetadataThroughConsumer(t *testing.T) {
	for _, scenario := range []string{"valid", "query failure", "partition failure", "missing result", "foreign metadata"} {
		t.Run(scenario, func(t *testing.T) {
			// Given discovered source partitions and independently mocked consumer progress.
			name := "orders"
			metadata := (Progress{Fingerprint: "fp", TopicID: "topic-id"}).Encode()
			request := []k.TopicPartition{{Topic: &name, Partition: 0}, {Topic: &name, Partition: 1}}
			consumer := &consumerMock{committed: []k.TopicPartition{{Topic: &name, Partition: 0, Offset: 42, Metadata: &metadata}, {Topic: &name, Partition: 1, Offset: k.OffsetInvalid}}}
			cause := errors.New("injected fetch failure")
			switch scenario {
			case "query failure":
				consumer.committedErr = cause
			case "partition failure":
				consumer.committed[0].Error = cause
			case "missing result":
				consumer.committed = consumer.committed[:1]
			case "foreign metadata":
				bad := "foreign"
				consumer.committed[0].Metadata = &bad
			}
			source := &Source{consumer: consumer, inspector: &sourceMetadataMock{parts: request}, fingerprint: "fp", plan: plan.Plan{Source: plan.Endpoint{Name: name}}}

			// When an assignment reads group progress, including an unowned partition.
			saved, progress, err := source.saved()

			// Then the consumer returns metadata without a coordinator-targeted admin call.
			if !reflect.DeepEqual(consumer.committedRequests, request) || consumer.committedTimeout != 10000 {
				t.Fatal("consumer did not query every source partition", consumer.committedRequests, consumer.committedTimeout)
			}
			if scenario == "valid" {
				if err != nil || len(saved) != 1 || saved[0].Offset != 42 || progress[0].Fingerprint != "fp" {
					t.Fatal("committed metadata lost", saved, progress, err)
				}
			} else if err == nil || (scenario == "query failure" || scenario == "partition failure") && !errors.Is(err, cause) {
				t.Fatal("invalid progress accepted or cause lost", err)
			}
		})
	}
}

func TestReleasedProgressSurvivesRename(t *testing.T) {
	// Given committed metadata in the released v1 format.
	metadata := `kfwd2:1;{"fp":"fp","tid":"topic-id"}`

	// When Kaferry reads and writes that progress.
	progress, err := decodeProgress(&metadata, "fp", "topic-id", false)

	// Then the persisted identity and wire format remain unchanged.
	if err != nil || progress.Encode() != metadata {
		t.Fatal("released Kafka progress changed", progress, err)
	}
}

func TestInvalidProgressNeverBecomesAStartFallback(t *testing.T) {
	// Given
	valid := Progress{Fingerprint: "fingerprint", TopicID: "topic"}.Encode()

	// When
	_, decodeProgressErr := decodeProgress(&valid, "fingerprint", "topic", false)

	// Then
	if decodeProgressErr != nil {
		t.Fatal(decodeProgressErr)
	}
	for _, text := range []string{"", "foreign", valid + "{}", `kfwd2:1;{"fp":"fingerprint","tid":"topic","unknown":1}`, Progress{Fingerprint: "other", TopicID: "topic"}.Encode(), Progress{Fingerprint: "fingerprint", TopicID: "recreated"}.Encode(), Progress{Fingerprint: "fingerprint", TopicID: "topic", FileID: "file", Extent: -1}.Encode()} {
		if _, err := decodeProgress(&text, "fingerprint", "topic", false); err == nil {
			t.Fatalf("accepted invalid progress: %s", text)
		}
	}
	if _, err := decodeProgress(&valid, "fingerprint", "topic", true); err == nil {
		t.Fatal("relative query accepted missing window")
	}
}

func TestSourceValidatesProgressThroughMetadataInterface(t *testing.T) {
	// Given
	valid := Progress{Fingerprint: "fp", TopicID: "topic-id"}.Encode()
	metadata := &sourceMetadataMock{parts: []k.TopicPartition{{Partition: 0, Offset: 10, Metadata: &valid}, {Partition: 2, Offset: k.OffsetInvalid}}}
	source := &Source{consumer: &consumerMock{committed: metadata.parts}, inspector: metadata, fingerprint: "fp", plan: plan.Plan{Group: "group"}}

	// When
	positions, progress, err := source.saved()

	// Then
	if err != nil || len(progress) != 1 || len(positions) != 1 || positions[0].Offset != 10 {
		t.Fatal("valid progress lost", progress, err)
	}
	foreign := "foreign"
	metadata.parts[0].Metadata = &foreign
	if _, _, err := source.saved(); err == nil {
		t.Fatal("foreign progress became a start fallback")
	}
}

func TestCommittedProgressValidationAtAssignment(t *testing.T) {
	for _, scenario := range []string{"valid", "absent", "partition error", "foreign metadata", "fingerprint", "topic identity", "file extent", "missing relative window", "window shape", "literal mismatch", "cutoff overflow"} {
		t.Run(scenario, func(t *testing.T) {
			// Given the same broker commits read by startup and by an assigned consumer.
			metadataSource := &sourceMetadataMock{parts: []k.TopicPartition{{Partition: 0}, {Partition: 1}}}
			progress := Progress{Fingerprint: "fp", TopicID: metadataSource.TopicID()}
			var spec plan.WindowSpec
			var cutoff int64
			lo := int64(10)
			switch scenario {
			case "fingerprint":
				progress.Fingerprint = "other"
			case "topic identity":
				progress.TopicID = "recreated"
			case "file extent":
				progress.FileID, progress.Extent = "archive", -1
			case "missing relative window":
				spec.Lower = &plan.Expression{Now: true}
			case "window shape":
				progress.Window = &plan.Window{Lower: &lo}
			case "literal mismatch":
				progress.Window = &plan.Window{Lower: &lo}
				spec.Lower = &plan.Expression{LiteralMS: lo + 1}
			case "cutoff overflow":
				hi := int64(math.MaxInt64)
				progress.Window = &plan.Window{Upper: &hi}
				spec.Upper = &plan.Expression{LiteralMS: hi}
				cutoff = 1
			}
			metadata := progress.Encode()
			parts := []k.TopicPartition{{Partition: 0, Offset: 42, Metadata: &metadata}, {Partition: 1, Offset: k.OffsetInvalid}}
			cause := errors.New("partition unavailable")
			switch scenario {
			case "absent":
				parts[0].Offset = k.OffsetInvalid
			case "foreign metadata":
				metadata = "foreign"
			case "partition error":
				parts[0].Error = cause
			}
			source := &Source{consumer: &consumerMock{committed: parts}, inspector: metadataSource, fingerprint: "fp", plan: plan.Plan{Window: spec, CutoffMS: cutoff}}

			// When both paths validate saved query identity and time windows.
			_, startup, startupErr := decodeCommitted(parts, "fp", metadataSource.TopicID(), spec, cutoff)
			_, assigned, assignmentErr := source.saved()

			// Then neither path replaces invalid progress with a fresh starting rule.
			valid := scenario == "valid" || scenario == "absent"
			if (startupErr == nil) != valid || (assignmentErr == nil) != valid {
				t.Fatalf("startup=%v assignment=%v", startupErr, assignmentErr)
			}
			wantCount := 0
			if scenario == "valid" {
				wantCount = 1
			}
			if len(startup) != wantCount || len(assigned) != wantCount {
				t.Fatal("saved progress differs", startup, assigned)
			}
			if scenario == "partition error" && (!errors.Is(startupErr, cause) || !errors.Is(assignmentErr, cause)) {
				t.Fatal("partition failure lost", startupErr, assignmentErr)
			}
		})
	}
}

func TestStartupProgressDoesNotSilentlyReplaceInvalidMetadata(t *testing.T) {
	for _, scenario := range []string{"absent", "foreign", "partition failure"} {
		t.Run(scenario, func(t *testing.T) {
			// Given startup metadata returned by an independently mocked admin transport.
			description := k.TopicDescription{Name: "orders", Partitions: []k.TopicPartitionInfo{{Partition: 0}}}
			topic := "orders"
			metadata := "foreign"
			part := k.TopicPartition{Topic: &topic, Partition: 0, Offset: 42, Metadata: &metadata}
			cause := errors.New("partition unavailable")
			if scenario == "absent" {
				part.Offset = k.OffsetInvalid
			}
			if scenario == "partition failure" {
				part.Error = cause
			}
			admin := &adminMock{described: k.DescribeTopicsResult{TopicDescriptions: []k.TopicDescription{description}}, groupOffsets: k.ListConsumerGroupOffsetsResult{ConsumerGroupsTopicPartitions: []k.ConsumerGroupTopicPartitions{{Partitions: []k.TopicPartition{part}}}}}
			inspector := &Inspector{admin: admin, topic: topic, description: description, log: quietLogger()}

			// When startup reads the query's saved progress.
			progress, err := inspector.ReadProgress("group", "fp", plan.WindowSpec{}, 0)

			// Then absent commits permit initialization; invalid metadata and transport failures stop it.
			if (err == nil) != (scenario == "absent") || len(progress) != 0 {
				t.Fatal("invalid startup decision", progress, err)
			}
			if scenario == "partition failure" && !errors.Is(err, cause) {
				t.Fatal("partition failure lost", err)
			}
		})
	}
}
