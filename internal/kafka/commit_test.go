package kafka

import (
	"context"
	"errors"
	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	core "github.com/pagrus7/kaferry/internal/copy"
	"testing"
)

func TestEmptyAssignmentDoesNotCommitNativeOffsets(t *testing.T) {
	// Given
	store := &Committer{}

	// When
	results, err := store.Save(context.Background(), nil)

	// Then
	if err != nil || len(results) != 0 {
		t.Fatal("empty assignment needs no native commit", results, err)
	}
}

func TestCommitClassification(t *testing.T) {
	// Given
	var recoverable *core.Recoverable
	for _, code := range []k.ErrorCode{k.ErrRebalanceInProgress, k.ErrIllegalGeneration, k.ErrUnknownMemberID, k.ErrCoordinatorNotAvailable, k.ErrNotCoordinator, k.ErrCoordinatorLoadInProgress, k.ErrRequestTimedOut, k.ErrTransport, k.ErrTimedOut, k.ErrWaitCoord, k.ErrAllBrokersDown} {

		// When
		classifyCommitErr := classifyCommit(k.NewError(code, "", false))

		// Then
		if !errors.As(classifyCommitErr, &recoverable) {
			t.Fatalf("%v should be recoverable on revocation", code)
		}
	}
	for _, err := range []error{
		k.NewError(k.ErrGroupAuthorizationFailed, "", false), k.NewError(k.ErrTopicAuthorizationFailed, "", false),
		k.NewError(k.ErrOffsetMetadataTooLarge, "", false), k.NewError(k.ErrFencedInstanceID, "", false),
		k.NewError(k.ErrTransport, "", true), k.NewError(k.ErrUnknown, "", false), errors.New("not a client error"),
	} {
		if classified := classifyCommit(err); errors.As(classified, &recoverable) {
			t.Fatalf("%v must stay terminal", err)
		}
	}
	if classifyCommit(nil) != nil {
		t.Fatal("success must stay nil")
	}
}

func TestKafkaFileCommitUsesDurableExtent(t *testing.T) {
	// Given
	native := &capturedCommits{}
	output := &durableExtent{extent: 256}
	store := &Committer{consumer: native, topic: "orders", metadata: map[int32]Progress{0: {Fingerprint: "fp", TopicID: "topic-id"}}}
	store.BindOutput(output)
	for _, extent := range []int64{256, 1024, 1024} {
		// Failed I/O cannot move DurableOutput; a later commit keeps the last E.
		output.extent = extent

		// When
		results, err := store.Save(context.Background(), []core.Cursor{{Partition: 0, Position: 5}})

		// Then
		if err != nil || results[0] != nil {
			t.Fatalf("commit: results=%v err=%v", results, err)
		}
		part := native.parts[len(native.parts)-1]
		progress, err := decodeProgress(part.Metadata, "fp", "topic-id", false)
		if err != nil || progress.FileID != "archive-id" || progress.Extent != extent || part.Offset != 5 {
			t.Fatalf("durable output identity/extent changed: part=%v progress=%+v err=%v", part, progress, err)
		}
	}
}

func TestCommitterRejectsCanceledOrForeignOutputBeforeNativeCommit(t *testing.T) {
	for _, scenario := range []string{"canceled", "foreign output"} {
		t.Run(scenario, func(t *testing.T) {
			// Given saved output identity and a canceled query or replacement archive.
			native := &capturedCommits{}
			progress := Progress{Fingerprint: "fp", TopicID: "topic-id", FileID: "archive-id", Extent: 128}
			if scenario == "foreign output" {
				progress.FileID = "another-archive"
			}
			store := &Committer{consumer: native, topic: "orders", output: &durableExtent{extent: 256}, metadata: map[int32]Progress{0: progress}}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if scenario == "canceled" {
				cancel()
			}

			// When attempting to save confirmed progress.
			results, err := store.Save(ctx, []core.Cursor{{Position: 5}})

			// Then no offset or metadata is changed; the cause distinguishes conflicts.
			var conflict *Conflict
			if results != nil || len(native.parts) != 0 || store.metadata[0] != progress || scenario == "canceled" && !errors.Is(err, context.Canceled) || scenario == "foreign output" && !errors.As(err, &conflict) {
				t.Fatal("invalid save reached native commit", results, native.parts, err)
			}
		})
	}
}
