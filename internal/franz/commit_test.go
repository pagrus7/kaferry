package franz

import (
	"context"
	"errors"
	"testing"

	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type outputProgressMock struct {
	id     string
	extent int64
}

func (o *outputProgressMock) DurableOutput() (string, int64) { return o.id, o.extent }
func TestCommitMetadataUsesTheCapturedDurableFileSnapshot(t *testing.T) {
	// Given a confirmed source cursor and a durable output position.
	id := [16]byte{1}
	output := &outputProgressMock{id: "archive", extent: 200}
	c := &Committer{topic: "input", group: "group", topicID: id, metadata: map[int32]kafkaport.Progress{0: {Fingerprint: "fp", TopicID: topicID(id)}}, output: output}
	offsets, encoded, err := c.snapshot([]core.Cursor{{Partition: 0, Position: 42}})
	if err != nil || offsets["input"][0].Offset != 42 {
		t.Fatal(err, offsets)
	}
	hook := c.metadataHook(encoded)
	// When the output and source state change before the asynchronous hook runs.
	output.extent = 500
	c.metadata[0] = kafkaport.Progress{Fingerprint: "other"}
	req := &kmsg.OffsetCommitRequest{Group: "group", MemberID: "member", Generation: 3, Topics: []kmsg.OffsetCommitRequestTopic{{TopicID: id, Partitions: []kmsg.OffsetCommitRequestTopicPartition{{Partition: 0, Offset: 42}}}}}
	if err := hook(req); err != nil {
		t.Fatal(err)
	}
	// Then the v10 ID-only request carries the original fsync-confirmed extent and identity.
	got, err := kafkaport.DecodeProgress(req.Topics[0].Partitions[0].Metadata, "fp", topicID(id), false)
	if err != nil || got.FileID != "archive" || got.Extent != 200 {
		t.Fatal("hook read mutable state after snapshot", got, err)
	}
	// When the client has lost its generation, then no generationless admin commit can escape.
	req.Generation = -1
	var recoverable *core.Recoverable
	if err := hook(req); !errors.As(err, &recoverable) {
		t.Fatal("unfenced commit accepted", err)
	}
}
func TestCommitterPreservesPartitionErrorsAndRejectsUnownedPositions(t *testing.T) {
	// Given an independently mocked group client and owned progress.
	client := &consumerMock{commitErr: kerr.RebalanceInProgress}
	c := &Committer{client: client, topic: "input", group: "group", metadata: map[int32]kafkaport.Progress{0: {Fingerprint: "fp", TopicID: "id"}}}
	// When the group changes, then a retryable request error remains distinguishable for revoke handling.
	_, err := c.Save(t.Context(), []core.Cursor{{Partition: 0, Position: 2}})
	var recoverable *core.Recoverable
	if !errors.As(err, &recoverable) || !errors.Is(err, kerr.RebalanceInProgress) {
		t.Fatal("commit error lost", err)
	}
	// When asked to save a lost partition or invalid position, then the client is never called.
	for _, cursor := range []core.Cursor{{Partition: 2, Position: 2}, {Partition: 0, Position: -1}} {
		client.committed = nil
		if _, err := c.Save(context.Background(), []core.Cursor{cursor}); err == nil || client.committed != nil {
			t.Fatal("unsafe cursor reached broker", cursor, err)
		}
	}
}
func TestCommittedOffsetsMatchTopicIDsWithoutNames(t *testing.T) {
	// Given a Kafka v10 response using topic IDs in place of names.
	id := [16]byte{1}
	i := &Inspector{topic: "input", topicID: id}
	r := &kmsg.OffsetFetchResponse{Groups: []kmsg.OffsetFetchResponseGroup{{Group: "group", Topics: []kmsg.OffsetFetchResponseGroupTopic{{TopicID: id, Partitions: []kmsg.OffsetFetchResponseGroupTopicPartition{{Partition: 0, Offset: 42}}}}}}}
	// When reading the response, then it is matched by the expected identity.
	parts, err := i.offsetParts(r, "group")
	if err != nil || parts[0].Offset != 42 {
		t.Fatal("ID-only offsets lost", parts, err)
	}
	// When a different topic reuses the name, then its ID cannot match the old source.
	if sameTopic("input", [16]byte{2}, id, "input") {
		t.Fatal("different topic identity accepted by name")
	}
}
