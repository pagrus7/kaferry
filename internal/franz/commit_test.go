package franz

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"
	"testing"

	"github.com/pagrus7/kaferry/internal/commitmeta"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestCommitMetadataUsesTheCapturedProgress(t *testing.T) {
	// Given a confirmed cursor and the metadata encoded for that commit.
	id := [16]byte{1}
	stop := &core.Stop{Reason: "tip", At: 42}
	end := int64(42)
	c := &Committer{codec: commitmeta.NewCodec(), topic: "input", group: "group", topicID: id, metadata: map[int32]kafkaport.Progress{0: {Fingerprint: "fp", TopicID: topicID(id)}}}
	offsets, encoded, err := c.snapshot([]core.Cursor{{Partition: 0, Position: 42, Stop: stop, End: &end}})
	require.NoError(t, err)
	require.EqualValues(t, 42, offsets["input"][0].Offset)
	hook := c.metadataHook(encoded)
	// When state changes before the asynchronous hook runs.
	stop.At = 43
	end = 43
	c.metadata[0] = kafkaport.Progress{Fingerprint: "other"}
	req := &kmsg.OffsetCommitRequest{Group: "group", MemberID: "member", Generation: 3, Topics: []kmsg.OffsetCommitRequestTopic{{TopicID: id, Partitions: []kmsg.OffsetCommitRequestTopicPartition{{Partition: 0, Offset: 42}}}}}
	require.NoError(t, hook(req))
	// Then the ID-only request carries the originally confirmed position and metadata.
	got, err := kafkaport.DecodeProgress(commitmeta.NewCodec(), req.Topics[0].Partitions[0].Metadata, "fp", topicID(id), false)
	require.NoError(t, err)
	require.Equal(t, &core.Stop{Reason: "tip", At: 42}, got.Stop)
	require.Equal(t, new(int64(42)), got.End)
	// When group ownership is lost, then no generationless commit can escape.
	req.Generation = -1
	var recoverable *core.Recoverable
	require.ErrorAs(t, hook(req), &recoverable)
}
func TestCommitterPreservesPartitionErrorsAndRejectsUnownedPositions(t *testing.T) {
	// Given an independently mocked group client and owned progress.
	client := &consumerMock{commitErr: kerr.RebalanceInProgress}
	c := &Committer{codec: commitmeta.NewCodec(), client: client, topic: "input", group: "group", metadata: map[int32]kafkaport.Progress{0: {Fingerprint: "fp", TopicID: "id"}}}
	// When the group changes, then a retryable request error remains distinguishable for revoke handling.
	_, err := c.Save(t.Context(), []core.Cursor{{Partition: 0, Position: 2}})
	var recoverable *core.Recoverable
	if !errors.As(err, &recoverable) || !errors.Is(err, kerr.RebalanceInProgress) {
		t.Fatal("commit error lost", err)
	}
	// When asked to save a lost partition or invalid position, then the client is never called.
	for _, cursor := range []core.Cursor{{Partition: 2, Position: 2}, {Partition: 0, Position: -1}, {Partition: 0, Position: 2, End: new(int64(1))}} {
		client.committed = nil
		if _, err := c.Save(context.Background(), []core.Cursor{cursor}); err == nil || client.committed != nil {
			t.Fatal("unsafe cursor reached broker", cursor, err)
		}
	}
}

func TestCommitterClassifiesUnreachableCoordinatorAndSecurityFailures(t *testing.T) {
	for _, scenario := range []struct {
		failure error
		retry   bool
	}{
		{fmt.Errorf("unable to dial: %w", &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}), true},
		{kerr.GroupAuthorizationFailed, false}, {kerr.SaslAuthenticationFailed, false},
		{&net.OpError{Op: "dial", Err: context.Canceled}, false},
	} {
		t.Run(scenario.failure.Error(), func(t *testing.T) {
			// Given an owned position and a coordinator request that fails.
			client := &consumerMock{commitErr: scenario.failure, commitResponse: &kmsg.OffsetCommitResponse{}}
			c := &Committer{codec: commitmeta.NewCodec(), client: client, topic: "input", metadata: map[int32]kafkaport.Progress{0: {Fingerprint: "fp", TopicID: "id"}}}
			// When saving confirmed progress.
			results, err := c.Save(t.Context(), []core.Cursor{{Partition: 0, Position: 2}})
			// Then only the unavailable coordinator is recoverable; nothing is confirmed.
			require.ErrorIs(t, err, scenario.failure)
			var retry *core.Recoverable
			require.Equal(t, scenario.retry, errors.As(err, &retry), "commit failure classification")
			require.Empty(t, results)
		})
	}
}
func TestCommittedOffsetsMatchTopicIDsWithoutNames(t *testing.T) {
	// Given a Kafka v10 response using topic IDs in place of names.
	id := [16]byte{1}
	i := &Inspector{codec: commitmeta.NewCodec(), topic: "input", topicID: id}
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

type metadataCodecMock struct {
	encoded kafkaport.Progress
	failure error
}

func (c *metadataCodecMock) Encode(p commitmeta.Progress) (string, error) {
	c.encoded = p
	return "encoded", c.failure
}
func (c *metadataCodecMock) Decode(string) (commitmeta.Progress, error) { return c.encoded, c.failure }
func TestCommitterUsesInjectedCodecAndClearsPreviousStop(t *testing.T) {
	// Given a previous stop, and an independently mocked codec and group client.
	codec := &metadataCodecMock{}
	client := &consumerMock{}
	c := &Committer{codec: codec, client: client, topic: "input", metadata: map[int32]kafkaport.Progress{0: {Fingerprint: "fp", TopicID: "id", Stop: &core.Stop{Reason: "tip", At: 7}}}}
	// When the new assignment saves its initial position without a stop.
	if _, _, err := c.snapshot([]core.Cursor{{Position: 7}}); err != nil {
		t.Fatal(err)
	}
	// Then encoding receives a cleared stop at the same position.
	require.Nil(t, codec.encoded.Stop)
	// When encoding fails, then no commit request can leave the component.
	codec.failure = errors.New("codec failure")
	if _, err := c.Save(t.Context(), []core.Cursor{{Position: 7}}); !errors.Is(err, codec.failure) || client.committed != nil {
		t.Fatal(err, client.committed)
	}
	// When a stop lies past the confirmed cursor, then even the codec is bypassed.
	codec.encoded = kafkaport.Progress{}
	if _, _, err := c.snapshot([]core.Cursor{{Position: 7, Stop: &core.Stop{Reason: "tip", At: 8}}}); err == nil || codec.encoded.Fingerprint != "" {
		t.Fatal(err, codec.encoded)
	}
}

func TestCommitterPreservesEachPartitionResultAlongsideRequestFailure(t *testing.T) {
	for _, requestError := range []error{nil, kerr.NotCoordinator} {
		t.Run(fmt.Sprint(requestError), func(t *testing.T) {
			// Given independently successful, recoverable, terminal and omitted partition results.
			client := &consumerMock{commitErr: requestError, commitResponse: &kmsg.OffsetCommitResponse{Topics: []kmsg.OffsetCommitResponseTopic{
				{Topic: "other", Partitions: []kmsg.OffsetCommitResponseTopicPartition{{Partition: 99}}},
				{Topic: "input", Partitions: []kmsg.OffsetCommitResponseTopicPartition{{Partition: 0}, {Partition: 1, ErrorCode: kerr.RebalanceInProgress.Code}, {Partition: 2, ErrorCode: kerr.TopicAuthorizationFailed.Code}}},
			}}}
			metadata := map[int32]kafkaport.Progress{}
			var cursors []core.Cursor
			for id := range int32(4) {
				metadata[id] = kafkaport.Progress{Fingerprint: "fp", TopicID: "id"}
				cursors = append(cursors, core.Cursor{Partition: id, Position: 42})
			}
			committer := &Committer{codec: commitmeta.NewCodec(), client: client, topic: "input", metadata: metadata}
			// When saving the positions, then request classification does not replace individual outcomes.
			results, err := committer.Save(t.Context(), cursors)
			require.ErrorIs(t, err, requestError)
			if requestError != nil {
				var retry *core.Recoverable
				require.ErrorAs(t, err, &retry)
			}
			require.Len(t, results, 3)
			require.Contains(t, results, int32(0))
			require.NoError(t, results[0])
			require.ErrorIs(t, results[1], kerr.RebalanceInProgress)
			var retry *core.Recoverable
			require.ErrorAs(t, results[1], &retry)
			require.ErrorIs(t, results[2], kerr.TopicAuthorizationFailed)
			require.False(t, errors.As(results[2], &retry))
			require.NotContains(t, results, int32(3))
			require.NotContains(t, results, int32(99))
		})
	}
}
