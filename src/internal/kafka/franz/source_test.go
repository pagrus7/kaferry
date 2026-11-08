package franz

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	core "github.com/pagrus7/kaferry/src/internal/copy"
	"github.com/pagrus7/kaferry/src/internal/kafka"
	"github.com/pagrus7/kaferry/src/internal/kafka/commitmeta"
	"github.com/pagrus7/kaferry/src/internal/plan"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type consumerMock struct {
	ConsumerClient
	commitErr       error
	commitResponse  *kmsg.OffsetCommitResponse
	committed       map[int32]int64
	paused, resumed []int32
}

func (c *consumerMock) PauseFetchPartitions(parts map[string][]int32) map[string][]int32 {
	c.paused = append(c.paused, parts["input"]...)
	return parts
}
func (c *consumerMock) ResumeFetchPartitions(parts map[string][]int32) {
	c.resumed = append(c.resumed, parts["input"]...)
}
func (c *consumerMock) CommitOffsetsSync(_ context.Context, offsets map[string]map[int32]kgo.EpochOffset, callback func(*kgo.Client, *kmsg.OffsetCommitRequest, *kmsg.OffsetCommitResponse, error)) {
	c.committed = make(map[int32]int64)
	response := &kmsg.OffsetCommitResponse{Topics: []kmsg.OffsetCommitResponseTopic{{Topic: "input"}}}
	for id, offset := range offsets["input"] {
		c.committed[id] = offset.Offset
		response.Topics[0].Partitions = append(response.Topics[0].Partitions, kmsg.OffsetCommitResponseTopicPartition{Partition: id})
	}
	if c.commitResponse != nil {
		response = c.commitResponse
	}
	callback(nil, nil, response, c.commitErr)
}

type sourceMetadataMock struct {
	low, high, stable map[int32]int64
	saved             map[int32]kafka.SavedProgress
	err               error
	savedReads        int
}

func (m *sourceMetadataMock) Offsets(_ context.Context, ids []int32, kind kafka.OffsetKind, committed bool) (map[int32]int64, error) {
	values := m.high
	if kind == kafka.EarliestOffset {
		values = m.low
	}
	if committed {
		values = m.stable
	}
	result := make(map[int32]int64, len(ids))
	for _, id := range ids {
		result[id] = values[id]
	}
	return result, m.err
}
func (m *sourceMetadataMock) OffsetsForTimes(context.Context, map[int32]int64) (map[int32]int64, error) {
	return map[int32]int64{0: 0}, m.err
}
func (m *sourceMetadataMock) Saved(context.Context, string, string, plan.WindowSpec, int64) (map[int32]kafka.SavedProgress, error) {
	m.savedReads++
	return m.saved, m.err
}

type lifecycleMock struct {
	assignments []core.Assignment
	revoked     []int32
	lost        bool
}

func (l *lifecycleMock) Assign(a []core.Assignment) error {
	l.assignments = append(l.assignments, a...)
	return nil
}
func (l *lifecycleMock) Revoke(ids []int32, lost bool) error {
	l.revoked = append(l.revoked, ids...)
	l.lost = lost
	return nil
}
func sourceFixture(t *testing.T) (*Source, *consumerMock) {
	t.Helper()
	client := &consumerMock{}
	p := testPlan(t, nil)
	store := &Committer{codec: commitmeta.NewCodec(), client: client, topic: "input", group: p.Group, metadata: map[int32]kafka.Progress{0: {Fingerprint: "fp", TopicID: "id"}}}
	metadata := &sourceMetadataMock{}
	s := &Source{lookup: kafka.NewOffsetLookupFactory().New(metadata, metadata), client: client, inspector: metadata, positioning: kafka.NewPositionerFactory(core.RealClock{}, quietLogger(), kafka.NewOffsetLookupFactory()), store: store, bridge: newRebalanceBridge("input"), pump: &fetchPump{fetched: make(chan fetchResult, 1)}, plan: p, fingerprint: "fp", topicID: "id", clientCancel: func() {}, life: &lifecycleMock{}, partitions: map[int32]*sourcePartition{0: {}}, log: quietLogger(), lastTip: time.Now()}
	return s, client
}
func feed(s *Source, record *kgo.Record, err error) {
	s.pump.fetched <- fetchResult{records: []*kgo.Record{record}, err: err, done: make(chan struct{})}
}

func TestFetchReportsTerminalErrorsEvenBesideRetryableOnes(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(map[bool]string{false: "retry first", true: "terminal first"}[reverse], func(t *testing.T) {
			// Given one response containing errors for multiple partitions.
			parts := []kgo.FetchPartition{{Partition: 0, Err: kerr.NotLeaderForPartition}, {Partition: 2, Err: kerr.TopicAuthorizationFailed}}
			if reverse {
				parts[0], parts[1] = parts[1], parts[0]
			}
			fetches := kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: "input", Partitions: parts}}}}
			// When selecting the error to surface, then a retryable error cannot hide an authorization failure.
			if err := fetchFailure(fetches); !errors.Is(err, kerr.TopicAuthorizationFailed) {
				t.Fatal("terminal partition error hidden", err)
			}
		})
	}
}

func TestExcludedAssignmentCannotPositionOrCommit(t *testing.T) {
	// Given a selected source and an invalid excluded assignment.
	s, client := sourceFixture(t)
	s.plan.Partitions = &plan.PartitionFilter{IDs: []int32{2}}
	answer := make(chan lifecycleAnswer, 1)
	// When handling the callback, then reject before reading progress or saving initial offsets.
	err := s.lifecycle(context.Background(), lifecycleEvent{ids: []int32{0}, answer: answer})
	require.ErrorContains(t, err, "excluded source partition")
	require.Error(t, (<-answer).err)
	require.Zero(t, s.inspector.(*sourceMetadataMock).savedReads)
	require.Empty(t, client.committed)
}

func TestSourcePreservesRecordBesideTransientFetchError(t *testing.T) {
	// Given a fetch with a retryable error for one partition and a valid record for another.
	s, _ := sourceFixture(t)
	record := &kgo.Record{Topic: "input", Partition: 0, Offset: 0, Key: []byte{}, Value: nil, Timestamp: time.UnixMilli(0), Headers: []kgo.RecordHeader{{Key: "a\x00b", Value: nil}, {Key: "a\x00b", Value: []byte{}}}}
	feed(s, record, kerr.NotLeaderForPartition)
	// When polling, then the valid record is delivered even though the client's cursor already passed it.
	event, err := s.Poll(context.Background(), time.Second)
	if err != nil || event.Input == nil || event.Input.Next != 1 || event.Input.Record.Key == nil || event.Input.Record.Value != nil || *event.Input.Record.TimestampMS != 0 || !reflect.DeepEqual(event.Input.Record.Headers, []core.Header{{Name: "a\x00b", Value: nil}, {Name: "a\x00b", Value: []byte{}}}) {
		t.Fatal("mixed fetch lost or changed its record", event, err)
	}
}
func TestSourceHoldsPausedRecordWhileOtherPartitionsContinue(t *testing.T) {
	// Given a buffered record from a just-paused partition.
	s, client := sourceFixture(t)
	s.partitions[2] = &sourcePartition{}
	require.NoError(t, s.Pause(0, true))
	feed(s, &kgo.Record{Partition: 0, Offset: 0, Value: []byte("held")}, nil)
	if event, err := s.Poll(context.Background(), time.Second); err != nil || event.Input != nil {
		t.Fatal("paused record delivered", event, err)
	}
	// When another partition has a record, then it continues despite the held record.
	feed(s, &kgo.Record{Partition: 2, Offset: 0, Value: []byte("other")}, nil)
	if event, err := s.Poll(context.Background(), time.Second); err != nil || event.Input == nil || event.Input.Cursor.Partition != 2 {
		t.Fatal("held partition blocked another", event, err)
	}
	// When resuming, then the held record is delivered first and pause calls target only its partition.
	require.NoError(t, s.Pause(0, false))
	event, err := s.Poll(context.Background(), time.Second)
	if err != nil || event.Input == nil || string(event.Input.Record.Value) != "held" || !reflect.DeepEqual(client.paused, []int32{0}) || !reflect.DeepEqual(client.resumed, []int32{0}) {
		t.Fatal("resume lost held record", event, err, client)
	}
}
func TestSourceRevokesOnlyNamedPartitionsAndTreatsLostSeparately(t *testing.T) {
	// Given paused records in two owned partitions.
	s, client := sourceFixture(t)
	s.store.assign(2, kafka.Progress{Fingerprint: "fp", TopicID: "id"})
	s.partitions[2] = &sourcePartition{position: 8}
	s.partitions[0].paused = true
	s.partitions[2].paused = true
	s.partitions[0].held = &kgo.Record{Partition: 0}
	s.partitions[2].held = &kgo.Record{Partition: 2}
	// When partition zero is lost, then only its generation's state is removed.
	event := lifecycleEvent{ids: []int32{0}, revoke: true, lost: true, answer: make(chan lifecycleAnswer, 1)}
	require.NoError(t, s.lifecycle(context.Background(), event))
	life := s.life.(*lifecycleMock)
	if !life.lost || !reflect.DeepEqual(life.revoked, []int32{0}) || len(s.partitions) != 1 || s.partitions[2].position != 8 || s.partitions[0] != nil || s.partitions[2].held == nil || !s.partitions[2].paused {
		t.Fatal("revocation crossed partition boundaries", s.partitions, life)
	}
	// When saving after revocation, the released partition is rejected before I/O.
	_, err := s.store.Save(t.Context(), []core.Cursor{{Partition: 0, Position: 8}})
	require.ErrorContains(t, err, "unowned partition 0")
	require.Empty(t, client.committed)
	// Then the remaining partition can still save its confirmed position.
	results, err := s.store.Save(t.Context(), []core.Cursor{{Partition: 2, Position: 8}})
	require.NoError(t, err)
	require.Equal(t, map[int32]error{2: nil}, results)
	require.Equal(t, map[int32]int64{2: 8}, client.committed)
}

func TestSourceCommitsResolvedStartBeforeExposingAssignment(t *testing.T) {
	failure := errors.New("initial commit failed")
	for _, tc := range []struct {
		name         string
		requestError error
		response     *kmsg.OffsetCommitResponse
		wantError    string
		cause        error
	}{
		{name: "committed"},
		{name: "request failed", requestError: failure, cause: failure},
		{name: "omitted partition", response: &kmsg.OffsetCommitResponse{}, wantError: "missing initial commit result for partition 0"},
		{name: "partition authorization", response: &kmsg.OffsetCommitResponse{Topics: []kmsg.OffsetCommitResponseTopic{{Topic: "input", Partitions: []kmsg.OffsetCommitResponseTopicPartition{{Partition: 0, ErrorCode: kerr.TopicAuthorizationFailed.Code}}}}}, cause: kerr.TopicAuthorizationFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given an assignment starting at saved offset seven and an independent commit response.
			s, client := sourceFixture(t)
			s.inspector = &sourceMetadataMock{low: map[int32]int64{0: 0}, high: map[int32]int64{0: 10}, saved: map[int32]kafka.SavedProgress{0: {Position: 7, Progress: kafka.Progress{Fingerprint: "fp", TopicID: "id"}}}}
			client.commitErr, client.commitResponse = tc.requestError, tc.response
			event := lifecycleEvent{ids: []int32{0}, answer: make(chan lifecycleAnswer, 1)}
			// When the core resolves and saves its initial position.
			err := s.lifecycle(context.Background(), event)
			// Then the core sees the assignment only after an explicit successful partition result.
			require.Equal(t, map[int32]int64{0: 7}, client.committed)
			life := s.life.(*lifecycleMock)
			if tc.cause != nil || tc.wantError != "" {
				require.Error(t, err)
				if tc.cause != nil {
					require.ErrorIs(t, err, tc.cause)
				}
				if tc.wantError != "" {
					require.EqualError(t, err, tc.wantError)
				}
				require.Empty(t, life.assignments)
			} else {
				require.NoError(t, err)
				require.Len(t, life.assignments, 1)
				require.Equal(t, int64(7), life.assignments[0].Cursor.Position)
			}
		})
	}
}

func TestSourceDropsStaleRecordsAndRecognizesMissingTimestamps(t *testing.T) {
	// Given a proven compaction gap ending at offset ten.
	s, _ := sourceFixture(t)
	s.partitions[0].position = 10
	// When an older fetch arrives, then it cannot reintroduce a record below the proof cursor.
	event, err := s.deliver(&kgo.Record{Partition: 0, Offset: 3})
	if err != nil || event.Input != nil || s.partitions[0].position != 10 {
		t.Fatal("stale record admitted", event, err)
	}
	// When Kafka's missing-timestamp sentinel arrives, then policy handling sees a missing timestamp.
	event, err = s.deliver(&kgo.Record{Partition: 0, Offset: 10, Timestamp: time.UnixMilli(-1)})
	if err != nil || event.Input == nil || event.Input.Record.TimestampMS != nil || event.Input.Record.TimestampType != "unknown" {
		t.Fatal("missing timestamp treated as an event time", event, err)
	}
}

type routingMock struct {
	calls int
	ids   []int32
	err   error
}

func (m *routingMock) ValidatePartitions(_ context.Context, ids []int32) error {
	m.calls++
	m.ids = append([]int32{}, ids...)
	return m.err
}

func TestAssignmentRejectsNewMissingPartitionBeforeInitialCommit(t *testing.T) {
	// Given a rebalance adding a partition absent from the destination.
	s, client := sourceFixture(t)
	failure := errors.New("destination has no partition 5")
	routing := &routingMock{err: failure}
	s.routing = routing
	event := lifecycleEvent{ids: []int32{5}, answer: make(chan lifecycleAnswer, 1)}
	// When processing the assignment, then the callback receives the failure before any initial commit or assignment.
	require.ErrorIs(t, s.lifecycle(context.Background(), event), failure)
	select {
	case answer := <-event.answer:
		require.ErrorIs(t, answer.err, failure)
	case <-time.After(time.Second):
		t.Fatal("assignment error was not returned to callback")
	}
	require.Equal(t, []int32{5}, routing.ids)
	require.Nil(t, client.committed)
	require.Zero(t, s.inspector.(*sourceMetadataMock).savedReads)
	require.Empty(t, s.life.(*lifecycleMock).assignments)
	require.NotContains(t, s.partitions, int32(5))
}

func TestEmptyAssignmentDoesNotInspectDestination(t *testing.T) {
	// Given a rebalance notification with no newly assigned partitions.
	s, _ := sourceFixture(t)
	routing := &routingMock{err: errors.New("destination unavailable")}
	s.routing = routing
	event := lifecycleEvent{answer: make(chan lifecycleAnswer, 1)}
	// When processing it, then an idle worker needs no destination request and still receives its assignment result.
	require.NoError(t, s.lifecycle(context.Background(), event))
	require.Zero(t, routing.calls)
	select {
	case answer := <-event.answer:
		require.NoError(t, answer.err)
		require.Empty(t, answer.assignments)
	case <-time.After(time.Second):
		t.Fatal("empty assignment was not returned to callback")
	}
}

func TestPollReturnsTerminalFetchErrorWithoutDelivery(t *testing.T) {
	// Given a terminal broker failure in the fetch pump.
	source, _ := sourceFixture(t)
	done := make(chan struct{})
	source.pump.fetched <- fetchResult{err: kerr.TopicAuthorizationFailed, done: done}
	// When polling, then return the same failure without producing an input event.
	event, err := source.Poll(t.Context(), time.Second)
	require.Same(t, kerr.TopicAuthorizationFailed, err)
	require.Empty(t, event)
	select {
	case <-done:
	default:
		t.Fatal("terminal fetch ownership was not released")
	}
}
