package franz

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type consumerMock struct {
	ConsumerClient
	commitErr       error
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
	callback(nil, nil, response, c.commitErr)
}

type sourceMetadataMock struct {
	low, high, stable map[int32]int64
	saved             map[int32]int64
	progress          map[int32]kafkaport.Progress
	err               error
}

func (m *sourceMetadataMock) Offsets(_ context.Context, ids []int32, kind kafkaport.OffsetKind, committed bool) (map[int32]int64, error) {
	values := m.high
	if kind == kafkaport.EarliestOffset {
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
func (m *sourceMetadataMock) Saved(context.Context, string, string, plan.WindowSpec, int64) (map[int32]int64, map[int32]kafkaport.Progress, error) {
	return m.saved, m.progress, m.err
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
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	client := &consumerMock{}
	p := testPlan(t, nil)
	store := &Committer{client: client, topic: "input", group: p.Group, metadata: map[int32]kafkaport.Progress{0: {Fingerprint: "fp", TopicID: "id"}}}
	s := &Source{client: client, inspector: &sourceMetadataMock{}, positioning: kafkaport.NewPositionerFactory(core.RealClock{}, quietLogger()), store: store, bridge: newRebalanceBridge(ctx, "input"), pump: &fetchPump{fetched: make(chan fetchResult, 1)}, plan: p, fingerprint: "fp", topicID: "id", query: ctx, ctx: ctx, cancel: cancel, clientCancel: func() {}, life: &lifecycleMock{}, positions: map[int32]int64{0: 0}, paused: make(map[int32]bool), held: make(map[int32]*kgo.Record), retention: make(map[int32]kafkaport.RetentionCheck), log: quietLogger(), lastTip: time.Now()}
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

func TestSourcePreservesRecordBesideTransientFetchError(t *testing.T) {
	// Given a fetch with a retryable error for one partition and a valid record for another.
	s, _ := sourceFixture(t)
	record := &kgo.Record{Topic: "input", Partition: 0, Offset: 0, Key: []byte{}, Value: nil, Timestamp: time.UnixMilli(0), Headers: []kgo.RecordHeader{{Key: "a\x00b", Value: nil}, {Key: "a\x00b", Value: []byte{}}}}
	feed(s, record, kerr.NotLeaderForPartition)
	// When polling, then the valid record is delivered even though the client's cursor already passed it.
	event, err := s.Poll(time.Second)
	if err != nil || event.Input == nil || event.Input.Next != 1 || event.Input.Record.Key == nil || event.Input.Record.Value != nil || *event.Input.Record.TimestampMS != 0 || !reflect.DeepEqual(event.Input.Record.Headers, []core.Header{{Name: "a\x00b", Value: nil}, {Name: "a\x00b", Value: []byte{}}}) {
		t.Fatal("mixed fetch lost or changed its record", event, err)
	}
}
func TestSourceHoldsPausedRecordWhileOtherPartitionsContinue(t *testing.T) {
	// Given a buffered record from a just-paused partition.
	s, client := sourceFixture(t)
	s.positions[2] = 0
	if err := s.Pause(0, true); err != nil {
		t.Fatal(err)
	}
	feed(s, &kgo.Record{Partition: 0, Offset: 0, Value: []byte("held")}, nil)
	if event, err := s.Poll(time.Second); err != nil || event.Input != nil {
		t.Fatal("paused record delivered", event, err)
	}
	// When another partition has a record, then it continues despite the held record.
	feed(s, &kgo.Record{Partition: 2, Offset: 0, Value: []byte("other")}, nil)
	if event, err := s.Poll(time.Second); err != nil || event.Input == nil || event.Input.Cursor.Partition != 2 {
		t.Fatal("held partition blocked another", event, err)
	}
	// When resuming, then the held record is delivered first and pause calls target only its partition.
	if err := s.Pause(0, false); err != nil {
		t.Fatal(err)
	}
	event, err := s.Poll(time.Second)
	if err != nil || event.Input == nil || string(event.Input.Record.Value) != "held" || !reflect.DeepEqual(client.paused, []int32{0}) || !reflect.DeepEqual(client.resumed, []int32{0}) {
		t.Fatal("resume lost held record", event, err, client)
	}
}
func TestSourceRevokesOnlyNamedPartitionsAndTreatsLostSeparately(t *testing.T) {
	// Given paused records in two owned partitions.
	s, _ := sourceFixture(t)
	s.positions[2] = 8
	s.paused[0] = true
	s.paused[2] = true
	s.held[0] = &kgo.Record{Partition: 0}
	s.held[2] = &kgo.Record{Partition: 2}
	// When partition zero is lost, then only its generation's state is removed.
	event := lifecycleEvent{ids: []int32{0}, revoke: true, lost: true, answer: make(chan lifecycleAnswer, 1)}
	if err := s.lifecycle(event); err != nil {
		t.Fatal(err)
	}
	life := s.life.(*lifecycleMock)
	if !life.lost || !reflect.DeepEqual(life.revoked, []int32{0}) || len(s.positions) != 1 || s.positions[2] != 8 || s.held[0] != nil || s.held[2] == nil || !s.paused[2] {
		t.Fatal("revocation crossed partition boundaries", s.positions, s.held, life)
	}
}
func TestSourceCommitsResolvedStartBeforeExposingAssignment(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "committed", true: "failed"}[fail], func(t *testing.T) {
			// Given an assignment starting at saved offset seven.
			s, client := sourceFixture(t)
			s.inspector = &sourceMetadataMock{low: map[int32]int64{0: 0}, high: map[int32]int64{0: 10}, saved: map[int32]int64{0: 7}, progress: map[int32]kafkaport.Progress{0: {Fingerprint: "fp", TopicID: "id"}}}
			failure := errors.New("initial commit failed")
			if fail {
				client.commitErr = failure
			}
			event := lifecycleEvent{ids: []int32{0}, answer: make(chan lifecycleAnswer, 1)}
			// When the core resolves and saves its initial position.
			err := s.lifecycle(event)
			// Then the core sees the assignment only after a successful initial commit.
			life := s.life.(*lifecycleMock)
			if (err != nil) != fail || fail && !errors.Is(err, failure) || client.committed[0] != 7 || (len(life.assignments) > 0) == fail {
				t.Fatal("assignment exposed before safe start", err, client.committed, life)
			}
		})
	}
}
func TestSourceDropsStaleRecordsAndRecognizesMissingTimestamps(t *testing.T) {
	// Given a proven compaction gap ending at offset ten.
	s, _ := sourceFixture(t)
	s.positions[0] = 10
	// When an older fetch arrives, then it cannot reintroduce a record below the proof cursor.
	event, err := s.deliver(&kgo.Record{Partition: 0, Offset: 3})
	if err != nil || event.Input != nil || s.positions[0] != 10 {
		t.Fatal("stale record admitted", event, err)
	}
	// When Kafka's missing-timestamp sentinel arrives, then policy handling sees a missing timestamp.
	event, err = s.deliver(&kgo.Record{Partition: 0, Offset: 10, Timestamp: time.UnixMilli(-1)})
	if err != nil || event.Input == nil || event.Input.Record.TimestampMS != nil || event.Input.Record.TimestampType != "unknown" {
		t.Fatal("missing timestamp treated as an event time", event, err)
	}
}
