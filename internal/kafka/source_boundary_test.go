package kafka

import (
	"errors"
	"reflect"
	"testing"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	core "github.com/pagrus7/kaferry/internal/copy"
)

type callbackPoller func() k.Event

func (p callbackPoller) Poll(int) k.Event { return p() }

func TestSourceDropsRecordWhenAssignmentFailsInsidePoll(t *testing.T) {
	// Given a consumer whose initial commit fails during a rebalance callback.
	failure := errors.New("initial commit failed")
	consumer := &consumerMock{commitErr: failure}
	positioning := &positioningMock{result: Positioning{Assignments: []core.Assignment{{Cursor: core.Cursor{Position: 7}}}, Progress: map[int32]Progress{0: {Fingerprint: "fp", TopicID: "topic-id"}}}}
	source, _ := sourceWithMocks(t, consumer, &sourceMetadataMock{}, positioning)
	var actions []string
	if err := source.Start(&lifecycleMock{actions: &actions}); err != nil {
		t.Fatal(err)
	}
	calls := 0
	source.events = callbackPoller(func() k.Event {
		calls++
		if err := consumer.callback(nil, k.AssignedPartitions{Partitions: []k.TopicPartition{{Partition: 0}}}); !errors.Is(err, failure) {
			t.Fatal("assignment failure lost", err)
		}
		return &k.Message{TopicPartition: k.TopicPartition{Offset: 7}}
	})

	// When native Poll returns a message after the failed callback, then no record
	// escapes and later polls retain the error without calling the consumer again.
	for range 2 {
		event, err := source.Poll(0)
		if !errors.Is(err, failure) || event.Input != nil || event.EOF != nil {
			t.Fatal("failed assignment admitted input", event, err)
		}
	}
	if calls != 1 || len(actions) != 0 {
		t.Fatal("failed assignment acquired ownership or continued polling", calls, actions)
	}
}

func TestSourceEOFAndPartitionFailureDoNotEmitRecords(t *testing.T) {
	// Given an EOF and a subsequent partition-level fetch error.
	failure := k.NewError(k.ErrTopicAuthorizationFailed, "denied", false)
	consumer := &consumerMock{events: []k.Event{k.PartitionEOF{Partition: 3, Offset: 43}, &k.Message{TopicPartition: k.TopicPartition{Error: failure}}}}
	source, _ := sourceWithMocks(t, consumer, &sourceMetadataMock{}, nil)
	source.retrying = true

	// When polling each event, then EOF reports the next cursor and clears retry
	// state, while the failed fetch emits neither a record nor an EOF.
	event, err := source.Poll(0)
	if err != nil || event.Input != nil || event.EOF == nil || *event.EOF != (core.Cursor{Partition: 3, Position: 43}) || source.retrying {
		t.Fatal("EOF cursor or recovery signal lost", event, err)
	}
	event, err = source.Poll(0)
	if !errors.Is(err, failure) || event.Input != nil || event.EOF != nil {
		t.Fatal("failed fetch admitted input", event, err)
	}
}

func TestSourcePauseAndResumeTargetOnlyRequestedPartition(t *testing.T) {
	// Given multiple partition ownership with one partition under backpressure.
	consumer := &consumerMock{}
	source, _ := sourceWithMocks(t, consumer, &sourceMetadataMock{}, nil)

	// When pausing and resuming partition 3.
	if err := source.Pause(3, true); err != nil {
		t.Fatal(err)
	}
	if err := source.Pause(3, false); err != nil {
		t.Fatal(err)
	}

	// Then both native calls target exactly that partition of the source topic.
	want := []k.TopicPartition{{Topic: &source.plan.Source.Name, Partition: 3}}
	if !reflect.DeepEqual(consumer.paused, want) || !reflect.DeepEqual(consumer.resumed, want) {
		t.Fatal("backpressure affected another partition", consumer.paused, consumer.resumed)
	}
}
