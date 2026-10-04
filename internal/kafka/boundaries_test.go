package kafka

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"kafka-fwd2/internal/config"
	core "kafka-fwd2/internal/copy"
	"kafka-fwd2/internal/plan"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

type mockClients struct {
	Clients
	admin    AdminClient
	consumer ConsumerClient
	producer ProducerClient
	cfg      k.ConfigMap
	err      error
}

func (m *mockClients) Admin(cfg k.ConfigMap) (AdminClient, error) { m.cfg = cfg; return m.admin, m.err }
func (m *mockClients) Consumer(cfg k.ConfigMap) (ConsumerClient, error) {
	m.cfg = cfg
	return m.consumer, m.err
}
func (m *mockClients) Producer(cfg k.ConfigMap) (ProducerClient, error) {
	m.cfg = cfg
	return m.producer, m.err
}

type sourceMetadataMock struct {
	OffsetReader
	parts []k.TopicPartition
	err   error
}

func (*sourceMetadataMock) TopicID() string                               { return "topic-id" }
func (m *sourceMetadataMock) Progress(string) ([]k.TopicPartition, error) { return m.parts, m.err }

type destinationMetadataMock struct {
	cfg   map[string]string
	count int
}

func (m destinationMetadataMock) Config() map[string]string { return m.cfg }
func (m destinationMetadataMock) PartitionCount() int       { return m.count }

type positioningMock struct {
	result Positioning
	err    error
}

func (m *positioningMock) New(OffsetReader, TimeReader, plan.Plan, string, string) AssignmentPositioner {
	return m
}
func (m *positioningMock) Locate([]k.TopicPartition, map[int32]k.TopicPartition, map[int32]Progress) (Positioning, error) {
	return m.result, m.err
}

type consumerMock struct {
	ConsumerClient
	callback             k.RebalanceCb
	actions              *[]string
	events               []k.Event
	commitErr, assignErr error
	commits              []k.TopicPartition
	lost, closed         bool
}

func (c *consumerMock) action(name string) {
	if c.actions != nil {
		*c.actions = append(*c.actions, name)
	}
}
func (c *consumerMock) Subscribe(_ string, callback k.RebalanceCb) error {
	c.callback = callback
	c.action("subscribe")
	return nil
}
func (c *consumerMock) CommitOffsets(parts []k.TopicPartition) ([]k.TopicPartition, error) {
	c.action("commit")
	c.commits = append(c.commits, parts...)
	return parts, c.commitErr
}
func (c *consumerMock) IncrementalAssign(parts []k.TopicPartition) error {
	c.action("assign")
	if len(parts) != 1 || parts[0].Offset != 7 {
		return errors.New("wrong assigned position")
	}
	return c.assignErr
}
func (c *consumerMock) IncrementalUnassign([]k.TopicPartition) error {
	c.action("unassign")
	return nil
}
func (c *consumerMock) AssignmentLost() bool { return c.lost }
func (c *consumerMock) Poll(int) k.Event {
	if len(c.events) == 0 {
		return nil
	}
	event := c.events[0]
	c.events = c.events[1:]
	return event
}
func (c *consumerMock) Close() error { c.closed = true; return nil }

type lifecycleMock struct {
	actions *[]string
	lost    bool
}

func (l *lifecycleMock) Assign([]core.Assignment) error {
	*l.actions = append(*l.actions, "assigned")
	return nil
}
func (l *lifecycleMock) Revoke(_ []int32, lost bool) error {
	*l.actions = append(*l.actions, "revoked")
	l.lost = lost
	return nil
}

func sourceWithMocks(t *testing.T, consumer *consumerMock, metadata *sourceMetadataMock, positioning PositionerFactory) (*Source, *mockClients) {
	t.Helper()
	clients := &mockClients{consumer: consumer}
	p := plan.Plan{Source: plan.Endpoint{Name: "orders"}, Group: "group", Start: "EARLIEST"}
	source, _, err := NewSource(clients, metadata, positioning, p, "fp", config.Defaults("home").Worker, quietLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = source.Close() })
	return source, clients
}
func TestSourceCommitsInitialIdentityBeforeAssigningAndStopsOnFailure(t *testing.T) {
	failure := errors.New("failure")
	for _, fail := range []string{"", "progress", "position", "commit", "assign"} {
		t.Run("failure-"+fail, func(t *testing.T) {
			var actions []string
			consumer := &consumerMock{actions: &actions}
			metadata := &sourceMetadataMock{}
			positioning := &positioningMock{result: Positioning{Assignments: []core.Assignment{{Cursor: core.Cursor{Partition: 0, Position: 7}}}, Progress: map[int32]Progress{0: {Fingerprint: "fp", TopicID: "topic-id"}}}}
			switch fail {
			case "progress":
				metadata.err = failure
			case "position":
				positioning.err = failure
			case "commit":
				consumer.commitErr = failure
			case "assign":
				consumer.assignErr = failure
			}
			source, clients := sourceWithMocks(t, consumer, metadata, positioning)
			if clients.cfg["enable.auto.commit"] != false || clients.cfg["enable.auto.offset.store"] != false || clients.cfg["isolation.level"] != "read_committed" {
				t.Fatal("unsafe consumer configuration", clients.cfg)
			}
			if err := source.Start(&lifecycleMock{actions: &actions}); err != nil {
				t.Fatal(err)
			}
			err := consumer.callback(nil, k.AssignedPartitions{Partitions: []k.TopicPartition{{Topic: &source.plan.Source.Name, Partition: 0}}})
			if fail != "" && !errors.Is(err, failure) {
				t.Fatal("failure lost", err)
			}
			if fail == "" {
				if err != nil || !reflect.DeepEqual(actions, []string{"subscribe", "commit", "assign", "assigned"}) {
					t.Fatal("ownership acquired before initial progress", actions, err)
				}
				saved, err := decodeProgress(consumer.commits[0].Metadata, "fp", "topic-id", false)
				if err != nil || saved.TopicID != "topic-id" || consumer.commits[0].Offset != 7 {
					t.Fatal("initial identity not committed", saved, err)
				}
			} else {
				if strings.Contains(strings.Join(actions, ","), "assigned") {
					t.Fatal("failed startup notified ownership", actions)
				}
				if _, err := source.Poll(0); !errors.Is(err, failure) {
					t.Fatal("failed startup resumed polling", err)
				}
			}
		})
	}
}
func TestSourceReleasesLostOwnershipThroughInjectedConsumer(t *testing.T) {
	actions := []string{}
	consumer := &consumerMock{actions: &actions, lost: true}
	source, _ := sourceWithMocks(t, consumer, &sourceMetadataMock{}, nil)
	lifecycle := &lifecycleMock{actions: &actions}
	if err := source.Start(lifecycle); err != nil {
		t.Fatal(err)
	}
	if err := consumer.callback(nil, k.RevokedPartitions{Partitions: []k.TopicPartition{{Partition: 2}}}); err != nil {
		t.Fatal(err)
	}
	if !lifecycle.lost || !reflect.DeepEqual(actions, []string{"subscribe", "revoked", "unassign"}) || len(consumer.commits) != 0 {
		t.Fatal("lost ownership was committed or retained", actions)
	}
}

type assignmentStoreMock struct {
	metadata map[int32]Progress
	cursors  []core.Cursor
	results  map[int32]error
	err      error
}

func (s *assignmentStoreMock) SetMetadata(id int32, progress Progress) { s.metadata[id] = progress }
func (s *assignmentStoreMock) Save(_ context.Context, cursors []core.Cursor) (map[int32]error, error) {
	s.cursors = append(s.cursors, cursors...)
	return s.results, s.err
}
func TestSourceAppliesPositioningAndRequiresEveryInitialCommitResult(t *testing.T) {
	failure := errors.New("commit failure")
	for _, stage := range []string{"success", "request-failure", "partition-failure", "missing-result"} {
		t.Run(stage, func(t *testing.T) {
			store := &assignmentStoreMock{metadata: make(map[int32]Progress), results: map[int32]error{0: nil}}
			switch stage {
			case "request-failure":
				store.err = failure
			case "partition-failure":
				store.results[0] = failure
			case "missing-result":
				delete(store.results, 0)
			}
			progress := Progress{Fingerprint: "fp", TopicID: "topic-id"}
			check := RetentionCheck{Low: 7, Lower: 100}
			cursor := core.Cursor{Partition: 0, Position: 7}
			positioning := &positioningMock{result: Positioning{
				Assignments: []core.Assignment{{Cursor: cursor}},
				Progress:    map[int32]Progress{0: progress},
				Retention:   map[int32]RetentionCheck{0: check},
			}}
			source := &Source{inspector: &sourceMetadataMock{}, positioning: positioning, committer: store, retentionChecks: make(map[int32]RetentionCheck)}
			parts := []k.TopicPartition{{Partition: 0, Offset: k.OffsetInvalid}}
			assigned, err := source.position(parts)
			if !reflect.DeepEqual(store.cursors, []core.Cursor{cursor}) || !reflect.DeepEqual(store.metadata[0], progress) || parts[0].Offset != 7 || source.retentionChecks[0] != check {
				t.Fatal("resolved position, metadata or retention check lost")
			}
			if stage == "success" {
				if err != nil || len(assigned) != 1 || assigned[0].Cursor != cursor {
					t.Fatal("confirmed assignment missing", assigned, err)
				}
			} else if err == nil || assigned != nil || stage != "missing-result" && !errors.Is(err, failure) {
				t.Fatal("unconfirmed assignment returned", assigned, err)
			}
		})
	}
}

func TestSourceDecodesFullRecordWithoutNativeClient(t *testing.T) {
	timestamp := time.UnixMilli(12345)
	message := &k.Message{TopicPartition: k.TopicPartition{Partition: 3, Offset: 42}, Key: nil, Value: []byte{}, Timestamp: timestamp, TimestampType: k.TimestampLogAppendTime, Headers: []k.Header{{Key: "dup", Value: nil}, {Key: "dup", Value: []byte{}}}}
	source, _ := sourceWithMocks(t, &consumerMock{events: []k.Event{message}}, &sourceMetadataMock{}, nil)
	event, err := source.Poll(0)
	if err != nil || event.Input == nil {
		t.Fatal("record missing", err)
	}
	record := event.Input.Record
	if record.Key != nil || record.Value == nil || record.TimestampMS == nil || *record.TimestampMS != 12345 || record.TimestampType != "log_append_time" || !reflect.DeepEqual(record.Headers, []core.Header{{Name: "dup", Value: nil}, {Name: "dup", Value: []byte{}}}) || record.Source.Offset != 42 || event.Input.Next != 43 {
		t.Fatal("logical record changed", record)
	}
}

type producerMock struct {
	ProducerClient
	events      chan k.Event
	messages    []*k.Message
	err         error
	closed      bool
	metadata    *k.Metadata
	metadataErr error
}

func (p *producerMock) Produce(message *k.Message, _ chan k.Event) error {
	p.messages = append(p.messages, message)
	return p.err
}
func (p *producerMock) Events() chan k.Event { return p.events }
func (p *producerMock) Close()               { p.closed = true; close(p.events) }
func (p *producerMock) GetMetadata(*string, bool, int) (*k.Metadata, error) {
	return p.metadata, p.metadataErr
}
func TestSinkRoutesRawRecordsAndAttributesDeliveryFailures(t *testing.T) {
	producer := &producerMock{events: make(chan k.Event, 4)}
	clients := &mockClients{producer: producer}
	clock := &fixedClock{now: time.UnixMilli(12345)}
	metadata := destinationMetadataMock{cfg: map[string]string{"message.timestamp.type": "CreateTime", "max.message.bytes": "10000"}, count: 5}
	sink, err := NewSink(clients, metadata, plan.Endpoint{Name: "orders"}, config.Defaults("home").Worker, clock, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	if clients.cfg["enable.idempotence"] != true || clients.cfg["partitioner"] != "murmur2_random" || clients.cfg["message.max.bytes"] != "10000" || sink.partitionCount != 5 {
		t.Fatal("unsafe destination configuration")
	}
	timestamp := int64(12345)
	token := core.Token{Partition: 2, Generation: 3, Sequence: 4}
	record := core.Record{Key: []byte{0, 255}, Value: nil, TimestampMS: &timestamp, Headers: []core.Header{{Name: "dup", Value: nil}, {Name: "dup", Value: []byte{}}}}
	if err := sink.Submit(record, token); err != nil {
		t.Fatal(err)
	}
	message := producer.messages[0]
	if message.TopicPartition.Partition != k.PartitionAny || !bytes.Equal(message.Key, record.Key) || message.Value != nil || message.Opaque != token || !message.Timestamp.Equal(clock.Now()) || !reflect.DeepEqual(message.Headers, []k.Header{{Key: "dup", Value: nil}, {Key: "dup", Value: []byte{}}}) {
		t.Fatal("raw record or routing changed", message)
	}
	failure := k.NewError(k.ErrTopicAuthorizationFailed, "denied", false)
	message.TopicPartition.Error = failure
	producer.events <- message
	producer.events <- k.NewError(k.ErrAllBrokersDown, "fatal", true)
	for _, fatal := range []bool{false, true} {
		select {
		case delivery := <-sink.Deliveries():
			if delivery.Fatal != fatal || delivery.Err == nil || !fatal && delivery.Token != token {
				t.Fatal("delivery attributed incorrectly", delivery)
			}
		case <-time.After(time.Second):
			t.Fatal("delivery report missing")
		}
	}
	producer.err = k.NewError(k.ErrQueueFull, "full", false)
	var busy *core.Busy
	if err := sink.Submit(record, token); !errors.As(err, &busy) {
		t.Fatal("queue-full error lost", err)
	}
}

func TestDestinationPartitionChangeWarnsWithoutChangingRecordRouting(t *testing.T) {
	producer := &producerMock{events: make(chan k.Event), metadata: &k.Metadata{Topics: map[string]k.TopicMetadata{"orders": {Partitions: make([]k.PartitionMetadata, 7)}}}}
	clock := &fixedClock{now: time.UnixMilli(12345)}
	var logs bytes.Buffer
	sink, err := NewSink(&mockClients{producer: producer}, destinationMetadataMock{count: 5}, plan.Endpoint{Name: "orders"}, config.Defaults("home").Worker, clock, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	clock.now = clock.now.Add(31 * time.Second)
	record := core.Record{Key: []byte{0, 255}, Value: []byte("opaque")}
	if err := sink.Submit(record, core.Token{}); err != nil {
		t.Fatal(err)
	}
	message := producer.messages[0]
	if sink.partitionCount != 7 || !strings.Contains(logs.String(), "destination partition count changed; key routing may change") || !bytes.Equal(message.Key, record.Key) || message.TopicPartition.Partition != k.PartitionAny {
		t.Fatal("partition expansion silently changed routing or record bytes", logs.String(), message)
	}
	producer.metadataErr = errors.New("metadata unavailable")
	clock.now = clock.now.Add(31 * time.Second)
	if err := sink.Submit(record, core.Token{}); err != nil || !strings.Contains(logs.String(), "destination partition count check failed") {
		t.Fatal("metadata failure unexpectedly stopped copying or was not logged", err, logs.String())
	}
}

type adminMock struct {
	AdminClient
	described    k.DescribeTopicsResult
	describeErr  error
	groupOffsets k.ListConsumerGroupOffsetsResult
	requested    []k.ConsumerGroupTopicPartitions
	offsetResult k.ListOffsetsResult
	groups       k.DescribeConsumerGroupsResult
	closed       bool
}

func (a *adminMock) DescribeTopics(context.Context, k.TopicCollection, ...k.DescribeTopicsAdminOption) (k.DescribeTopicsResult, error) {
	return a.described, a.describeErr
}
func (a *adminMock) ListConsumerGroupOffsets(_ context.Context, request []k.ConsumerGroupTopicPartitions, _ ...k.ListConsumerGroupOffsetsAdminOption) (k.ListConsumerGroupOffsetsResult, error) {
	a.requested = request
	return a.groupOffsets, nil
}
func (a *adminMock) ListOffsets(context.Context, map[k.TopicPartition]k.OffsetSpec, ...k.ListOffsetsAdminOption) (k.ListOffsetsResult, error) {
	return a.offsetResult, nil
}
func (a *adminMock) DescribeConsumerGroups(context.Context, []string, ...k.DescribeConsumerGroupsAdminOption) (k.DescribeConsumerGroupsResult, error) {
	return a.groups, nil
}
func (a *adminMock) Close() { a.closed = true }
func TestInspectorClosesAdminWhenPreflightFails(t *testing.T) {
	for _, result := range []k.DescribeTopicsResult{{}, {TopicDescriptions: []k.TopicDescription{{Name: "orders"}}}} {
		admin := &adminMock{described: result}
		if _, err := NewInspector(&mockClients{admin: admin}, plan.Properties{}, "orders", quietLogger()); err == nil || !admin.closed {
			t.Fatal("invalid topic identity accepted or admin leaked", err)
		}
	}
}
func TestInspectorRequestsCurrentPartitions(t *testing.T) {
	description := k.TopicDescription{Name: "orders", Partitions: []k.TopicPartitionInfo{{Partition: 0}, {Partition: 2}}}
	admin := &adminMock{described: k.DescribeTopicsResult{TopicDescriptions: []k.TopicDescription{description}}, groupOffsets: k.ListConsumerGroupOffsetsResult{ConsumerGroupsTopicPartitions: []k.ConsumerGroupTopicPartitions{{Partitions: []k.TopicPartition{{Partition: 0, Offset: 10}, {Partition: 2, Offset: k.OffsetInvalid}}}}}}
	inspector := &Inspector{admin: admin, topic: "orders", description: description, log: quietLogger()}
	parts, err := inspector.Progress("group")
	if err != nil || len(parts) != 2 || len(admin.requested) != 1 || admin.requested[0].Group != "group" || len(admin.requested[0].Partitions) != 2 || admin.requested[0].Partitions[1].Partition != 2 {
		t.Fatal("current partition discovery failed", parts, admin.requested, err)
	}
}
func TestSourceValidatesProgressThroughMetadataInterface(t *testing.T) {
	valid := Progress{Fingerprint: "fp", TopicID: "topic-id"}.Encode()
	metadata := &sourceMetadataMock{parts: []k.TopicPartition{{Partition: 0, Offset: 10, Metadata: &valid}, {Partition: 2, Offset: k.OffsetInvalid}}}
	source := &Source{inspector: metadata, fingerprint: "fp", plan: plan.Plan{Group: "group"}}
	positions, progress, err := source.saved()
	if err != nil || len(progress) != 1 || len(positions) != 1 || positions[0].Offset != 10 {
		t.Fatal("valid progress lost", progress, err)
	}
	foreign := "foreign"
	metadata.parts[0].Metadata = &foreign
	if _, _, err := source.saved(); err == nil {
		t.Fatal("foreign progress became a start fallback")
	}
}
func TestInspectorRejectsMissingOffsetResultsAndForeignMembers(t *testing.T) {
	admin := &adminMock{offsetResult: k.ListOffsetsResult{ResultInfos: map[k.TopicPartition]k.ListOffsetsResultInfo{}}}
	inspector := &Inspector{admin: admin, topic: "orders", log: quietLogger()}
	if _, err := inspector.Offsets([]int32{0}, k.LatestOffsetSpec, true); err == nil {
		t.Fatal("missing offset accepted")
	}
	admin.groups = k.DescribeConsumerGroupsResult{ConsumerGroupDescriptions: []k.ConsumerGroupDescription{{Members: []k.MemberDescription{{ClientID: "another-app"}}}}}
	if err := inspector.CheckMembers("group"); err == nil {
		t.Fatal("foreign active member accepted")
	}
}
