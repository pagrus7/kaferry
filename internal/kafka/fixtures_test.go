package kafka

import (
	"context"
	"errors"
	"fmt"
	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
	"io"
	"log/slog"
	"testing"
	"time"
)

type scriptedPoller struct{ events []k.Event }

func (p *scriptedPoller) Poll(int) k.Event {
	if len(p.events) == 0 {
		return nil
	}
	event := p.events[0]
	p.events = p.events[1:]
	return event
}

type capturedCommits struct{ parts []k.TopicPartition }

func (c *capturedCommits) CommitOffsets(parts []k.TopicPartition) ([]k.TopicPartition, error) {
	c.parts = append(c.parts, parts...)
	return parts, nil
}

type durableExtent struct{ extent int64 }

func (d *durableExtent) DurableOutput() (string, int64) { return "archive-id", d.extent }

type countingOffsets struct {
	calls     []string
	low, high int64
	stable    int64
}

func (o *countingOffsets) Offsets(partitions []int32, spec k.OffsetSpec, readCommitted bool) (map[int32]int64, error) {
	o.calls = append(o.calls, fmt.Sprint(spec, readCommitted))
	value := o.high
	switch {
	case spec == k.EarliestOffsetSpec:
		value = o.low
	case readCommitted:
		value = o.stable
	}
	out := make(map[int32]int64, len(partitions))
	for _, partition := range partitions {
		out[partition] = value
	}
	return out, nil
}

type countingTimes struct {
	calls int
	topic string
}

func (c *countingTimes) OffsetsForTimes(parts []k.TopicPartition, _ int) ([]k.TopicPartition, error) {
	c.calls++
	out := make([]k.TopicPartition, len(parts))
	for n, part := range parts {
		out[n] = k.TopicPartition{Topic: &c.topic, Partition: part.Partition, Offset: 500}
		if part.Partition%3 == 0 {
			out[n].Offset = k.OffsetEnd // No match: start at the read-committed end.
		}
	}
	return out, nil
}

type fixedClock struct{ now time.Time }

func (c *fixedClock) Now() time.Time { return c.now }

func (*fixedClock) Sleep(time.Duration) {}

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

func (*sourceMetadataMock) TopicID() string { return "topic-id" }

func (m *sourceMetadataMock) CurrentPartitions() ([]k.TopicPartition, error) { return m.parts, m.err }

type destinationMetadataMock struct {
	cfg   map[string]string
	count int
}

func (m destinationMetadataMock) Config() map[string]string { return m.cfg }

func (m destinationMetadataMock) PartitionCount() int { return m.count }

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
	resumeErr            error
	resumed              []k.TopicPartition
	paused               []k.TopicPartition
	commits              []k.TopicPartition
	committed            []k.TopicPartition
	committedErr         error
	committedRequests    []k.TopicPartition
	committedTimeout     int
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

func (c *consumerMock) Resume(parts []k.TopicPartition) error {
	c.action("resume")
	c.resumed = append(c.resumed, parts...)
	return c.resumeErr
}

func (c *consumerMock) Pause(parts []k.TopicPartition) error {
	c.paused = append(c.paused, parts...)
	return nil
}

func (c *consumerMock) Committed(parts []k.TopicPartition, timeout int) ([]k.TopicPartition, error) {
	c.committedRequests = append([]k.TopicPartition(nil), parts...)
	c.committedTimeout = timeout
	if c.committedErr != nil {
		return nil, c.committedErr
	}
	if c.committed != nil {
		return c.committed, nil
	}
	out := append([]k.TopicPartition(nil), parts...)
	for n := range out {
		out[n].Offset = k.OffsetInvalid
		out[n].Metadata = nil
	}
	return out, nil
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
	consumer.committed = metadata.parts
	clients := &mockClients{consumer: consumer}
	p := plan.Plan{Source: plan.Endpoint{Name: "orders"}, Group: "group", Start: "EARLIEST"}
	source, _, err := NewSource(clients, metadata, positioning, p, "fp", config.Defaults("home").Worker, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = source.Close() })
	return source, clients
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

func (p *producerMock) Close() { p.closed = true; close(p.events) }

func (p *producerMock) GetMetadata(*string, bool, int) (*k.Metadata, error) {
	return p.metadata, p.metadataErr
}

type adminMock struct {
	AdminClient
	described     k.DescribeTopicsResult
	describeErr   error
	groupOffsets  k.ListConsumerGroupOffsetsResult
	requested     []k.ConsumerGroupTopicPartitions
	offsetResult  k.ListOffsetsResult
	offsetErr     error
	groups        k.DescribeConsumerGroupsResult
	configs       []k.ConfigResourceResult
	configErr     error
	configRequest []k.ConfigResource
	closed        bool
}

func (a *adminMock) DescribeTopics(context.Context, k.TopicCollection, ...k.DescribeTopicsAdminOption) (k.DescribeTopicsResult, error) {
	return a.described, a.describeErr
}

func (a *adminMock) ListConsumerGroupOffsets(_ context.Context, request []k.ConsumerGroupTopicPartitions, _ ...k.ListConsumerGroupOffsetsAdminOption) (k.ListConsumerGroupOffsetsResult, error) {
	a.requested = request
	return a.groupOffsets, nil
}

func (a *adminMock) ListOffsets(context.Context, map[k.TopicPartition]k.OffsetSpec, ...k.ListOffsetsAdminOption) (k.ListOffsetsResult, error) {
	return a.offsetResult, a.offsetErr
}

func (a *adminMock) DescribeConsumerGroups(context.Context, []string, ...k.DescribeConsumerGroupsAdminOption) (k.DescribeConsumerGroupsResult, error) {
	return a.groups, nil
}

func (a *adminMock) Close() { a.closed = true }

func (a *adminMock) DescribeConfigs(_ context.Context, request []k.ConfigResource, _ ...k.DescribeConfigsAdminOption) ([]k.ConfigResourceResult, error) {
	a.configRequest = request
	return a.configs, a.configErr
}
