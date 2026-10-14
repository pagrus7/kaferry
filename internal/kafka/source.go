package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pagrus7/kaferry/internal/config"
	copy "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
)

type poller interface{ Poll(int) k.Event }

type Source struct {
	consumer        ConsumerClient
	events          poller
	offsets         OffsetReader
	times           TimeReader
	inspector       SourceMetadata
	positioning     PositionerFactory
	committer       AssignmentStore
	plan            plan.Plan
	fingerprint     string
	log             *slog.Logger
	lifecycle       copy.Lifecycle
	err             error
	retrying        bool
	retentionChecks map[int32]RetentionCheck
}

type RetentionCheck struct {
	Low   int64
	Lower int64
}

func NewSource(clients Clients, inspector SourceMetadata, positioning PositionerFactory, p plan.Plan, fp string, limits config.Worker, logger *slog.Logger) (*Source, SourceProgress, error) {
	cfg, err := consumerConfig(p, limits)
	if err != nil {
		return nil, nil, err
	}
	c, err := clients.Consumer(cfg)
	if err != nil {
		return nil, nil, conflict("invalid Kafka consumer configuration")
	}
	store := &Committer{consumer: c, topic: p.Source.Name, metadata: make(map[int32]Progress)}
	s := &Source{consumer: c, events: c, offsets: inspector, times: c, inspector: inspector, positioning: positioning, committer: store, plan: p, fingerprint: fp, log: logger, retentionChecks: make(map[int32]RetentionCheck)}
	return s, store, nil
}
func (s *Source) Start(lifecycle copy.Lifecycle) error {
	s.lifecycle = lifecycle
	return s.consumer.Subscribe(s.plan.Source.Name, s.rebalance)
}
func (s *Source) rebalance(_ *k.Consumer, event k.Event) error {
	c := s.consumer
	if s.err != nil {
		return s.err
	}
	switch event := event.(type) {
	case k.AssignedPartitions:
		assignments, err := s.position(event.Partitions)
		if err != nil {
			s.err = err
			return err
		}
		if err = c.IncrementalAssign(event.Partitions); err != nil {
			s.err = err
			return err
		}
		// Application pauses survive unassignment in librdkafka. New assignments
		// start unpaused; the runner reapplies its current caps before the next Poll.
		if len(event.Partitions) > 0 {
			if err = c.Resume(event.Partitions); err != nil {
				s.err = err
				return err
			}
		}
		s.err = s.lifecycle.Assign(assignments)
	case k.RevokedPartitions:
		ids := make([]int32, len(event.Partitions))
		for n, part := range event.Partitions {
			ids[n] = part.Partition
			delete(s.retentionChecks, part.Partition)
		}
		if c.AssignmentLost() {
			s.log.Warn("assignment ownership lost; unconfirmed progress will replay")
		}
		// The runner commits only confirmed work and fences late outcomes.
		s.err = s.lifecycle.Revoke(ids, c.AssignmentLost())
		if err := c.IncrementalUnassign(event.Partitions); err != nil && s.err == nil {
			s.err = err
		}
	}
	return s.err
}
func (s *Source) saved() (map[int32]k.TopicPartition, map[int32]Progress, error) {
	requested, err := s.inspector.CurrentPartitions()
	if err != nil {
		return nil, nil, err
	}
	parts, err := s.consumer.Committed(requested, 10000)
	if err != nil {
		return nil, nil, err
	}
	if len(parts) != len(requested) {
		return nil, nil, fmt.Errorf("missing committed partition results")
	}
	return decodeCommitted(parts, s.fingerprint, s.inspector.TopicID(), s.plan.Window, s.plan.CutoffMS)
}
func (s *Source) position(parts []k.TopicPartition) ([]copy.Assignment, error) {
	if len(parts) == 0 {
		return nil, nil
	}
	saved, metadata, err := s.saved()
	if err != nil {
		return nil, err
	}
	assignments, err := s.locate(parts, saved, metadata)
	if err != nil {
		return nil, err
	}
	initial := make([]copy.Cursor, len(assignments))
	for n, assignment := range assignments {
		initial[n] = assignment.Cursor
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	results, err := s.committer.Save(ctx, initial)
	if err != nil {
		return nil, err
	}
	for _, part := range parts {
		if err, present := results[part.Partition]; !present || err != nil {
			if !present {
				return nil, fmt.Errorf("missing initial commit result for partition %d", part.Partition)
			}
			return nil, err
		}
	}
	return assignments, nil
}

// locate applies the positions resolved by the assignment positioner.
func (s *Source) locate(parts []k.TopicPartition, saved map[int32]k.TopicPartition, metadata map[int32]Progress) ([]copy.Assignment, error) {
	if len(parts) == 0 {
		return nil, nil
	}
	positioner := s.positioning.New(s.offsets, s.times, s.plan, s.fingerprint, s.inspector.TopicID())
	positioned, err := positioner.Locate(parts, saved, metadata)
	if err != nil {
		return nil, err
	}
	for n, assignment := range positioned.Assignments {
		id, position := assignment.Cursor.Partition, assignment.Cursor.Position
		parts[n].Offset = k.Offset(position)
		s.committer.SetMetadata(id, positioned.Progress[id])
	}
	for id, check := range positioned.Retention {
		s.retentionChecks[id] = check
	}
	return positioned.Assignments, nil
}
func (s *Source) Poll(timeout time.Duration) (copy.Event, error) {
	if s.err != nil {
		return copy.Event{}, s.err
	}
	event := s.events.Poll(int(timeout.Milliseconds()))
	if s.err != nil {
		return copy.Event{}, s.err
	}
	switch event := event.(type) {
	case *k.Message:
		s.retrying = false
		if event.TopicPartition.Error != nil {
			return copy.Event{}, event.TopicPartition.Error
		}
		record := copy.Record{Key: event.Key, Value: event.Value, TimestampType: "unknown", Source: &copy.Provenance{Topic: s.plan.Source.Name, Partition: event.TopicPartition.Partition, Offset: int64(event.TopicPartition.Offset)}}
		if event.TimestampType != k.TimestampNotAvailable {
			timestamp := event.Timestamp.UnixMilli()
			record.TimestampMS = &timestamp
			if event.TimestampType == k.TimestampLogAppendTime {
				record.TimestampType = "log_append_time"
			} else {
				record.TimestampType = "create_time"
			}
		}
		for _, header := range event.Headers {
			record.Headers = append(record.Headers, copy.Header{Name: header.Key, Value: header.Value})
		}
		s.checkRetention(event.TopicPartition.Partition, record.TimestampMS)
		cursor := copy.Cursor{Partition: event.TopicPartition.Partition, Position: int64(event.TopicPartition.Offset)}
		return copy.Event{Input: &copy.Input{Cursor: cursor, Next: cursor.Position + 1, Record: record}}, nil
	case k.PartitionEOF:
		s.retrying = false
		cursor := copy.Cursor{Partition: event.Partition, Position: int64(event.Offset)}
		return copy.Event{EOF: &cursor}, nil
	case k.Error:
		// The client reconnects on its own; a quiet partition gives no recovery signal,
		// so transient outages have no deadline here. tip_wait bounds waiting through H.
		if transient(event) {
			if !s.retrying {
				s.retrying = true
				s.log.Warn("source connection retrying", "code", event.Code().String())
			}
			return copy.Event{}, nil
		}
		return copy.Event{}, event
	}
	return copy.Event{}, nil
}

func (s *Source) checkRetention(partition int32, timestamp *int64) {
	check, pending := s.retentionChecks[partition]
	if !pending {
		return
	}
	delete(s.retentionChecks, partition)
	if timestamp != nil && *timestamp > check.Lower {
		s.log.Warn("time window may begin before retained data", "partition", partition, "log_start", check.Low, "first_retained_timestamp", *timestamp, "lower", check.Lower)
	}
}

func transient(err k.Error) bool {
	if err.IsFatal() {
		return false
	}
	switch err.Code() {
	case k.ErrTransport, k.ErrResolve, k.ErrAllBrokersDown, k.ErrTimedOut, k.ErrTimedOutQueue, k.ErrCoordinatorLoadInProgress, k.ErrCoordinatorNotAvailable, k.ErrNotCoordinator, k.ErrRequestTimedOut, k.ErrNetworkException:
		return true
	}
	return false
}
func (s *Source) Pause(partition int32, pause bool) error {
	parts := []k.TopicPartition{{Topic: &s.plan.Source.Name, Partition: partition}}
	if pause {
		return s.consumer.Pause(parts)
	}
	return s.consumer.Resume(parts)
}
func (s *Source) HighWatermark(partition int32) (int64, error) {
	// A read-committed consumer's watermark is its last stable offset. Sample
	// the actual log end so open transactions remain eligible until tip_wait.
	offsets, err := s.offsets.Offsets([]int32{partition}, k.LatestOffsetSpec, false)
	var native k.Error
	if errors.Is(err, context.DeadlineExceeded) || errors.As(err, &native) && watermarkTransient(native) {
		return 0, &copy.Recoverable{Err: err}
	}
	if err != nil {
		return 0, err
	}
	high, present := offsets[partition]
	if !present || high < 0 {
		return 0, fmt.Errorf("missing or invalid high watermark for partition %d", partition)
	}
	return high, nil
}

// Watermark queries return leader-change errors that ordinary Poll handles internally.
func watermarkTransient(err k.Error) bool {
	if err.IsFatal() {
		return false
	}
	if transient(err) {
		return true
	}
	switch err.Code() {
	case k.ErrNotLeaderForPartition, k.ErrLeaderNotAvailable, k.ErrReplicaNotAvailable,
		k.ErrKafkaStorageError, k.ErrOffsetNotAvailable, k.ErrFencedLeaderEpoch, k.ErrUnknownLeaderEpoch:
		return true
	}
	return false
}
func (s *Source) Close() error { return s.consumer.Close() }
