package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"kafka-fwd2/internal/config"
	copy "kafka-fwd2/internal/copy"
	"kafka-fwd2/internal/plan"
)

type Committer struct {
	consumer *k.Consumer
	topic    string
	metadata map[int32]Progress
	output   copy.OutputProgress
}

func (c *Committer) Save(ctx context.Context, cursors []copy.Cursor) (map[int32]error, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(cursors) == 0 {
		return map[int32]error{}, nil
	}
	parts := make([]k.TopicPartition, len(cursors))
	for n, cursor := range cursors {
		progress := c.metadata[cursor.Partition]
		if c.output != nil {
			id, extent := c.output.DurableOutput()
			if progress.FileID != "" && progress.FileID != id {
				return nil, conflict("destination file identity mismatch")
			}
			progress.FileID = id
			progress.Extent = extent
			c.metadata[cursor.Partition] = progress
		}
		metadata := progress.Encode()
		parts[n] = k.TopicPartition{Topic: &c.topic, Partition: cursor.Partition, Offset: k.Offset(cursor.Position), Metadata: &metadata}
	}
	result, err := c.consumer.CommitOffsets(parts)
	if err != nil {
		return nil, err
	}
	errors := make(map[int32]error, len(result))
	for _, part := range result {
		errors[part.Partition] = part.Error
	}
	return errors, nil
}

type Source struct {
	consumer        *k.Consumer
	inspector       *Inspector
	committer       *Committer
	plan            plan.Plan
	fingerprint     string
	clock           copy.Clock
	log             *slog.Logger
	lifecycle       copy.Lifecycle
	err             error
	retrying        time.Time
	retryBudget     time.Duration
	retentionChecks map[int32]retentionCheck
}

type retentionCheck struct {
	low   int64
	lower int64
}

func NewSource(inspector *Inspector, p plan.Plan, fp string, limits config.Worker, clock copy.Clock, logger *slog.Logger, output copy.OutputProgress) (*Source, *Committer, error) {
	if err := inspector.CheckMembers(p.Group); err != nil {
		return nil, nil, err
	}
	cfg := native(p.Source.Properties)
	queueKB := (limits.ByteCap + 1023) / 1024
	if existing, ok := cfg["queued.max.messages.kbytes"]; !ok {
		cfg["queued.max.messages.kbytes"] = fmt.Sprint(queueKB)
	} else {
		var value int64
		if _, err := fmt.Sscan(fmt.Sprint(existing), &value); err != nil || value > queueKB || value < 1 {
			return nil, nil, conflict("queued.max.messages.kbytes must be positive and no larger than the worker byte cap")
		}
	}
	protected := map[string]any{"group.id": p.Group, "client.id": "kafka-fwd2-" + p.Group, "enable.auto.commit": false, "enable.auto.offset.store": false, "auto.offset.reset": "error", "isolation.level": "read_committed", "enable.partition.eof": true, "partition.assignment.strategy": "cooperative-sticky", "group.protocol": "classic", "allow.auto.create.topics": false, "go.events.channel.enable": false}
	for key, value := range protected {
		if old, ok := cfg[key]; ok && fmt.Sprint(old) != fmt.Sprint(value) {
			return nil, nil, conflict("protected Kafka property %q cannot be overridden", key)
		}
		cfg[key] = value
	}
	c, err := k.NewConsumer(&cfg)
	if err != nil {
		return nil, nil, conflict("invalid Kafka consumer configuration")
	}
	store := &Committer{consumer: c, topic: p.Source.Name, metadata: make(map[int32]Progress), output: output}
	s := &Source{consumer: c, inspector: inspector, committer: store, plan: p, fingerprint: fp, clock: clock, log: logger, retentionChecks: make(map[int32]retentionCheck)}
	s.retryBudget, _ = time.ParseDuration(limits.DeliveryTimeout)
	if _, _, err = s.saved(); err != nil {
		_ = c.Close()
		return nil, nil, err
	}
	return s, store, nil
}
func (s *Source) Start(lifecycle copy.Lifecycle) error {
	s.lifecycle = lifecycle
	return s.consumer.Subscribe(s.plan.Source.Name, s.rebalance)
}
func (s *Source) rebalance(c *k.Consumer, event k.Event) error {
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
		s.err = s.lifecycle.Assign(assignments)
		s.retrying = time.Time{}
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
	parts, err := s.inspector.Progress(s.plan.Group)
	if err != nil {
		return nil, nil, err
	}
	saved := make(map[int32]k.TopicPartition)
	metadata := make(map[int32]Progress)
	for _, part := range parts {
		if part.Offset < 0 {
			continue
		}
		progress, err := decodeProgress(part.Metadata, s.fingerprint, s.inspector.TopicID(), s.plan.Window.Relative())
		if err != nil {
			return nil, nil, err
		}
		if err := validateWindow(progress, s.plan.Window, s.plan.CutoffMS); err != nil {
			return nil, nil, err
		}
		saved[part.Partition] = part
		metadata[part.Partition] = progress
	}
	return saved, metadata, nil
}
func (s *Source) position(parts []k.TopicPartition) ([]copy.Assignment, error) {
	saved, metadata, err := s.saved()
	if err != nil {
		return nil, err
	}
	var adopted *plan.Window
	ids := make([]int, 0, len(metadata))
	for id := range metadata {
		ids = append(ids, int(id))
	}
	sort.Ints(ids)
	if len(ids) > 0 {
		adopted = metadata[int32(ids[0])].Window
	}
	now := s.clock.Now()
	assignments := make([]copy.Assignment, 0, len(parts))
	initial := make([]copy.Cursor, 0, len(parts))
	for n, part := range parts {
		origin := "resolved"
		window, err := s.plan.Window.Resolve(now)
		if err != nil {
			return nil, conflict("invalid resolved time window: %v", err)
		}
		progress := Progress{Fingerprint: s.fingerprint, TopicID: s.inspector.TopicID()}
		position := int64(-1)
		if stored, ok := saved[part.Partition]; ok {
			origin = "saved"
			position = int64(stored.Offset)
			progress = metadata[part.Partition]
			if progress.Window != nil {
				window = *progress.Window
			}
		} else if adopted != nil && s.plan.Window.Relative() {
			origin = "adopted"
			window = *adopted
		}
		if s.plan.Window.Relative() {
			progress.Window = &window
		}
		if err := s.plan.Window.ValidateStored(window, s.plan.CutoffMS); err != nil {
			return nil, conflict("invalid time window: %v", err)
		}
		if s.plan.Window.Relative() {
			s.log.Info("partition window positioned", "partition", part.Partition, "origin", origin, "window", window)
		}
		low, high, err := s.consumer.QueryWatermarkOffsets(s.plan.Source.Name, part.Partition, 5000)
		if err != nil {
			return nil, err
		}
		if position > high {
			return nil, conflict("partition %d committed offset %d exceeds log end %d", part.Partition, position, high)
		}
		if position >= 0 && position < low {
			s.log.Warn("expired source position", "partition", part.Partition, "saved", position, "log_start", low, "offset_gap", low-position)
			position = low
		}
		if position < 0 {
			position, err = s.start(part.Partition, window, low, high, len(saved) > 0)
			if err != nil {
				return nil, err
			}
		}
		parts[n].Offset = k.Offset(position)
		if low > 0 && position == low && window.Lower != nil {
			s.retentionChecks[part.Partition] = retentionCheck{low: low, lower: *window.Lower}
		}
		s.committer.metadata[part.Partition] = progress
		cursor := copy.Cursor{Partition: part.Partition, Position: position}
		initial = append(initial, cursor)
		assignments = append(assignments, copy.Assignment{Cursor: cursor, Window: window})
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
func (s *Source) start(partition int32, window plan.Window, low, high int64, hasProgress bool) (int64, error) {
	if window.Lower != nil {
		if *window.Lower <= 0 {
			return low, nil
		} // Negative timestamps are reserved offset specs in Kafka.
		result, err := s.consumer.OffsetsForTimes([]k.TopicPartition{{Topic: &s.plan.Source.Name, Partition: partition, Offset: k.Offset(*window.Lower)}}, 5000)
		if err != nil {
			return 0, err
		}
		if result[0].Error != nil {
			return 0, result[0].Error
		}
		if result[0].Offset < 0 {
			return s.inspector.End(partition)
		}
		return int64(result[0].Offset), nil
	}
	if window.Upper != nil || hasProgress || s.plan.Start == "EARLIEST" {
		if hasProgress && window.Upper == nil {
			s.log.Info("new partition starts at earliest retained offset", "partition", partition)
		}
		return low, nil
	}
	return s.inspector.End(partition)
}
func (s *Source) Poll(timeout time.Duration) (copy.Event, error) {
	if s.err != nil {
		return copy.Event{}, s.err
	}
	if !s.retrying.IsZero() && s.clock.Now().Sub(s.retrying) >= s.retryBudget {
		return copy.Event{}, fmt.Errorf("source recovery exceeded %s", s.retryBudget)
	}
	event := s.consumer.Poll(int(timeout.Milliseconds()))
	if s.err != nil {
		return copy.Event{}, s.err
	}
	switch event := event.(type) {
	case *k.Message:
		s.retrying = time.Time{}
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
		s.retrying = time.Time{}
		cursor := copy.Cursor{Partition: event.Partition, Position: int64(event.Offset)}
		return copy.Event{EOF: &cursor}, nil
	case k.Error:
		if transient(event) {
			if s.retrying.IsZero() {
				s.retrying = s.clock.Now()
				s.log.Warn("source connection retrying", "code", event.Code().String(), "budget", s.retryBudget.String())
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
	if timestamp != nil && *timestamp > check.lower {
		s.log.Warn("time window may begin before retained data", "partition", partition, "log_start", check.low, "first_retained_timestamp", *timestamp, "lower", check.lower)
	}
}

func transient(err k.Error) bool {
	if err.IsFatal() {
		return false
	}
	switch err.Code() {
	case k.ErrTransport, k.ErrAllBrokersDown, k.ErrTimedOut, k.ErrTimedOutQueue, k.ErrCoordinatorLoadInProgress, k.ErrCoordinatorNotAvailable, k.ErrNotCoordinator, k.ErrRequestTimedOut, k.ErrNetworkException:
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
	_, high, err := s.consumer.QueryWatermarkOffsets(s.plan.Source.Name, partition, 5000)
	return high, err
}
func (s *Source) Close() error { return s.consumer.Close() }
