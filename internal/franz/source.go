package franz

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/delivery"
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/twmb/franz-go/pkg/kgo"
)

type sourceMetadata interface {
	PartitionIDs() []int32
	kafkaport.OffsetReader
	kafkaport.TimeReader
	Saved(context.Context, string, string, plan.WindowSpec, int64) (map[int32]int64, map[int32]kafkaport.Progress, error)
}
type Source struct {
	delivery             *delivery.Session
	client               ConsumerClient
	routing              kafkaport.PartitionValidator
	inspector            sourceMetadata
	lookup               kafkaport.OffsetLookup
	positioning          kafkaport.PositionerFactory
	store                *Committer
	bridge               *rebalanceBridge
	pump                 *fetchPump
	plan                 plan.Plan
	fingerprint, topicID string
	clusterID            string
	originalTopicID      [16]byte
	query, ctx           context.Context
	cancel, clientCancel context.CancelFunc
	life                 core.Lifecycle
	observer             core.Observer
	positions            map[int32]int64
	paused               map[int32]bool
	held                 map[int32]*kgo.Record
	retention            map[int32]kafkaport.RetentionCheck
	readyEOF             []core.Cursor
	gaps                 gapReader
	lastGap              int32
	lastTip              time.Time
	err                  error
	retrying             bool
	log                  *slog.Logger
}

func (s *Source) Start(life core.Lifecycle, observer core.Observer) error {
	if s.pump != nil {
		return fmt.Errorf("source already started")
	}
	if err := s.query.Err(); err != nil {
		return err
	}
	s.life = life
	s.observer = observer
	s.client.AddConsumeTopics(s.plan.Source.Name)
	s.pump = newFetchPump(s.ctx, s.client)
	go s.pump.run()
	return nil
}
func (s *Source) Poll(timeout time.Duration) (core.Event, error) {
	if s.err != nil {
		return core.Event{}, s.err
	}
	if s.pump == nil {
		return core.Event{}, fmt.Errorf("source is not started")
	}
	event, err := s.poll(timeout)
	if err != nil {
		s.err = err
	}
	return event, err
}
func (s *Source) poll(timeout time.Duration) (core.Event, error) {
	if err := s.checkTips(); err != nil {
		return core.Event{}, err
	}
	for len(s.readyEOF) > 0 {
		cursor := s.readyEOF[0]
		s.readyEOF = s.readyEOF[1:]
		if position, owned := s.positions[cursor.Partition]; owned && !s.paused[cursor.Partition] && position == cursor.Position {
			return core.Event{EOF: &cursor}, nil
		}
	}
	for id, record := range s.held {
		if !s.paused[id] {
			delete(s.held, id)
			return s.deliver(record)
		}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case event := <-s.bridge.events:
		return core.Event{}, s.lifecycle(event)
	case fetched := <-s.pump.fetched:
		defer close(fetched.done)
		if fetched.err != nil {
			if !s.transient(fetched.err) {
				return core.Event{}, fetched.err
			}
		} else {
			s.retrying = false
		}
		if len(fetched.records) == 0 {
			return core.Event{}, nil
		}
		record := fetched.records[0]
		if s.paused[record.Partition] {
			if s.held[record.Partition] != nil {
				return core.Event{}, fmt.Errorf("multiple held records for paused partition")
			}
			s.held[record.Partition] = record
			return core.Event{}, nil
		}
		return s.deliver(record)
	case <-timer.C:
		return core.Event{}, s.checkTips()
	case <-s.query.Done():
		return core.Event{}, s.query.Err()
	case <-s.ctx.Done():
		return core.Event{}, s.ctx.Err()
	}
}
func (s *Source) deliver(r *kgo.Record) (core.Event, error) {
	position, owned := s.positions[r.Partition]
	if !owned || r.Offset < position {
		return core.Event{}, nil
	}
	s.positions[r.Partition] = r.Offset + 1
	cursor := core.Cursor{Partition: r.Partition, Position: r.Offset}
	if r.Attrs.IsControl() {
		cursor.Position++
		return core.Event{EOF: &cursor}, nil
	}
	record := core.Record{Key: r.Key, Value: r.Value, TimestampType: "unknown", Source: &core.Provenance{Cluster: s.clusterID, TopicID: s.originalTopicID, Topic: r.Topic, Partition: r.Partition, Offset: r.Offset}}
	if kind := r.Attrs.TimestampType(); kind >= 0 && r.Timestamp.UnixMilli() != -1 {
		ms := r.Timestamp.UnixMilli()
		record.TimestampMS = &ms
		record.TimestampType = "create_time"
		if kind == 1 {
			record.TimestampType = "log_append_time"
		}
	}
	for _, h := range r.Headers {
		record.Headers = append(record.Headers, core.Header{Name: h.Key, Value: h.Value})
	}
	s.checkRetention(r.Partition, record.TimestampMS)
	return core.Event{Input: &core.Input{Cursor: cursor, Next: r.Offset + 1, Record: record}}, nil
}
func (s *Source) checkRetention(id int32, stamp *int64) {
	check, ok := s.retention[id]
	if !ok {
		return
	}
	delete(s.retention, id)
	if stamp != nil && *stamp > check.Lower {
		s.log.Warn("requested time may precede retained data", "partition", id, "log_start", check.Low, "requested_timestamp", check.Lower, "first_timestamp", *stamp)
	}
}
func (s *Source) Pause(id int32, paused bool) error {
	s.paused[id] = paused
	parts := map[string][]int32{s.plan.Source.Name: {id}}
	if paused {
		s.client.PauseFetchPartitions(parts)
	} else {
		s.client.ResumeFetchPartitions(parts)
	}
	return nil
}
func (s *Source) HighWatermark(id int32) (int64, error) {
	ctx, cancel := context.WithTimeout(s.query, 2*time.Second)
	defer cancel()
	ends, err := s.lookup.ReplicatedEnd(ctx, []int32{id})
	if err != nil {
		return 0, transientFailure(err)
	}
	return ends[id], nil
}
func (s *Source) transient(err error) bool {
	var retry *core.Recoverable
	if !errors.As(transientFailure(err), &retry) {
		return false
	}
	if !s.retrying {
		s.log.Warn("source unavailable; Kafka client will retry")
	}
	s.retrying = true
	return true
}
func (s *Source) Close() error {
	s.cancel()
	force := time.AfterFunc(6*time.Second, s.clientCancel)
	defer force.Stop()
	if s.pump != nil {
		<-s.pump.done
	}
	s.client.AllowRebalance()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.client.LeaveGroupContext(ctx)
	s.clientCancel()
	s.client.CloseAllowingRebalance()
	return err
}
