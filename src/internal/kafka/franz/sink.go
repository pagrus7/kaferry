package franz

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	core "github.com/pagrus7/kaferry/src/internal/copy"
	"github.com/twmb/franz-go/pkg/kgo"
)

type destinationMetadata interface {
	Config(context.Context) map[string]string
	DestinationPartitions(context.Context) (int, error)
}
type Sink struct {
	client         ProducerClient
	preserve       bool
	topic          string
	settings       settings
	configuration  map[string]string
	inspector      destinationMetadata
	clock          core.Clock
	log            *slog.Logger
	reports        chan core.Delivery
	cancel         context.CancelFunc
	records, bytes atomic.Int64
	fatal          atomic.Pointer[core.Delivery]
	flushes        sync.WaitGroup
	workMu         sync.Mutex
	writes         map[*kgo.Record]context.CancelFunc
	flushCancel    context.CancelFunc
	stamped        int64
	partitionCount int
	lastMetadata   time.Time
}

func (s *Sink) Submit(ctx context.Context, record core.Record, token core.Token) error {
	if s.preserve && (record.Source == nil || record.Source.Partition < 0) {
		return fmt.Errorf("partition preservation requires valid original source partition metadata")
	}
	if err := s.validate(record); err != nil {
		return err
	}
	size := record.Size()
	if s.records.Load() >= s.settings.queueRecords || s.records.Load() > 0 && s.bytes.Load()+size > s.settings.queueBytes {
		return core.ErrBusy
	}
	s.inspectRouting(ctx)
	r := &kgo.Record{Topic: s.topic, Key: record.Key, Value: record.Value}
	if s.preserve {
		r.Partition = record.Source.Partition
	}
	if record.TimestampMS != nil && s.configuration["message.timestamp.type"] != "LogAppendTime" {
		r.Timestamp = time.UnixMilli(*record.TimestampMS)
	}
	for _, h := range record.Headers {
		r.Headers = append(r.Headers, kgo.RecordHeader{Key: h.Name, Value: h.Value})
	}
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.settings.delivery)
	s.workMu.Lock()
	if s.writes == nil {
		s.writes = make(map[*kgo.Record]context.CancelFunc)
	}
	s.writes[r] = cancel
	s.workMu.Unlock()
	s.records.Add(1)
	s.bytes.Add(size)
	// TryProduce never waits for a full client buffer. Timeouts may have appended
	// a record; an unsuccessful delivery leaves its source position unconfirmed.
	s.client.TryProduce(writeCtx, r, func(_ *kgo.Record, err error) {
		s.workMu.Lock()
		delete(s.writes, r)
		s.workMu.Unlock()
		cancel()
		select {
		case s.reports <- core.Delivery{Token: token, Err: err}:
		default:
			s.fatal.CompareAndSwap(nil, &core.Delivery{Fatal: true, Err: fmt.Errorf("producer delivery queue exceeded the worker record cap")})
		}
		s.records.Add(-1)
		s.bytes.Add(-size)
	})
	if record.TimestampMS == nil {
		s.stamped++
	}
	return nil
}
func (s *Sink) Deliveries() <-chan core.Delivery {
	if failure := s.fatal.Load(); failure != nil {
		select {
		case s.reports <- *failure:
			s.fatal.CompareAndSwap(failure, nil)
		default:
		}
	}
	return s.reports
}
func (s *Sink) Flush(ctx context.Context) error {
	s.workMu.Lock()
	if s.flushCancel == nil {
		flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		s.flushCancel = cancel
		s.flushes.Add(1)
		go func() {
			defer s.flushes.Done()
			defer cancel()
			defer func() {
				s.workMu.Lock()
				s.flushCancel = nil
				s.workMu.Unlock()
			}()
			_ = s.client.Flush(flushCtx)
		}()
	}
	s.workMu.Unlock()
	return nil
}
func (s *Sink) Close() {
	s.cancel()
	s.workMu.Lock()
	cancels := make([]context.CancelFunc, 0, len(s.writes)+1)
	for _, cancel := range s.writes {
		cancels = append(cancels, cancel)
	}
	if s.flushCancel != nil {
		cancels = append(cancels, s.flushCancel)
	}
	s.workMu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	s.client.Close()
	s.flushes.Wait()
	if s.stamped > 0 {
		s.log.Warn("records received producer timestamps", "count", s.stamped)
	}
}
func (s *Sink) inspectRouting(ctx context.Context) {
	if s.clock.Now().Sub(s.lastMetadata) < 30*time.Second {
		return
	}
	s.lastMetadata = s.clock.Now()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer cancel()
	count, err := s.inspector.DestinationPartitions(ctx)
	if err != nil {
		s.log.Warn("destination partition count check failed")
		return
	}
	if count != s.partitionCount {
		s.log.Warn("destination partition count changed", "topic", s.topic, "previous", s.partitionCount, "current", count)
		s.partitionCount = count
	}
}
func (s *Sink) validate(record core.Record) error {
	size := kafkaRecordBufferBytes(record)
	if size > s.settings.messageBytes {
		return &core.RecordError{Class: "validation", Err: fmt.Errorf("record exceeds producer size limit")}
	}
	if size > s.settings.queueBytes {
		return &core.RecordError{Class: "validation", Err: fmt.Errorf("record exceeds producer queue byte limit")}
	}
	if record.TimestampMS == nil || s.configuration["message.timestamp.type"] == "LogAppendTime" {
		return nil
	}
	ts := *record.TimestampMS
	if ts < math.MinInt64/1000000 || ts > math.MaxInt64/1000000 {
		return &core.RecordError{Class: "rejected_timestamp", Err: fmt.Errorf("timestamp outside client-safe range")}
	}
	if s.configuration["message.timestamp.type"] != "CreateTime" {
		return nil
	}
	now := s.clock.Now().UnixMilli()
	before, beforeKnown := timestampLimit(s.configuration, "before")
	after, afterKnown := timestampLimit(s.configuration, "after")
	if ts < now && beforeKnown && now-ts > before || ts > now && afterKnown && ts-now > after {
		return &core.RecordError{Class: "rejected_timestamp", Err: fmt.Errorf("timestamp exceeds destination acceptance bounds")}
	}
	return nil
}
func timestampLimit(configuration map[string]string, direction string) (int64, bool) {
	value, err := strconv.ParseInt(configuration["message.timestamp."+direction+".max.ms"], 10, 64)
	if err != nil || value == math.MaxInt64 {
		value, err = strconv.ParseInt(configuration["message.timestamp.difference.max.ms"], 10, 64)
	}
	return value, err == nil && value >= 0
}
func (s settings) withTopicLimit(configuration map[string]string) settings {
	if value, err := strconv.ParseInt(configuration["max.message.bytes"], 10, 64); err == nil && value >= 0 && (!s.explicitMessageBytes || value < s.messageBytes) {
		s.messageBytes = max(1000, min(value, 1000000000))
	}
	return s
}
