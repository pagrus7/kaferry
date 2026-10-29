package franz

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	core "github.com/pagrus7/kaferry/internal/copy"
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
	ctx            context.Context
	cancel         context.CancelFunc
	records, bytes atomic.Int64
	fatal          atomic.Pointer[core.Delivery]
	stamped        int64
	partitionCount int
	lastMetadata   time.Time
}

func (s *Sink) Submit(record core.Record, token core.Token) error {
	if s.preserve && (record.Source == nil || record.Source.Partition < 0) {
		return fmt.Errorf("partition preservation requires valid original source partition metadata")
	}
	if err := s.validate(record); err != nil {
		return err
	}
	size := record.Size()
	if s.records.Load() >= s.settings.queueRecords || s.records.Load() > 0 && s.bytes.Load()+size > s.settings.queueBytes {
		return &core.Busy{}
	}
	s.inspectRouting()
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
	writeCtx, cancel := context.WithTimeout(s.ctx, s.settings.delivery)
	s.records.Add(1)
	s.bytes.Add(size)
	// TryProduce never waits for a full client buffer. Timeouts may have appended
	// a record; an unsuccessful delivery leaves its source position unconfirmed.
	s.client.TryProduce(writeCtx, r, core.Cursor{Partition: token.Partition, Position: token.Next}, func(_ *kgo.Record, err error) {
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
func (s *Sink) Flush() error {
	ctx, cancel := context.WithTimeout(s.ctx, s.settings.delivery)
	defer cancel()
	return s.client.Flush(ctx)
}
func (s *Sink) Close() {
	s.cancel()
	s.client.Close()
	if s.stamped > 0 {
		s.log.Warn("records received producer timestamps", "count", s.stamped)
	}
}
func (s *Sink) inspectRouting() {
	if s.clock.Now().Sub(s.lastMetadata) < 30*time.Second {
		return
	}
	s.lastMetadata = s.clock.Now()
	ctx, cancel := context.WithTimeout(s.ctx, time.Second)
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
