//go:build franzspike

// Package franzspike is an experimental adapter, excluded from normal builds.
package franzspike

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Only this preflight subset is implemented; this is not a production config mapper.
func endpointClient(e plan.Endpoint, opts ...kgo.Opt) (*kgo.Client, error) {
	for key := range e.Properties.Values() {
		switch key {
		case "bootstrap.servers", "linger.ms":
		default:
			return nil, fmt.Errorf("spike does not map connection key %s", key)
		}
	}
	options := []kgo.Opt{kgo.SeedBrokers(strings.Split(e.Properties.Get("bootstrap.servers"), ",")...), kgo.RequestTimeoutOverhead(2 * time.Second), kgo.RetryTimeout(5 * time.Second)}
	return kgo.NewClient(append(options, opts...)...)
}

type progress struct {
	Fingerprint string       `json:"fp"`
	TopicID     string       `json:"tid"`
	Window      *plan.Window `json:"win,omitempty"`
	FileID      string       `json:"fid,omitempty"`
	Extent      int64        `json:"extent,omitempty"`
}

func (p progress) encode() string { data, _ := json.Marshal(p); return "kfwd2:1;" + string(data) }

type lifeEvent struct {
	assign   []core.Assignment
	meta     map[int32]progress
	revoke   []int32
	lost     bool
	isRevoke bool
	done     chan error
}
type fetchResult struct {
	records []*kgo.Record
	err     error
	done    chan struct{}
}

type Source struct {
	client               *kgo.Client
	admin                *kadm.Client
	adminClient          *kgo.Client
	plan                 plan.Plan
	fingerprint, topicID string
	ctx                  context.Context
	cancel               context.CancelFunc
	clientCancel         context.CancelFunc
	pumpDone             chan struct{}
	life                 core.Lifecycle
	events               chan lifeEvent
	fetched              chan fetchResult
	pending              *fetchResult
	held                 map[int32]*kgo.Record
	positions            map[int32]int64
	metadata             map[int32]progress
	paused               map[int32]bool
	lastTip              time.Time
	readyEOF             []core.Cursor
	output               core.OutputProgress
}

func NewSource(p plan.Plan, fp, topicID string) (*Source, error) {
	background, cancel := context.WithCancel(context.Background())
	a, err := endpointClient(p.Source)
	if err != nil {
		cancel()
		return nil, err
	}
	s := &Source{admin: kadm.NewClient(a), adminClient: a, plan: p, fingerprint: fp, topicID: topicID, ctx: background, cancel: cancel, events: make(chan lifeEvent), fetched: make(chan fetchResult), positions: make(map[int32]int64), metadata: make(map[int32]progress), paused: make(map[int32]bool), held: make(map[int32]*kgo.Record)}
	desired := make(map[int32]int64) // Accessed only by serialized group callbacks.
	notify := func(event lifeEvent) error {
		event.done = make(chan error, 1)
		select {
		case s.events <- event:
		case <-background.Done():
			return background.Err()
		}
		select {
		case err := <-event.done:
			return err
		case <-background.Done():
			return background.Err()
		}
	}
	clientCtx, clientCancel := context.WithCancel(context.Background())
	s.clientCancel = clientCancel
	c, err := endpointClient(p.Source, kgo.WithContext(clientCtx), kgo.ConsumerGroup(p.Group), kgo.ClientID("kaferry-"+p.Group), kgo.ConsumeTopics(p.Source.Name), kgo.DisableAutoCommit(), kgo.ConsumeResetOffset(kgo.NoResetOffset()), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.FetchMaxWait(500*time.Millisecond), kgo.KeepControlRecords(), kgo.BlockRebalanceOnPoll(), kgo.Balancers(kgo.CooperativeStickyBalancer()), kgo.OnPartitionsAssigned(func(ctx context.Context, _ *kgo.Client, parts map[string][]int32) {
		assignments, metadata, err := s.locate(ctx, parts[p.Source.Name])
		if err != nil {
			select {
			case s.fetched <- fetchResult{err: err}:
			case <-background.Done():
			}
			return
		}
		for _, a := range assignments {
			desired[a.Cursor.Partition] = a.Cursor.Position
		}
		_ = notify(lifeEvent{assign: assignments, meta: metadata})
	}), kgo.AdjustFetchOffsetsFn(func(_ context.Context, offsets map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
		for part := range offsets[p.Source.Name] {
			if position, ok := desired[part]; ok {
				offsets[p.Source.Name][part] = kgo.NewOffset().At(position)
			}
		}
		return offsets, nil
	}), kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, parts map[string][]int32) {
		_ = notify(lifeEvent{revoke: parts[p.Source.Name], isRevoke: true})
	}), kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, parts map[string][]int32) {
		_ = notify(lifeEvent{revoke: parts[p.Source.Name], lost: true, isRevoke: true})
	}))
	if err != nil {
		cancel()
		clientCancel()
		a.Close()
		return nil, err
	}
	s.client = c
	return s, nil
}
func (s *Source) locate(parent context.Context, ids []int32) ([]core.Assignment, map[int32]progress, error) {
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	low, err := s.admin.ListStartOffsets(ctx, s.plan.Source.Name)
	if err != nil {
		return nil, nil, err
	}
	high, err := s.admin.ListEndOffsets(ctx, s.plan.Source.Name)
	if err != nil {
		return nil, nil, err
	}
	stable, err := s.admin.ListCommittedOffsets(ctx, s.plan.Source.Name)
	if err != nil {
		return nil, nil, err
	}
	saved, err := s.admin.FetchOffsets(ctx, s.plan.Group)
	if err != nil && !errors.Is(err, kerr.GroupIDNotFound) {
		return nil, nil, err
	}
	window, err := s.plan.Window.Resolve(time.Now())
	if err != nil {
		return nil, nil, err
	}
	metadata := make(map[int32]progress)
	var assignments []core.Assignment
	for _, id := range ids {
		p := progress{Fingerprint: s.fingerprint, TopicID: s.topicID}
		old := saved[s.plan.Source.Name][id]
		position := low[s.plan.Source.Name][id].Offset
		if old.At >= 0 && old.Metadata != "" {
			if !strings.HasPrefix(old.Metadata, "kfwd2:1;") {
				return nil, nil, fmt.Errorf("foreign progress")
			}
			if err := json.Unmarshal([]byte(strings.TrimPrefix(old.Metadata, "kfwd2:1;")), &p); err != nil {
				return nil, nil, err
			}
			if p.Fingerprint != s.fingerprint || p.TopicID != s.topicID {
				return nil, nil, fmt.Errorf("progress identity mismatch")
			}
			position = old.At
			if p.Window != nil {
				window = *p.Window
			}
		} else if window.Lower != nil && *window.Lower > 0 {
			position, err = s.timestampOffset(ctx, id, *window.Lower)
			if err != nil {
				return nil, nil, err
			}
			if position < 0 {
				position = stable[s.plan.Source.Name][id].Offset
			}
		} else if window.Lower == nil && window.Upper == nil && s.plan.Start == "LATEST" {
			position = stable[s.plan.Source.Name][id].Offset
		}
		position = max(position, low[s.plan.Source.Name][id].Offset)
		if position > high[s.plan.Source.Name][id].Offset {
			return nil, nil, fmt.Errorf("saved position exceeds end")
		}
		if s.plan.Window.Relative() {
			w := window
			p.Window = &w
		}
		metadata[id] = p
		assignments = append(assignments, core.Assignment{Cursor: core.Cursor{Partition: id, Position: position}, Window: window})
	}
	return assignments, metadata, nil
}
func (s *Source) timestampOffset(ctx context.Context, id int32, stamp int64) (int64, error) {
	req := kmsg.NewPtrListOffsetsRequest()
	req.IsolationLevel = 1
	topic := kmsg.NewListOffsetsRequestTopic()
	topic.Topic = s.plan.Source.Name
	part := kmsg.NewListOffsetsRequestTopicPartition()
	part.Partition = id
	part.Timestamp = stamp
	topic.Partitions = []kmsg.ListOffsetsRequestTopicPartition{part}
	req.Topics = []kmsg.ListOffsetsRequestTopic{topic}
	res, err := req.RequestWith(ctx, s.adminClient)
	if err != nil {
		return 0, err
	}
	result := res.Topics[0].Partitions[0]
	return result.Offset, kerr.ErrorForCode(result.ErrorCode)
}
func (s *Source) Start(life core.Lifecycle) error {
	s.life = life
	s.pumpDone = make(chan struct{})
	go func() {
		defer close(s.pumpDone)
		for {
			s.client.AllowRebalance()
			fetch := s.client.PollRecords(s.ctx, 1)
			if s.ctx.Err() != nil {
				return
			}
			result := fetchResult{records: fetch.Records(), err: fetch.Err(), done: make(chan struct{})}
			select {
			case s.fetched <- result:
			case <-s.ctx.Done():
				return
			}
			select {
			case <-result.done:
			case <-s.ctx.Done():
				return
			}
		}
	}()
	return nil
}
func (s *Source) Poll(timeout time.Duration) (core.Event, error) {
	for len(s.readyEOF) > 0 {
		cursor := s.readyEOF[0]
		s.readyEOF = s.readyEOF[1:]
		if _, owned := s.positions[cursor.Partition]; owned && !s.paused[cursor.Partition] {
			return core.Event{EOF: &cursor}, nil
		}
	}
	if s.pending != nil {
		return s.deliverPending()
	}
	for id, record := range s.held {
		if !s.paused[id] {
			delete(s.held, id)
			return s.deliverRecord(record)
		}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case event := <-s.events:
		var err error
		if event.isRevoke {
			err = s.life.Revoke(event.revoke, event.lost)
			for _, id := range event.revoke {
				delete(s.positions, id)
				s.client.ResumeFetchPartitions(map[string][]int32{s.plan.Source.Name: {id}})
				delete(s.paused, id)
				delete(s.held, id)
				delete(s.metadata, id)
				if s.pending != nil && len(s.pending.records) > 0 && s.pending.records[0].Partition == id {
					close(s.pending.done)
					s.pending = nil
				}
			}
		} else {
			for _, a := range event.assign {
				s.positions[a.Cursor.Partition] = a.Cursor.Position
				s.metadata[a.Cursor.Partition] = event.meta[a.Cursor.Partition]
			}
			err = s.life.Assign(event.assign)
		}
		event.done <- err
		if err == nil && !event.isRevoke {
			var cursors []core.Cursor
			for _, a := range event.assign {
				cursors = append(cursors, a.Cursor)
			}
			deadline, cancel := context.WithTimeout(s.ctx, 10*time.Second)
			defer cancel()
			result, failure := s.Save(deadline, cursors)
			err = failure
			if err == nil {
				for _, failure := range result {
					if failure != nil {
						err = failure
						break
					}
				}
			}
		}
		return core.Event{}, err
	case result := <-s.fetched:
		if result.err != nil {
			if result.done != nil {
				close(result.done)
			}
			return core.Event{}, result.err
		}
		s.pending = &result
		return s.deliverPending()
	case <-timer.C:
		if time.Since(s.lastTip) < time.Second {
			return core.Event{}, nil
		}
		s.lastTip = time.Now()
		deadline, cancel := context.WithTimeout(s.ctx, 2*time.Second)
		defer cancel()
		ends, err := s.admin.ListCommittedOffsets(deadline, s.plan.Source.Name)
		if err != nil {
			return core.Event{}, err
		}
		if err := ends.Error(); err != nil {
			return core.Event{}, err
		}
		for id, position := range s.positions {
			end, ok := ends.Lookup(s.plan.Source.Name, id)
			if !ok {
				return core.Event{}, fmt.Errorf("offset response omitted partition %d", id)
			}
			if position >= end.Offset && !s.paused[id] {
				s.readyEOF = append(s.readyEOF, core.Cursor{Partition: id, Position: position})
			}
		}
		if len(s.readyEOF) > 0 {
			return s.Poll(0)
		}
		return core.Event{}, nil
	case <-s.ctx.Done():
		return core.Event{}, s.ctx.Err()
	}
}
func (s *Source) deliverPending() (core.Event, error) {
	result := s.pending
	s.pending = nil
	close(result.done)
	if len(result.records) == 0 {
		return core.Event{}, nil
	}
	r := result.records[0]
	if s.paused[r.Partition] {
		if s.held[r.Partition] != nil {
			return core.Event{}, fmt.Errorf("multiple held records for paused partition")
		}
		s.held[r.Partition] = r
		return core.Event{}, nil
	}
	return s.deliverRecord(r)
}
func (s *Source) deliverRecord(r *kgo.Record) (core.Event, error) {
	s.positions[r.Partition] = r.Offset + 1
	cursor := core.Cursor{Partition: r.Partition, Position: r.Offset}
	if r.Attrs.IsControl() {
		cursor.Position = r.Offset + 1
		return core.Event{EOF: &cursor}, nil
	}
	record := core.Record{Key: r.Key, Value: r.Value, TimestampType: "unknown", Source: &core.Provenance{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset}}
	if kind := r.Attrs.TimestampType(); kind >= 0 {
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
	return core.Event{Input: &core.Input{Cursor: cursor, Next: r.Offset + 1, Record: record}}, nil
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
	deadline, cancel := context.WithTimeout(s.ctx, 2*time.Second)
	defer cancel()
	ends, err := s.admin.ListEndOffsets(deadline, s.plan.Source.Name)
	if err != nil {
		return 0, err
	}
	if err := ends.Error(); err != nil {
		return 0, err
	}
	r, ok := ends.Lookup(s.plan.Source.Name, id)
	if !ok {
		return 0, fmt.Errorf("offset response omitted partition %d", id)
	}
	return r.Offset, r.Err
}
func (s *Source) BindOutput(output core.OutputProgress) { s.output = output }
func (s *Source) Save(ctx context.Context, cursors []core.Cursor) (map[int32]error, error) {
	if len(cursors) == 0 {
		return map[int32]error{}, nil
	}
	offsets := map[string]map[int32]kgo.EpochOffset{s.plan.Source.Name: {}}
	for _, cursor := range cursors {
		offsets[s.plan.Source.Name][cursor.Partition] = kgo.EpochOffset{Epoch: -1, Offset: cursor.Position}
	}
	commitCtx := kgo.PreCommitFnContext(ctx, func(req *kmsg.OffsetCommitRequest) error {
		for i := range req.Topics {
			for j := range req.Topics[i].Partitions {
				part := &req.Topics[i].Partitions[j]
				p := s.metadata[part.Partition]
				if s.output != nil {
					p.FileID, p.Extent = s.output.DurableOutput()
				}
				value := p.encode()
				part.Metadata = &value
			}
		}
		return nil
	})
	var failure error
	results := make(map[int32]error)
	s.client.CommitOffsetsSync(commitCtx, offsets, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, res *kmsg.OffsetCommitResponse, err error) {
		failure = err
		if res != nil {
			for _, topic := range res.Topics {
				for _, part := range topic.Partitions {
					results[part.Partition] = kerr.ErrorForCode(part.ErrorCode)
				}
			}
		}
	})
	return results, failure
}
func (s *Source) Close() error {
	s.cancel()
	// A canceled poll can still set the rebalance block before returning.
	// Wait for it before allowing the final revoke.
	force := time.AfterFunc(2*time.Second, s.clientCancel)
	defer force.Stop()
	if s.pumpDone != nil {
		<-s.pumpDone
	}
	s.client.AllowRebalance()
	deadline, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := s.client.LeaveGroupContext(deadline)
	s.clientCancel()
	s.client.CloseAllowingRebalance()
	s.adminClient.Close()
	return err
}

type Sink struct {
	client          *kgo.Client
	reports         chan core.Delivery
	limits          config.Worker
	deliveryTimeout time.Duration
	flushing        atomic.Bool
	flushes         sync.WaitGroup
	ctx             context.Context
	cancel          context.CancelFunc
}

func NewSink(p plan.Plan, limits config.Worker) (*Sink, error) {
	duration, err := time.ParseDuration(limits.DeliveryTimeout)
	if err != nil {
		return nil, err
	}
	background, cancel := context.WithCancel(context.Background())
	opts := []kgo.Opt{kgo.WithContext(background), kgo.DefaultProduceTopic(p.Destination.Name), kgo.ProducerLinger(5 * time.Millisecond), kgo.ProducerBatchCompression(kgo.NoCompression()), kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)), kgo.MaxBufferedRecords(limits.RecordCap), kgo.MaxBufferedBytes(int(max(limits.ByteCap, 1000000)))}
	if value := p.Destination.Properties.Get("linger.ms"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil {
			cancel()
			return nil, err
		}
		opts = append(opts, kgo.ProducerLinger(time.Duration(n)*time.Millisecond))
	}
	c, err := endpointClient(p.Destination, opts...)
	if err != nil {
		cancel()
		return nil, err
	}
	return &Sink{client: c, reports: make(chan core.Delivery, limits.RecordCap+1), limits: limits, deliveryTimeout: duration, ctx: background, cancel: cancel}, nil
}
func (s *Sink) Submit(record core.Record, token core.Token) error {
	if record.Size() > 1000000 {
		return &core.RecordError{Class: "size", Err: fmt.Errorf("record exceeds spike's fixed message limit")}
	}
	if s.client.BufferedProduceRecords() >= int64(s.limits.RecordCap) {
		return &core.Busy{}
	}
	if s.client.BufferedProduceRecords() > 0 && s.client.BufferedProduceBytes()+record.Size() > s.limits.ByteCap {
		return &core.Busy{}
	}
	r := &kgo.Record{Key: record.Key, Value: record.Value}
	if record.TimestampMS != nil {
		ms := *record.TimestampMS
		if ms < -9223372036854 || ms > 9223372036854 {
			return &core.RecordError{Class: "rejected_timestamp", Err: fmt.Errorf("timestamp outside client-safe range")}
		}
		r.Timestamp = time.UnixMilli(ms)
	}
	for _, h := range record.Headers {
		r.Headers = append(r.Headers, kgo.RecordHeader{Key: h.Name, Value: h.Value})
	}
	// A copied timestamp must not determine how long a new write may take.
	writeCtx, cancel := context.WithTimeout(s.ctx, s.deliveryTimeout)
	s.client.Produce(writeCtx, r, func(_ *kgo.Record, err error) {
		cancel()
		select {
		case s.reports <- core.Delivery{Token: token, Err: err, Fatal: err != nil}:
		case <-s.ctx.Done():
		}
	})
	return nil
}
func (s *Sink) Flush() error {
	if s.flushing.CompareAndSwap(false, true) {
		s.flushes.Add(1)
		go func() { defer s.flushes.Done(); defer s.flushing.Store(false); _ = s.client.Flush(s.ctx) }()
	}
	return nil
}
func (s *Sink) Deliveries() <-chan core.Delivery { return s.reports }
func (s *Sink) Close()                           { s.cancel(); s.client.Close(); s.flushes.Wait() }

func EndpointIdentity(ctx context.Context, e plan.Endpoint) (cluster, topicID string, err error) {
	if e.Kind == "FILE" {
		return "", "", nil
	}
	c, err := endpointClient(e)
	if err != nil {
		return "", "", err
	}
	defer c.Close()
	m, err := kadm.NewClient(c).Metadata(ctx, e.Name)
	if err != nil {
		return "", "", err
	}
	topic := m.Topics[e.Name]
	if topic.Err != nil {
		return "", "", topic.Err
	}
	return m.Cluster, base64.RawStdEncoding.EncodeToString(topic.ID[:]), nil
}

var _ core.Source = (*Source)(nil)
var _ core.Sink = (*Sink)(nil)
var _ core.ProgressStore = (*Source)(nil)
