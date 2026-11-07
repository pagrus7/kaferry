//go:build integration && load

package compat

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	core "github.com/pagrus7/kaferry/src/internal/copy"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func loadRecordCount(t *testing.T, name string, fallback, maximum int) int {
	t.Helper()
	text := os.Getenv(name)
	if text == "" {
		return fallback
	}
	count, err := strconv.Atoi(text)
	require.NoError(t, err, "%s must be an integer", name)
	require.Greater(t, count, 0, "%s must be positive", name)
	require.LessOrEqual(t, count, maximum, "%s exceeds the fixture's memory budget", name)
	return count
}

type streamRecords struct {
	topic                  string
	stamp                  time.Time
	headers                []kgo.RecordHeader
	seen                   map[int]bool
	published, contiguous  int
	order                  map[[2]int32]int
	verified, acknowledged int
	received, unique       int
	duplicates, corrupted  int
	unexpected, reordered  int
	publishErrors          int
	next                   []int
	offsets                map[int32]int64
	nonblocking            bool
	keyRouting             kgo.TopicPartitioner
	destinationPartitions  int
	observer               *deliveryObserver
	integrity              *streamIntegrity
}

type soakDelivery struct {
	Submitted              int               `json:"submitted"`
	Published              int               `json:"published"`
	PublishErrors          int               `json:"publish_errors"`
	Received               int               `json:"received"`
	UniqueReceived         int               `json:"unique_received"`
	Verified               int               `json:"verified"`
	Missing                *int              `json:"missing"`
	Pending                int               `json:"pending"`
	Duplicates             int               `json:"duplicates"`
	Corrupted              int               `json:"corrupted"`
	Unexpected             int               `json:"unexpected"`
	Reordered              int               `json:"reordered"`
	ReconciliationComplete bool              `json:"reconciliation_complete"`
	Observed               *observedDelivery `json:"observed,omitempty"`
	Integrity              integrityEvidence `json:"integrity"`
}

func (s *streamRecords) delivery(complete bool) soakDelivery {
	r := soakDelivery{Submitted: s.published, Published: s.acknowledged, PublishErrors: s.publishErrors,
		Received: s.received, UniqueReceived: s.unique, Verified: s.verified,
		Pending: s.published - s.unique, Duplicates: s.duplicates, Corrupted: s.corrupted,
		Unexpected: s.unexpected, Reordered: s.reordered,
		ReconciliationComplete: complete && s.acknowledged == s.published && s.publishErrors == 0}
	if r.ReconciliationComplete {
		missing := r.Pending
		r.Missing = &missing
	}
	if s.observer != nil {
		observed := s.observer.snapshot()
		r.Observed = &observed
	}
	r.Integrity = s.integrity.snapshot(r.ReconciliationComplete, s.published, s.unique)
	return r
}

func newStreamRecords(topic string, stamp time.Time) *streamRecords {
	return newPartitionedStreamRecords(topic, stamp, 4)
}

// A finite copy may be read one destination partition at a time after key routing.
// Bound both ID and digest history by the entire input, independent of arrival order.
func newFiniteStreamRecords(topic string, stamp time.Time, count int) *streamRecords {
	s := newStreamRecords(topic, stamp)
	s.integrity.maxPending = count
	return s
}

func newPartitionedStreamRecords(topic string, stamp time.Time, partitions int) *streamRecords {
	next := make([]int, partitions)
	for i := range next {
		next[i] = i
	}
	return &streamRecords{topic: topic, stamp: stamp, headers: []kgo.RecordHeader{{Key: "trace", Value: []byte{0, 255}}, {Key: "dup", Value: nil}, {Key: "dup", Value: []byte{}}}, seen: make(map[int]bool), order: make(map[[2]int32]int), next: next, offsets: make(map[int32]int64), integrity: newPartitionedStreamIntegrity(partitions)}
}
func (s *streamRecords) publish(t *testing.T, ctx context.Context, producer *kgo.Client, reports chan kgo.ProduceResult, count int) {
	t.Helper()
	for range count {
		id := s.published
		payload := archiveLoadRecordPartitions(id, len(s.next)).Value
		key := []byte(fmt.Sprintf("key-%d", id%257))
		if id%7 == 0 {
			key = nil
		}
		message := &kgo.Record{Topic: s.topic, Partition: int32(id % len(s.next)), Key: key, Value: payload, Timestamp: s.stamp, Headers: s.headers}
		s.integrity.submit(id, message)
		callback := deliveryReporter(ctx, reports)
		if observer := s.observer; observer != nil {
			observer.submit(id)
			report := callback
			callback = func(record *kgo.Record, err error) {
				// Observe before report-channel buffering can delay the acknowledgement.
				observer.publication(id, err)
				report(record, err)
			}
		}
		if s.nonblocking {
			producer.TryProduce(ctx, message, callback)
		} else {
			producer.Produce(ctx, message, callback)
		}
		s.published++
	}
}
func (s *streamRecords) verify(t *testing.T, record *kgo.Record) {
	t.Helper()
	s.offsets[record.Partition] = max(s.offsets[record.Partition], record.Offset+1)
	id, unique := s.receive(t, record.Value)
	if !unique {
		return
	}
	s.integrity.receive(t, id, record.Key, record.Value, record.Headers)
	key := []byte(fmt.Sprintf("key-%d", id%257))
	if id%7 == 0 {
		key = nil
	}
	if !reflect.DeepEqual(key, record.Key) || !record.Timestamp.Equal(s.stamp) ||
		!reflect.DeepEqual(s.headers, record.Headers) || !reflect.DeepEqual(archiveLoadRecordPartitions(id, len(s.next)).Value, record.Value) {
		s.corrupted++
		return
	}
	if s.keyRouting != nil && record.Key != nil && int(record.Partition) != s.keyRouting.Partition(record, s.destinationPartitions) {
		s.corrupted++
		return
	}
	pair := [2]int32{int32(id % len(s.next)), record.Partition}
	if prior, exists := s.order[pair]; exists && id <= prior {
		s.reordered++
	}
	s.order[pair] = max(s.order[pair], id)
	s.verified++
}

func (s *streamRecords) verifyArchive(t *testing.T, record core.Record, identity *core.Provenance) {
	t.Helper()
	id, unique := s.receive(t, record.Value)
	if !unique {
		return
	}
	headers := make([]kgo.RecordHeader, len(record.Headers))
	for i, header := range record.Headers {
		headers[i] = kgo.RecordHeader{Key: header.Name, Value: header.Value}
	}
	s.integrity.receive(t, id, record.Key, record.Value, headers)
	want := archiveLoadRecordPartitions(id, len(s.next))
	want.Source.Cluster, want.Source.Topic, want.Source.TopicID = identity.Cluster, identity.Topic, identity.TopicID
	stamp := s.stamp.UnixMilli()
	want.TimestampMS = &stamp
	if !reflect.DeepEqual(want, record) {
		s.corrupted++
		return
	}
	pair := [2]int32{int32(id % len(s.next)), int32(id % len(s.next))}
	if prior, exists := s.order[pair]; exists && id <= prior {
		s.reordered++
	}
	s.order[pair] = max(s.order[pair], id)
	s.verified++
}

func (s *streamRecords) receive(t *testing.T, value []byte) (int, bool) {
	t.Helper()
	s.received++
	if len(value) < 4 {
		s.unexpected++
		return 0, false
	}
	id := int(binary.BigEndian.Uint32(value))
	if id >= s.published {
		s.unexpected++
		return id, false
	}
	part := id % len(s.next)
	if id < s.next[part] || s.seen[id] {
		s.duplicates++
		return id, false
	}
	s.unique++
	if s.observer != nil {
		s.observer.receive(id)
	}
	s.seen[id] = true
	for s.seen[s.next[part]] {
		delete(s.seen, s.next[part])
		s.next[part] += len(s.next)
	}
	s.contiguous = s.next[0]
	for _, next := range s.next {
		s.contiguous = min(s.contiguous, next)
	}
	require.LessOrEqual(t, len(s.seen), s.integrity.maxPending, "verification gaps exceeded the bounded ID window; totals are partial")
	return id, true
}
func (s *streamRecords) drainReports(t *testing.T, reports <-chan kgo.ProduceResult) {
	t.Helper()
	for {
		select {
		case event := <-reports:
			if event.Err != nil {
				s.publishErrors++
			} else {
				s.acknowledged++
			}
		default:
			return
		}
	}
}
func (s *streamRecords) catchUp(t *testing.T, consumer *kgo.Client, reports <-chan kgo.ProduceResult) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Millisecond)
	defer cancel()
	fetches := consumer.PollRecords(ctx, 512)
	for _, failure := range fetches.Errors() {
		if !errors.Is(failure.Err, context.DeadlineExceeded) {
			t.Fatal(failure)
		}
	}
	for _, record := range fetches.Records() {
		s.verify(t, record)
	}

	s.drainReports(t, reports)
}
func rssSample(pid int) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	data, err := exec.CommandContext(ctx, "ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
}

type rssPeak struct {
	peak              int64
	samples, failures int
}

func watchRSS(t *testing.T, process *cliProcess) func() rssPeak {
	t.Helper()
	first, err := rssSample(process.pid())
	require.NoError(t, err, "sample CLI memory before releasing SQL input")
	require.Positive(t, first, "initial CLI RSS")
	stop, result := make(chan struct{}), make(chan rssPeak, 1)
	go func() {
		stats := rssPeak{peak: first, samples: 1}
		tick := time.NewTicker(50 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				result <- stats
				return
			case <-tick.C:
				rss, err := rssSample(process.pid())
				if err != nil {
					stats.failures++
					continue
				}
				stats.samples++
				stats.peak = max(stats.peak, rss)
			}
		}
	}()
	finish := sync.OnceValue(func() rssPeak { close(stop); return <-result })
	t.Cleanup(func() { finish() })
	return finish
}
func medianRSS(samples []int64) int64 {
	sorted := append([]int64(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return sorted[len(sorted)/2]
}
func memoryEnvelope(t *testing.T, samples []int64) (warm, final, peak int64) {
	t.Helper()
	if len(samples) < 30 {
		t.Fatal("not enough samples for memory stability")
	}
	warm, final = medianRSS(samples[10:30]), medianRSS(samples[len(samples)-20:])
	for _, sample := range samples {
		peak = max(peak, sample)
	}
	if peak > 512<<10 || final > warm+32<<10 {
		t.Fatal("memory did not stay within the soak envelope", warm, final, peak)
	}
	return
}

// measureCLI uses one timing/sampling boundary for all comparable load workloads.
func measureCLI(t *testing.T, report *loadReport, process *cliProcess, statement string) copySummary {
	t.Helper()
	report.cli = process
	report.MeasurementMethod = "sql_release_to_exit"
	stopSampling := watchRSS(t, process)
	start := time.Now()
	require.NoError(t, process.send(statement))
	err := process.wait()
	elapsed := time.Since(start)
	memory := stopSampling()
	report.ElapsedSeconds = elapsed.Seconds()
	report.ChildCPUSeconds = process.cpu().Seconds()
	report.SampledPeakRSSBytes = memory.peak * 1024
	report.RSSSamples = memory.samples
	require.NoError(t, err, process.logs())
	for _, line := range strings.Split(process.logs(), "\n") {
		var event struct {
			Time    time.Time `json:"time"`
			Message string    `json:"msg"`
		}
		if json.Unmarshal([]byte(line), &event) == nil && event.Message == "partition assigned" {
			report.ReadySeconds = event.Time.Sub(start).Seconds()
			break
		}
	}
	summary := summaryFrom(t, process.logs())
	report.Records = summary.Confirmed
	report.RecordsPerSecond = float64(report.Records) / elapsed.Seconds()
	report.PeakUncommittedRecords = summary.PeakRecords
	report.PeakUncommittedBytes = summary.PeakBytes
	return summary
}
