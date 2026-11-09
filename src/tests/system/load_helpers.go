//go:build integration && load

package compat

import (
	"context"
	"encoding/json"
	"errors"
	"os"
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
	profile                *workloadProfile
	topic                  string
	stamp                  time.Time
	headers                []kgo.RecordHeader
	published              int
	order                  map[[2]int32]int
	verified, acknowledged int
	received, unique       int
	duplicates, corrupted  int
	unexpected, reordered  int
	publishErrors          int
	offsets                map[int32]int64
	nonblocking            bool
	keyRouting             kgo.TopicPartitioner
	destinationPartitions  int
	observer               *deliveryObserver
	receipts               *receiptLedger
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
	r.Integrity = s.receipts.snapshot(r.ReconciliationComplete, s.published, s.unique)
	return r
}

func newStreamRecords(topic string, stamp time.Time) *streamRecords {
	return newPartitionedStreamRecords(topic, stamp, 4)
}

// A finite copy may be read one destination partition at a time after key routing.
// Bound both ID and digest history by the entire input, independent of arrival order.
func newFiniteStreamRecords(topic string, stamp time.Time, count int) *streamRecords {
	s := newStreamRecords(topic, stamp)
	s.receipts.maxPending = count
	return s
}

func newPartitionedStreamRecords(topic string, stamp time.Time, partitions int) *streamRecords {
	return &streamRecords{topic: topic, stamp: stamp, headers: []kgo.RecordHeader{{Key: "trace", Value: []byte{0, 255}}, {Key: "dup", Value: nil}, {Key: "dup", Value: []byte{}}}, order: make(map[[2]int32]int), offsets: make(map[int32]int64), receipts: newReceiptLedger(partitions)}
}
func (s *streamRecords) publish(t *testing.T, ctx context.Context, producer *kgo.Client, reports chan kgo.ProduceResult, count int) {
	t.Helper()
	for range count {
		id := s.published
		message := s.record(id)
		s.receipts.submit(id, message)
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
	id, unique := s.receive(t, record.Key, record.Value, record.Headers)
	if !unique {
		return
	}
	want := s.record(id)
	if !reflect.DeepEqual(want.Key, record.Key) || !record.Timestamp.Equal(s.stamp) ||
		!reflect.DeepEqual(want.Headers, record.Headers) || !reflect.DeepEqual(want.Value, record.Value) {
		s.corrupted++
		return
	}
	if s.keyRouting != nil && record.Key != nil && int(record.Partition) != s.keyRouting.Partition(record, s.destinationPartitions) {
		s.corrupted++
		return
	}
	pair := [2]int32{int32(id % len(s.receipts.next)), record.Partition}
	if prior, exists := s.order[pair]; exists && id <= prior {
		s.reordered++
	}
	s.order[pair] = max(s.order[pair], id)
	s.verified++
}

func (s *streamRecords) verifyArchive(t *testing.T, record core.Record, identity *core.Provenance) {
	t.Helper()
	headers := make([]kgo.RecordHeader, len(record.Headers))
	for i, header := range record.Headers {
		headers[i] = kgo.RecordHeader{Key: header.Name, Value: header.Value}
	}
	id, unique := s.receive(t, record.Key, record.Value, headers)
	if !unique {
		return
	}
	want := archiveLoadRecordPartitions(id, len(s.receipts.next))
	if s.profile != nil {
		message := s.record(id)
		want.Key, want.Value = message.Key, message.Value
		want.Headers = make([]core.Header, len(message.Headers))
		for i, header := range message.Headers {
			want.Headers[i] = core.Header{Name: header.Key, Value: header.Value}
		}
	}
	want.Source.Cluster, want.Source.Topic, want.Source.TopicID = identity.Cluster, identity.Topic, identity.TopicID
	stamp := s.stamp.UnixMilli()
	want.TimestampMS = &stamp
	if !reflect.DeepEqual(want, record) {
		s.corrupted++
		return
	}
	pair := [2]int32{int32(id % len(s.receipts.next)), int32(id % len(s.receipts.next))}
	if prior, exists := s.order[pair]; exists && id <= prior {
		s.reordered++
	}
	s.order[pair] = max(s.order[pair], id)
	s.verified++
}

func (s *streamRecords) receive(t *testing.T, key, value []byte, headers []kgo.RecordHeader) (int, bool) {
	t.Helper()
	s.received++
	id, valid := s.recordID(value, headers)
	if !valid {
		s.unexpected++
		return 0, false
	}
	if id >= s.published {
		s.unexpected++
		return id, false
	}
	// Observe before digest work; the observer ignores duplicate receipts itself.
	if s.observer != nil {
		s.observer.receive(id)
	}
	if !s.receipts.receive(t, id, key, value, headers) {
		s.duplicates++
		return id, false
	}
	s.unique++
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
func (s *streamRecords) catchUp(t *testing.T, consumer *kgo.Client, reports <-chan kgo.ProduceResult) time.Duration {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Millisecond)
	defer cancel()
	pollStarted := time.Now()
	fetches := consumer.PollRecords(ctx, 512)
	pollDuration := time.Since(pollStarted)
	for _, failure := range fetches.Errors() {
		if !errors.Is(failure.Err, context.DeadlineExceeded) {
			t.Fatal(failure)
		}
	}
	for _, record := range fetches.Records() {
		s.verify(t, record)
	}

	s.drainReports(t, reports)
	return pollDuration
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
func memoryEnvelope(t *testing.T, samples []int64, finalSamples int) (warm, final, peak int64) {
	t.Helper()
	if len(samples) < max(30, finalSamples) {
		t.Fatal("not enough samples for memory stability")
	}
	warm, final = medianRSS(samples[10:30]), medianRSS(samples[len(samples)-finalSamples:])
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
