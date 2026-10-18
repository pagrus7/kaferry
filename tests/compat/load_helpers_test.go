//go:build integration && load

package compat

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

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

type copySummary struct {
	Message     string `json:"msg"`
	Submitted   int    `json:"submitted"`
	Confirmed   int    `json:"confirmed"`
	PeakRecords int    `json:"peak_uncommitted_records"`
	PeakBytes   int64  `json:"peak_uncommitted_bytes"`
}

func summaryFrom(t *testing.T, output string) copySummary {
	t.Helper()
	for _, line := range strings.Split(output, "\n") {
		var summary copySummary
		if json.Unmarshal([]byte(line), &summary) == nil && summary.Message == "copy summary" {
			return summary
		}
	}
	t.Fatal("CLI omitted copy summary", output)
	return copySummary{}
}

type streamRecords struct {
	topic                  string
	stamp                  time.Time
	headers                []kgo.RecordHeader
	seen                   []bool
	order                  map[[2]int32]int
	verified, acknowledged int
}

func newStreamRecords(topic string, stamp time.Time) *streamRecords {
	return &streamRecords{topic: topic, stamp: stamp, headers: []kgo.RecordHeader{{Key: "trace", Value: []byte{0, 255}}, {Key: "dup", Value: nil}, {Key: "dup", Value: []byte{}}}, order: make(map[[2]int32]int)}
}
func (s *streamRecords) publish(t *testing.T, ctx context.Context, producer *kgo.Client, reports chan kgo.ProduceResult, count int) {
	t.Helper()
	for range count {
		id := len(s.seen)
		payload := bytes.Repeat([]byte{byte(id % 251)}, 1024+id%4096)
		binary.BigEndian.PutUint32(payload, uint32(id))
		key := []byte(fmt.Sprintf("key-%d", id%257))
		if id%7 == 0 {
			key = nil
		}
		message := &kgo.Record{Topic: s.topic, Partition: int32(id % 4), Key: key, Value: payload, Timestamp: s.stamp, Headers: s.headers}
		producer.Produce(ctx, message, deliveryReporter(ctx, reports))
		s.seen = append(s.seen, false)
	}
}
func (s *streamRecords) verify(t *testing.T, record *kgo.Record) {
	t.Helper()
	if len(record.Value) < 4 {
		t.Fatal("invalid output", record)
	}
	id := int(binary.BigEndian.Uint32(record.Value))
	if id >= len(s.seen) || s.seen[id] {
		t.Fatal("unexpected or duplicated output", id)
	}
	key := []byte(fmt.Sprintf("key-%d", id%257))
	if id%7 == 0 {
		key = nil
	}
	require.Equal(t, key, record.Key, "record %d key", id)
	require.Len(t, record.Value, 1024+id%4096, "record %d value length", id)
	require.True(t, record.Timestamp.Equal(s.stamp), "record %d timestamp: got %v, want %v", id, record.Timestamp, s.stamp)
	require.Equal(t, s.headers, record.Headers, "record %d headers", id)
	wantByte := byte(id % 251)
	for offset, b := range record.Value[4:] {
		if b != wantByte {
			t.Fatalf("record %d payload byte %d: got %d, want %d", id, offset+4, b, wantByte)
		}
	}
	pair := [2]int32{int32(id % 4), record.Partition}
	if prior, exists := s.order[pair]; exists && id <= prior {
		t.Fatal("source/destination partition order changed", pair, prior, id)
	}
	s.order[pair] = id
	s.seen[id] = true
	s.verified++
}
func (s *streamRecords) drainReports(t *testing.T, reports <-chan kgo.ProduceResult) {
	t.Helper()
	for {
		select {
		case event := <-reports:
			if event.Err != nil {
				t.Fatal("fixture publish failed", event)
			}
			s.acknowledged++
		default:
			return
		}
	}
}
func (s *streamRecords) catchUp(t *testing.T, consumer *kgo.Client, reports <-chan kgo.ProduceResult) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Millisecond)
	defer cancel()
	fetches := consumer.PollRecords(ctx, 1)
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
