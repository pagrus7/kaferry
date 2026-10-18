//go:build integration && load

package compat

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os/exec"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/require"
)

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
	headers                []k.Header
	seen                   []bool
	order                  map[[2]int32]int
	verified, acknowledged int
}

func newStreamRecords(topic string, stamp time.Time) *streamRecords {
	return &streamRecords{topic: topic, stamp: stamp, headers: []k.Header{{Key: "trace", Value: []byte{0, 255}}, {Key: "dup", Value: nil}, {Key: "dup", Value: []byte{}}}, order: make(map[[2]int32]int)}
}
func (s *streamRecords) publish(t *testing.T, producer *k.Producer, reports chan k.Event, count int) {
	t.Helper()
	for range count {
		id := len(s.seen)
		payload := bytes.Repeat([]byte{byte(id % 251)}, 1024+id%4096)
		binary.BigEndian.PutUint32(payload, uint32(id))
		key := []byte(fmt.Sprintf("key-%d", id%257))
		if id%7 == 0 {
			key = nil
		}
		message := &k.Message{TopicPartition: k.TopicPartition{Topic: &s.topic, Partition: int32(id % 4)}, Key: key, Value: payload, Timestamp: s.stamp, Headers: s.headers}
		require.NoError(t, producer.Produce(message, reports))
		s.seen = append(s.seen, false)
	}
}
func (s *streamRecords) verify(t *testing.T, record *k.Message) {
	t.Helper()
	if record.TopicPartition.Error != nil || len(record.Value) < 4 {
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
	if !bytes.Equal(record.Key, key) || (record.Key == nil) != (key == nil) || len(record.Value) != 1024+id%4096 || !record.Timestamp.Equal(s.stamp) || !reflect.DeepEqual(record.Headers, s.headers) {
		t.Fatal("logical record changed", id)
	}
	for _, b := range record.Value[4:] {
		require.Equal(t, byte(id%251), b, "payload bytes changed")
	}
	pair := [2]int32{int32(id % 4), record.TopicPartition.Partition}
	if prior, exists := s.order[pair]; exists && id <= prior {
		t.Fatal("source/destination partition order changed", pair, prior, id)
	}
	s.order[pair] = id
	s.seen[id] = true
	s.verified++
}
func (s *streamRecords) drainReports(t *testing.T, reports <-chan k.Event) {
	t.Helper()
	for {
		select {
		case event := <-reports:
			message, ok := event.(*k.Message)
			if !ok || message.TopicPartition.Error != nil {
				t.Fatal("fixture publish failed", event)
			}
			s.acknowledged++
		default:
			return
		}
	}
}
func (s *streamRecords) catchUp(t *testing.T, consumer *k.Consumer, reports <-chan k.Event) {
	t.Helper()
	switch event := consumer.Poll(5).(type) {
	case *k.Message:
		s.verify(t, event)
	case k.Error:
		if event.IsFatal() || (event.Code() != k.ErrTransport && event.Code() != k.ErrAllBrokersDown && event.Code() != k.ErrTimedOut && event.Code() != k.ErrResolve) {
			t.Fatal(event)
		}
	}
	s.drainReports(t, reports)
}
func rssSample(pid int) (int64, error) {
	data, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
}

type rssPeak struct {
	peak              int64
	samples, failures int
}

func watchRSS(process *cliProcess) func() rssPeak {
	stop, result := make(chan struct{}), make(chan rssPeak, 1)
	go func() {
		var stats rssPeak
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
	return func() rssPeak { close(stop); return <-result }
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
