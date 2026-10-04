//go:build integration && load

package compat

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestStreamingSoakFidelityCommitLagMemoryAndShutdown(t *testing.T) {
	duration := 2 * time.Minute
	if value := os.Getenv("KFWD_SOAK_DURATION"); value != "" {
		var err error
		duration, err = time.ParseDuration(value)
		if err != nil || duration < time.Minute {
			t.Fatal("KFWD_SOAK_DURATION must be at least 1m")
		}
	}
	a := admin(t)
	src, dst := topic(t, a, 4), topic(t, a, 5)
	group := src + "-soak"
	p := producer(t, nil)
	reports := make(chan k.Event, 4096)
	c := consumer(t, dst+"-verify")
	parts := make([]k.TopicPartition, 5)
	for i := range parts {
		parts[i] = k.TopicPartition{Topic: &dst, Partition: int32(i), Offset: 0}
	}
	if err := c.Assign(parts); err != nil {
		t.Fatal(err)
	}
	statement := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s') INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' OPTIONS(GROUP='%s',START=EARLIEST)", broker(t), dst, src, group)
	ctx, cancel := context.WithTimeout(context.Background(), duration+45*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, testBinary(t), "run", "-e", statement)
	var output bytes.Buffer
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	finished := false
	t.Cleanup(func() {
		cancel()
		if !finished {
			_ = cmd.Process.Kill()
			<-done
		}
	})
	waitMembers(t, a, group, 1, 4, nil)
	committedPartitions(t, a, group, src, 4)
	stamp := time.Now().Truncate(time.Millisecond)
	headers := []k.Header{{Key: "trace", Value: []byte{0, 255}}, {Key: "dup", Value: nil}, {Key: "dup", Value: []byte{}}}
	seen := make([]bool, 0)
	verified, acknowledged, maxLag := 0, 0, 0
	lastOrder := make(map[[2]int32]int)
	verify := func(record *k.Message) {
		if record.TopicPartition.Error != nil || len(record.Value) < 4 {
			t.Fatal("invalid output", record)
		}
		id := int(binary.BigEndian.Uint32(record.Value))
		if id >= len(seen) || seen[id] {
			t.Fatal("unexpected or duplicated output", id)
		}
		key := []byte(fmt.Sprintf("key-%d", id%257))
		if id%7 == 0 {
			key = nil
		}
		if !bytes.Equal(record.Key, key) || (record.Key == nil) != (key == nil) || len(record.Value) != 1024+id%4096 || !record.Timestamp.Equal(stamp) || !reflect.DeepEqual(record.Headers, headers) {
			t.Fatal("logical record changed", id)
		}
		for _, b := range record.Value[4:] {
			if b != byte(id%251) {
				t.Fatal("payload bytes changed", id)
			}
		}
		pair := [2]int32{int32(id % 4), record.TopicPartition.Partition}
		if prior, exists := lastOrder[pair]; exists && id <= prior {
			t.Fatal("source/destination partition order changed", pair, prior, id)
		}
		lastOrder[pair] = id
		seen[id] = true
		verified++
	}
	drainReports := func() {
		for {
			select {
			case event := <-reports:
				message, ok := event.(*k.Message)
				if !ok || message.TopicPartition.Error != nil {
					t.Fatal("fixture publish failed", event)
				}
				acknowledged++
			default:
				return
			}
		}
	}
	start := time.Now()
	nextPublish, nextSample := start, start.Add(time.Second)
	var rss []int64
	for time.Since(start) < duration || verified < len(seen) || acknowledged < len(seen) {
		select {
		case err := <-done:
			finished = true
			t.Fatalf("soak process exited early: %v %s", err, output.String())
		case <-ctx.Done():
			t.Fatal("soak did not catch up")
		default:
		}
		now := time.Now()
		if now.Sub(start) < duration && !now.Before(nextPublish) {
			for range 16 {
				id := len(seen)
				payload := bytes.Repeat([]byte{byte(id % 251)}, 1024+id%4096)
				binary.BigEndian.PutUint32(payload, uint32(id))
				key := []byte(fmt.Sprintf("key-%d", id%257))
				if id%7 == 0 {
					key = nil
				}
				if err := p.Produce(&k.Message{TopicPartition: k.TopicPartition{Topic: &src, Partition: int32(id % 4)}, Key: key, Value: payload, Timestamp: stamp, Headers: headers}, reports); err != nil {
					t.Fatal(err)
				}
				seen = append(seen, false)
			}
			nextPublish = now.Add(20 * time.Millisecond)
		}
		switch event := c.Poll(5).(type) {
		case *k.Message:
			verify(event)
		case k.Error:
			t.Fatal(event)
		}
		drainReports()
		if !now.Before(nextSample) {
			data, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(cmd.Process.Pid)).Output()
			if err != nil {
				t.Fatal("RSS sampling failed", err)
			}
			sample, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			rss = append(rss, sample)
			committed := 0
			for _, part := range committedPartitions(t, a, group, src, 4) {
				committed += int(part.Offset)
			}
			lag := len(seen) - committed
			if lag < 0 {
				t.Fatal("committed work never published to the source")
			}
			if lag > maxLag {
				maxLag = lag
			}
			nextSample = now.Add(time.Second)
		}
	}
	shutdown := time.Now()
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		finished = true
		if err != nil {
			t.Fatalf("soak shutdown: %v %s", err, output.String())
		}
	case <-ctx.Done():
		t.Fatal("soak shutdown timed out")
	}
	shutdownTime := time.Since(shutdown)
	committed := 0
	for _, part := range committedPartitions(t, a, group, src, 4) {
		committed += int(part.Offset)
	}
	if committed != len(seen) {
		t.Fatal("shutdown did not commit all verified records", committed, len(seen))
	}
	var summary struct {
		Submitted, Confirmed int
		PeakRecords          int   `json:"peak_uncommitted_records"`
		PeakBytes            int64 `json:"peak_uncommitted_bytes"`
	}
	for _, line := range strings.Split(output.String(), "\n") {
		var event struct {
			Message string `json:"msg"`
		}
		if json.Unmarshal([]byte(line), &event) == nil && event.Message == "copy summary" {
			if err := json.Unmarshal([]byte(line), &summary); err != nil {
				t.Fatal(err)
			}
		}
	}
	if summary.Submitted != len(seen) || summary.Confirmed != len(seen) || summary.PeakRecords > 20000 || summary.PeakBytes > 128<<20 {
		t.Fatal("soak counts or uncommitted caps violated", summary)
	}
	if len(rss) < 30 {
		t.Fatal("not enough samples for memory stability")
	}
	median := func(samples []int64) int64 {
		copy := append([]int64(nil), samples...)
		sort.Slice(copy, func(i, j int) bool { return copy[i] < copy[j] })
		return copy[len(copy)/2]
	}
	warm := median(rss[10:30])
	final := median(rss[len(rss)-20:])
	var peak int64
	for _, sample := range rss {
		if sample > peak {
			peak = sample
		}
	}
	if peak > 512<<10 || final > warm+32<<10 {
		t.Fatal("memory did not stay within the soak envelope", warm, final, peak)
	}
	t.Logf("duration=%s records=%d records/s=%.0f child_cpu=%s peak_RSS_MiB=%.1f warm_median_RSS_MiB=%.1f final_median_RSS_MiB=%.1f max_sampled_commit_lag=%d shutdown=%s peak_uncommitted_records=%d peak_uncommitted_bytes=%d", duration, len(seen), float64(len(seen))/time.Since(start).Seconds(), cmd.ProcessState.UserTime()+cmd.ProcessState.SystemTime(), float64(peak)/1024, float64(warm)/1024, float64(final)/1024, maxLag, shutdownTime, summary.PeakRecords, summary.PeakBytes)
}
