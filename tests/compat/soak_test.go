//go:build integration && load

package compat

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestStreamingSoakFidelityCommitLagMemoryAndShutdown(t *testing.T) {
	// Given
	report := newLoadReport(t, "soak")
	report.WorkerRecordCap, report.WorkerByteCap = 1024, 128<<20
	duration := 2 * time.Minute
	if value := os.Getenv("KFWD_SOAK_DURATION"); value != "" {
		var err error
		duration, err = time.ParseDuration(value)
		if err != nil || duration < time.Minute {
			t.Fatal("KFWD_SOAK_DURATION must be at least 1m")
		}
	}
	a := admin(t)
	destinationBrokers := broker(t)
	destinationAdmin := a
	stall := os.Getenv("KFWD_SOAK_DEST_STALL_CONTAINER")
	if stall != "" {
		ownedSoakDestination(t, stall)
		destinationBrokers = os.Getenv("KAFKA_TEST_DEST_BROKERS")
		if destinationBrokers == "" || destinationBrokers == broker(t) {
			t.Fatal("destination stalls require a separate KAFKA_TEST_DEST_BROKERS cluster")
		}
		var err error
		destinationAdmin, err = k.NewAdminClient(&k.ConfigMap{"bootstrap.servers": destinationBrokers})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { closeAdmin(t, destinationAdmin) })
	}
	src, dst := topic(t, a, 4), topic(t, destinationAdmin, 5)
	group := src + "-soak"
	p := producer(t, nil)
	reports := make(chan k.Event, 4096)
	c, err := k.NewConsumer(&k.ConfigMap{"bootstrap.servers": destinationBrokers, "group.id": dst + "-verify", "enable.auto.commit": false, "enable.auto.offset.store": false, "isolation.level": "read_committed"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	parts := make([]k.TopicPartition, 5)
	for i := range parts {
		parts[i] = k.TopicPartition{Topic: &dst, Partition: int32(i), Offset: 0}
	}
	if err := c.Assign(parts); err != nil {
		t.Fatal(err)
	}
	statement := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s'), CONNECTION destination AS ('bootstrap.servers'='%s') INSERT INTO TOPIC '%s@destination' SELECT * FROM TOPIC '%s' OPTIONS(GROUP='%s',START=EARLIEST)", broker(t), destinationBrokers, dst, src, group)
	configPath := filepath.Join(t.TempDir(), "soak.toml")
	if err := os.WriteFile(configPath, []byte("[worker]\nrecord_cap=1024\npartition_record_cap=256\n"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), duration+45*time.Second)
	defer cancel()
	process := startCLI(t, ctx, testBinary(t), "", "run", "-e", statement, "-c", configPath)
	report.cli = process
	waitMembers(t, a, group, 1, 4, nil)
	committedPartitions(t, a, group, src, 4)
	records := newStreamRecords(src, time.Now().Truncate(time.Millisecond))
	start := time.Now()
	nextPublish, nextSample := start, start.Add(time.Second)
	nextBurst, nextStall := start.Add(10*time.Second), start.Add(30*time.Second)
	bursts := 0
	stalls := &destinationStalls{container: stall, next: nextStall}
	t.Cleanup(func() { stalls.recover(t) })
	monitor := soakMonitor{next: nextSample}

	// When copying the controlled stream with periodic bursts and optional destination stalls.
	for time.Since(start) < duration || records.verified < len(records.seen) || records.acknowledged < len(records.seen) {
		select {
		case <-process.done:
			t.Fatalf("soak process exited early: %v %s", process.wait(), process.logs())
		case <-ctx.Done():
			t.Fatal("soak did not catch up")
		default:
		}
		now := time.Now()
		stalls.step(t, now, now.Sub(start) < duration)
		if now.Sub(start) < duration && !now.Before(nextPublish) {
			batch := 16
			if !now.Before(nextBurst) {
				batch = 2048
				bursts++
				nextBurst = now.Add(time.Minute)
			}
			records.publish(t, p, reports, batch)
			nextPublish = now.Add(20 * time.Millisecond)
		}
		records.catchUp(t, c, reports)
		monitor.sample(t, now, process.pid(), a, group, src, len(records.seen))
	}

	// When stopping intake after every fixture record is verified.
	shutdown := time.Now()
	if err := process.signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	select {
	case <-process.done:
		if err := process.wait(); err != nil {
			t.Fatalf("soak shutdown: %v %s", err, process.logs())
		}
	case <-ctx.Done():
		t.Fatal("soak shutdown timed out")
	}
	shutdownTime := time.Since(shutdown)
	report.ElapsedSeconds = time.Since(start).Seconds()
	report.Records = records.verified
	report.RecordsPerSecond = float64(report.Records) / report.ElapsedSeconds
	report.ChildCPUSeconds = process.cpu().Seconds()
	report.ShutdownSeconds = shutdownTime.Seconds()
	report.Bursts, report.DestinationStalls = bursts, stalls.count
	report.MaxSampledCommitLag = monitor.maxLag

	// Then shutdown saves all progress, preserves fidelity and stays within work/RSS caps.
	committed := 0
	for _, part := range committedPartitions(t, a, group, src, 4) {
		committed += int(part.Offset)
	}
	if committed != len(records.seen) {
		t.Fatal("shutdown did not commit all verified records", committed, len(records.seen))
	}
	summary := summaryFrom(t, process.logs())
	report.PeakUncommittedRecords, report.PeakUncommittedBytes = summary.PeakRecords, summary.PeakBytes
	if summary.Submitted != len(records.seen) || summary.Confirmed != len(records.seen) || summary.PeakRecords > 1024 || summary.PeakBytes > 128<<20 {
		t.Fatal("soak counts or uncommitted caps violated", summary)
	}
	warm, final, peak := memoryEnvelope(t, monitor.rss)
	report.SampledPeakRSSBytes, report.WarmMedianRSSBytes, report.FinalMedianRSSBytes = peak*1024, warm*1024, final*1024
	t.Logf("duration=%s bursts=%d destination_stalls=%d records=%d records/s=%.0f child_cpu=%s peak_RSS_MiB=%.1f warm_median_RSS_MiB=%.1f final_median_RSS_MiB=%.1f max_sampled_commit_lag=%d shutdown=%s peak_uncommitted_records=%d peak_uncommitted_bytes=%d", duration, bursts, stalls.count, len(records.seen), float64(len(records.seen))/time.Since(start).Seconds(), process.cpu(), float64(peak)/1024, float64(warm)/1024, float64(final)/1024, monitor.maxLag, shutdownTime, summary.PeakRecords, summary.PeakBytes)
}

func ownedSoakDestination(t *testing.T, container string) {
	t.Helper()
	data, err := exec.Command("docker", "inspect", "--format", "{{index .Config.Labels \"com.docker.compose.project\"}} {{index .Config.Labels \"com.docker.compose.service\"}}", container).Output()
	if err != nil || strings.TrimSpace(string(data)) != "kafka-fwd2-compat kafka-dr" {
		t.Fatal("destination stalls require the project's disposable kafka-dr service", err)
	}
}

type destinationStalls struct {
	container   string
	paused      bool
	until, next time.Time
	count       int
}

func (s *destinationStalls) recover(t *testing.T) {
	t.Helper()
	if !s.paused {
		return
	}
	if data, err := exec.Command("docker", "unpause", s.container).CombinedOutput(); err != nil {
		t.Fatalf("destination recovery: %v %s", err, data)
	}
	s.paused = false
}
func (s *destinationStalls) step(t *testing.T, now time.Time, publishing bool) {
	t.Helper()
	if s.container != "" && !s.paused && publishing && !now.Before(s.next) {
		if data, err := exec.Command("docker", "pause", s.container).CombinedOutput(); err != nil {
			t.Fatalf("destination pause: %v %s", err, data)
		}
		s.paused = true
		s.count++
		s.until = time.Now().Add(time.Second)
		s.next = now.Add(time.Minute)
	}
	if s.paused && !now.Before(s.until) {
		s.recover(t)
	}
}

type soakMonitor struct {
	next   time.Time
	rss    []int64
	maxLag int
}

func (m *soakMonitor) sample(t *testing.T, now time.Time, pid int, admin *k.AdminClient, group, source string, published int) {
	t.Helper()
	if now.Before(m.next) {
		return
	}
	rss, err := rssSample(pid)
	if err != nil {
		t.Fatal("RSS sampling failed", err)
	}
	m.rss = append(m.rss, rss)
	committed := 0
	for _, part := range committedPartitions(t, admin, group, source, 4) {
		committed += int(part.Offset)
	}
	lag := published - committed
	if lag < 0 {
		t.Fatal("committed work never published to the source")
	}
	m.maxLag = max(m.maxLag, lag)
	m.next = now.Add(time.Second)
}
