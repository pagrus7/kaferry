//go:build integration && load

package compat

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	core "github.com/pagrus7/kaferry/internal/copy"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestStreamingSoakFidelityCommitLagMemoryAndShutdown(t *testing.T) {
	// Given
	report := newLoadReport(t, "soak")
	report.SourcePartitions, report.DestinationPartitions = 4, 5
	report.WorkerRecordCap, report.WorkerByteCap = 1024, 128<<20
	duration := 2 * time.Minute
	if value := os.Getenv("KAFERRY_SOAK_DURATION"); value != "" {
		var err error
		duration, err = time.ParseDuration(value)
		if err != nil || duration < time.Minute {
			t.Fatal("KAFERRY_SOAK_DURATION must be at least 1m")
		}
	}
	mode := os.Getenv("KAFERRY_SOAK_MODE")
	require.True(t, mode == "" || mode == "kafka" || mode == "archive", "unknown soak mode")
	archiveMode := mode == "archive"
	rate := 800
	if value := os.Getenv("KAFERRY_SOAK_RATE"); value != "" {
		var err error
		rate, err = strconv.Atoi(value)
		require.NoError(t, err)
		require.True(t, rate > 0 && rate <= 10000, "soak rate must be 1..10000")
	}
	require.Less(t, duration.Seconds()*float64(rate)*1.05, float64(1<<32)-8192, "requested stream exceeds the fixture ID range")
	a := admin(t)
	destinationBrokers := broker(t)
	destinationAdmin := a
	stall := os.Getenv("KAFERRY_SOAK_DEST_STALL_CONTAINER")
	if archiveMode && stall != "" {
		t.Fatal("archive soak does not use a destination broker stall")
	}
	if stall != "" {
		ownedSoakDestination(t, stall)
		destinationBrokers = os.Getenv("KAFKA_TEST_DEST_BROKERS")
		if destinationBrokers == "" || destinationBrokers == broker(t) {
			t.Fatal("destination stalls require a separate KAFKA_TEST_DEST_BROKERS cluster")
		}
		destinationAdmin = kadm.NewClient(fixtureClient(t, destinationBrokers))
	}
	src, dst := topic(t, a, 4), topic(t, destinationAdmin, 5)
	group := src + "-soak"
	p := producer(t)
	reports := make(chan kgo.ProduceResult, 4096)
	c := fixtureClient(t, destinationBrokers, kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	assign(t, c, dst, 5)
	statement := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s'), CONNECTION destination AS ('bootstrap.servers'='%s') INSERT INTO TOPIC '%s@destination' WITH (PARTITIONING = KEY) SELECT * FROM TOPIC '%s' WITH (GROUP='%s', START=EARLIEST) ", broker(t), destinationBrokers, dst, src, group)
	archive := filepath.Join(t.TempDir(), "captured")
	if archiveMode {
		report.Kind, report.DestinationPartitions = "capture-soak", 0
		statement = fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s') INSERT INTO ARCHIVE '%s' SELECT * FROM TOPIC '%s' WITH (GROUP='%s', START=EARLIEST) ", broker(t), archive, src, group)
	}
	configPath := filepath.Join(t.TempDir(), "soak.toml")
	require.NoError(t, os.WriteFile(configPath, []byte("[worker]\nrecord_cap=1024\npartition_record_cap=256\n"), 0600))
	ctx, cancel := context.WithTimeout(context.Background(), duration+4*time.Minute)
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
	if directory := os.Getenv("KAFERRY_REPORT_DIR"); directory != "" {
		file, err := os.Create(filepath.Join(directory, "soak.samples.jsonl"))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, file.Close()) })
		monitor.encoder = json.NewEncoder(file)
	}
	batch := max(1, rate/50)
	period := time.Second * time.Duration(batch) / time.Duration(rate)

	// When copying the controlled stream with periodic bursts and optional destination stalls.
	for time.Since(start) < duration || records.acknowledged < records.published || (!archiveMode && records.verified < records.published) || (archiveMode && monitor.committed < records.published) {
		select {
		case <-process.done:
			t.Fatalf("soak process exited early: %v %s", process.wait(), process.logs())
		case <-ctx.Done():
			t.Fatal("soak did not catch up")
		default:
		}
		now := time.Now()
		stalls.step(t, now, now.Sub(start) < duration)
		if now.Sub(start) < duration && !now.Before(nextPublish) && (archiveMode || records.published-records.contiguous < 8192) && records.published-records.acknowledged < 8192 {
			count := batch
			if !now.Before(nextBurst) {
				count = min(2048, rate*2)
				bursts++
				nextBurst = now.Add(time.Minute)
			}
			records.publish(t, ctx, p, reports, count)
			nextPublish = now.Add(period)
		}
		if archiveMode {
			records.drainReports(t, reports)
			time.Sleep(5 * time.Millisecond)
		} else {
			records.catchUp(t, c, reports)
		}
		monitor.sample(t, now, process.pid(), a, group, src, records.published)
	}

	// When stopping intake after every fixture record is verified.
	shutdown := time.Now()
	require.NoError(t, process.signal(os.Interrupt))
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
	if archiveMode {
		identity := brokerSourceIdentity(t, a, src)
		next := [4]int{0, 1, 2, 3}
		scanArchive(t, archive, func(record core.Record) {
			require.NotNil(t, record.Source)
			require.GreaterOrEqual(t, len(record.Value), 4)
			id := int(binary.BigEndian.Uint32(record.Value))
			part := id % 4
			require.Equal(t, next[part], id, "archive has missing, duplicated or reordered input")
			next[part] += 4
			want := archiveLoadRecord(id)
			want.Source.Cluster, want.Source.Topic, want.Source.TopicID = identity.Cluster, identity.Topic, identity.TopicID
			stamp := records.stamp.UnixMilli()
			want.TimestampMS = &stamp
			require.Equal(t, want, record)
			records.verified++
		})
		require.Equal(t, records.published, records.verified)
	}
	report.Records = records.verified
	report.RecordsPerSecond = float64(report.Records) / report.ElapsedSeconds
	report.ChildCPUSeconds = process.cpu().Seconds()
	report.ShutdownSeconds = shutdownTime.Seconds()
	report.Bursts, report.DestinationStalls = bursts, stalls.count
	report.MaxSampledCommitLag = monitor.maxLag
	report.RSSSamples = monitor.samples

	// Then shutdown saves all progress, preserves fidelity and stays within work/RSS caps.
	committed := 0
	for _, part := range committedPartitions(t, a, group, src, 4) {
		committed += int(part.At)
	}
	require.Equal(t, records.published, committed, "shutdown did not commit all verified records")
	summary := summaryFrom(t, process.logs())
	report.PeakUncommittedRecords, report.PeakUncommittedBytes = summary.PeakRecords, summary.PeakBytes
	if summary.Submitted != records.published || summary.Confirmed != records.published || summary.PeakRecords > 1024 || summary.PeakBytes > 128<<20 {
		t.Fatal("soak counts or uncommitted caps violated", summary)
	}
	warm, final, peak := memoryEnvelope(t, monitor.rss)
	peak = max(peak, monitor.peak)
	require.LessOrEqual(t, peak, int64(512<<10), "peak RSS exceeds 512 MiB")
	report.SampledPeakRSSBytes, report.WarmMedianRSSBytes, report.FinalMedianRSSBytes = peak*1024, warm*1024, final*1024
	t.Logf("duration=%s bursts=%d destination_stalls=%d records=%d records/s=%.0f child_cpu=%s peak_RSS_MiB=%.1f warm_median_RSS_MiB=%.1f final_median_RSS_MiB=%.1f max_sampled_commit_lag=%d shutdown=%s peak_uncommitted_records=%d peak_uncommitted_bytes=%d", duration, bursts, stalls.count, records.published, float64(records.published)/time.Since(start).Seconds(), process.cpu(), float64(peak)/1024, float64(warm)/1024, float64(final)/1024, monitor.maxLag, shutdownTime, summary.PeakRecords, summary.PeakBytes)
}

func ownedSoakDestination(t *testing.T, container string) {
	t.Helper()
	data, err := exec.Command("docker", "inspect", "--format", "{{index .Config.Labels \"com.docker.compose.project\"}} {{index .Config.Labels \"com.docker.compose.service\"}}", container).Output()
	if err != nil || strings.TrimSpace(string(data)) != "kaferry-compat kafka-dr" {
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
	next               time.Time
	rss                []int64
	peak               int64
	maxLag             int
	committed, samples int
	encoder            *json.Encoder
}

func (m *soakMonitor) sample(t *testing.T, now time.Time, pid int, admin *kadm.Client, group, source string, published int) {
	t.Helper()
	if now.Before(m.next) {
		return
	}
	rss, err := rssSample(pid)
	require.NoError(t, err, "RSS sampling failed")
	m.samples++
	if len(m.rss) < 50 {
		m.rss = append(m.rss, rss)
	} else {
		copy(m.rss[30:49], m.rss[31:50])
		m.rss[49] = rss
	}
	m.peak = max(m.peak, rss)
	committed := 0
	for _, part := range committedPartitions(t, admin, group, source, 4) {
		committed += int(part.At)
	}
	lag := published - committed
	if lag < 0 {
		t.Fatal("committed work never published to the source")
	}
	m.committed = committed
	if m.encoder != nil {
		require.NoError(t, m.encoder.Encode(map[string]any{"at": now.UTC(), "rss_kib": rss, "commit_lag": lag, "published": published}))
	}
	m.maxLag = max(m.maxLag, lag)
	m.next = now.Add(time.Second)
}
