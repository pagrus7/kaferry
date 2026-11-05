//go:build integration && load

package compat

import (
	"context"
	"encoding/json"
	"fmt"
	core "github.com/pagrus7/kaferry/internal/copy"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const soakDrainDuration = 5 * time.Minute

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
	ctx, cancel := context.WithTimeout(context.Background(), duration+soakDrainDuration+4*time.Minute)
	defer cancel()
	process := startCLI(t, ctx, testBinary(t), "", "run", "-e", statement, "-c", configPath)
	report.cli = process
	waitMembers(t, a, group, 1, 4, nil)
	committedPartitions(t, a, group, src, 4)
	records := newStreamRecords(src, time.Now().Truncate(time.Millisecond))
	if !archiveMode {
		records.observer = newDeliveryObserver(time.Now)
	}
	start := time.Now()
	nextPublish, nextSample := start, start.Add(time.Second)
	nextBurst, nextStall := start.Add(10*time.Second), start.Add(30*time.Second)
	bursts := 0
	stalls := &destinationStalls{container: stall, next: nextStall}
	t.Cleanup(func() { stalls.recover(t) })
	monitor := soakMonitor{next: nextSample, kafka: !archiveMode}
	reconciled := false
	report.PublishingSeconds, report.DrainSeconds = duration.Seconds(), soakDrainDuration.Seconds()
	t.Cleanup(func() {
		delivery := records.delivery(reconciled)
		report.Delivery = &delivery
		report.Records, report.ElapsedSeconds = records.verified, time.Since(start).Seconds()
		report.RecordsPerSecond = float64(report.Records) / report.ElapsedSeconds
		report.Bursts, report.DestinationStalls = bursts, stalls.count
		report.RSSSamples, report.MaxSampledCommitLag = monitor.samples, monitor.maxLag
		report.SampledPeakRSSBytes = monitor.peak * 1024
		if process.finished() {
			report.ChildCPUSeconds = process.cpu().Seconds()
		}
		if !archiveMode {
			if monitor.samples > 0 {
				peak := monitor.maxDestinationLag
				report.MaxSampledDestinationLag = &peak
			}
			if monitor.destinationConsumerSamples > 0 {
				consumerPeak := monitor.maxDestinationConsumerLag
				report.MaxSampledDestinationConsumerLag = &consumerPeak
			}
			if delivery.ReconciliationComplete {
				lag, consumerLag := max(0, records.acknowledged-records.unique), monitor.destinationConsumerLag
				report.FinalDestinationLag, report.FinalDestinationConsumerLag = &lag, &consumerLag
			}
		}
	})
	if directory := os.Getenv("KAFERRY_REPORT_DIR"); directory != "" {
		file, err := os.Create(filepath.Join(directory, "soak.samples.jsonl"))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, file.Close()) })
		monitor.encoder = json.NewEncoder(file)
	}
	batch := max(1, rate/50)
	period := time.Second * time.Duration(batch) / time.Duration(rate)

	// When copying the controlled stream with periodic bursts and optional destination stalls.
	for time.Since(start) < duration+soakDrainDuration {
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
		var destinationEnds map[int32]int64
		if !archiveMode && !stalls.paused && !now.Before(monitor.next) {
			destinationEnds = destinationOffsets(t, destinationAdmin, dst, 5)
		}
		monitor.sample(t, now, process.pid(), a, group, src, records, destinationEnds)
	}

	// When the full consume-only period ends, stop copying and reconcile the final output.
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
		scanArchive(t, archive, func(record core.Record) {
			id, unique := records.receive(t, record.Value)
			if !unique {
				return
			}
			part := id % 4
			want := archiveLoadRecord(id)
			want.Source.Cluster, want.Source.Topic, want.Source.TopicID = identity.Cluster, identity.Topic, identity.TopicID
			stamp := records.stamp.UnixMilli()
			want.TimestampMS = &stamp
			if !reflect.DeepEqual(want, record) {
				records.corrupted++
				return
			}
			pair := [2]int32{int32(part), int32(part)}
			if prior, exists := records.order[pair]; exists && id <= prior {
				records.reordered++
			}
			records.order[pair] = max(records.order[pair], id)
			records.verified++
		})
	} else {
		ends := destinationOffsets(t, destinationAdmin, dst, 5)
		for destinationOffsetLag(ends, records.offsets) != 0 {
			require.NoError(t, ctx.Err(), "destination verifier did not reach the final offsets")
			records.catchUp(t, c, reports)
		}
		monitor.destinationConsumerLag = destinationOffsetLag(ends, records.offsets)
	}
	records.drainReports(t, reports)
	reconciled = true
	report.ShutdownSeconds = shutdownTime.Seconds()
	delivery := records.delivery(reconciled)
	require.True(t, delivery.ReconciliationComplete, "source publication did not complete: %+v", delivery)
	require.Zero(t, *delivery.Missing, "missing destination records")
	require.Zero(t, delivery.Duplicates, "duplicate destination records")
	require.Zero(t, delivery.Corrupted, "corrupted destination records")
	require.Zero(t, delivery.Unexpected, "unexpected destination records")
	require.Zero(t, delivery.Reordered, "reordered destination records")
	if delivery.Observed != nil {
		require.Zero(t, delivery.Observed.Backlog, "unreceived acknowledged records")
		require.Zero(t, delivery.Observed.Unsettled, "unsettled delivery timings")
		require.Equal(t, delivery.UniqueReceived, delivery.Observed.TimedReceipts, "every unique receipt is timed")
		if delivery.UniqueReceived > 0 {
			require.NotNil(t, delivery.Observed.MaxLatencySeconds)
			t.Logf("observed_peak_backlog=%d timed_receipts=%d max_delivery_seconds=%.6f",
				delivery.Observed.PeakBacklog, delivery.Observed.TimedReceipts, *delivery.Observed.MaxLatencySeconds)
		}
	}

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
	next                       time.Time
	rss                        []int64
	peak                       int64
	maxLag                     int
	samples                    int
	encoder                    *json.Encoder
	maxDestinationLag          int
	destinationConsumerLag     int64
	maxDestinationConsumerLag  int64
	kafka                      bool
	destinationConsumerSamples int
}

func (m *soakMonitor) sample(t *testing.T, now time.Time, pid int, admin *kadm.Client, group, source string, records *streamRecords, destinationEnds map[int32]int64) {
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
	lag := records.published - committed
	if lag < 0 {
		t.Fatal("committed work never published to the source")
	}
	sample := map[string]any{"at": now.UTC(), "rss_kib": rss, "commit_lag": lag, "submitted": records.published, "delivery": records.delivery(false)}
	if m.kafka {
		destinationLag := max(0, records.acknowledged-records.unique)
		m.maxDestinationLag = max(m.maxDestinationLag, destinationLag)
		sample["destination_lag"] = destinationLag
	}
	if destinationEnds != nil {
		m.destinationConsumerSamples++
		m.destinationConsumerLag = destinationOffsetLag(destinationEnds, records.offsets)
		m.maxDestinationConsumerLag = max(m.maxDestinationConsumerLag, m.destinationConsumerLag)
		sample["destination_consumer_lag"] = m.destinationConsumerLag
	}
	if m.encoder != nil {
		require.NoError(t, m.encoder.Encode(sample))
	}
	m.maxLag = max(m.maxLag, lag)
	m.next = now.Add(time.Second)
}

func destinationOffsets(t *testing.T, admin *kadm.Client, topic string, partitions int) map[int32]int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ends, err := admin.ListEndOffsets(ctx, topic)
	require.NoError(t, err)
	require.NoError(t, ends.Error())
	require.Len(t, ends[topic], partitions)
	result := make(map[int32]int64, partitions)
	for partition, end := range ends[topic] {
		result[partition] = end.Offset
	}
	return result
}

func destinationOffsetLag(ends, consumed map[int32]int64) int64 {
	var lag int64
	for partition, end := range ends {
		lag += max(int64(0), end-consumed[partition])
	}
	return lag
}
