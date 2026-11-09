//go:build integration && load

package compat

import (
	"context"
	"encoding/json"
	"fmt"
	core "github.com/pagrus7/kaferry/src/internal/copy"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const soakDrainDuration = 10 * time.Second

func consumeOnlyStart(scheduledEnd, lastPublish time.Time) time.Time {
	if lastPublish.After(scheduledEnd) {
		return lastPublish
	}
	return scheduledEnd
}

func RunStreamingSoakFidelityCommitLagMemoryAndShutdown(t *testing.T) {
	// Given
	report := newLoadReport(t, "soak")
	sourcePartitions, destinationPartitions := 4, 5
	profile := os.Getenv("KAFERRY_SOAK_PROFILE")
	require.True(t, profile == "" || profile == "application", "unknown soak profile")
	endurance := profile == "application"
	topology := os.Getenv("KAFERRY_SOAK_TOPOLOGY")
	require.True(t, topology == "" || topology == "single" || topology == "replicated", "unknown soak topology")
	replicated := topology == "replicated"
	require.True(t, !replicated || endurance, "replicated topology requires application endurance")
	if endurance {
		sourcePartitions, destinationPartitions = 24, 32
	}
	report.SourcePartitions, report.DestinationPartitions = sourcePartitions, destinationPartitions
	report.WorkerRecordCap, report.WorkerByteCap = 1024, 128<<20
	duration := 2 * time.Minute
	if endurance {
		duration = 24 * time.Hour
	}
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
	if endurance {
		rate = 200
	}
	if value := os.Getenv("KAFERRY_SOAK_RATE"); value != "" {
		var err error
		rate, err = strconv.Atoi(value)
		require.NoError(t, err)
		require.True(t, rate > 0 && rate <= 10000, "soak rate must be 1..10000")
	}
	if endurance {
		require.False(t, archiveMode, "application endurance requires Kafka output")
		require.Equal(t, 200, rate, "application endurance uses a fixed 200 records/second")
		require.Equal(t, "random", os.Getenv("KAFERRY_ARCHIVE_PAYLOAD"), "application endurance requires high entropy payloads")
		require.NotEmpty(t, os.Getenv("KAFERRY_SOAK_DEST_STALL_CONTAINER"), "application endurance requires destination stalls")
		report.Endurance = newEnduranceEvidence(duration)
		if replicated {
			report.Endurance.Topology = "replicated"
			report.Endurance.ReplicationFactor, report.Endurance.MinInsyncReplicas = 3, 2
		}
		report.Workload.Name = report.Endurance.Profile
	}
	require.Less(t, duration.Seconds()*float64(rate)*1.05, float64(1<<32)-8192, "requested stream exceeds the fixture ID range")
	a := admin(t)
	destinationBrokers := broker(t)
	destinationAdmin := a
	stall := os.Getenv("KAFERRY_SOAK_DEST_STALL_CONTAINER")
	var stalledBrokers []*ownedBroker
	if archiveMode && stall != "" {
		t.Fatal("archive soak does not use a destination broker stall")
	}
	if stall != "" {
		project := "kaferry-compat"
		if endurance {
			project = "kaferry-endurance"
		}
		if replicated {
			project = fixtureProject("kaferry-replicated")
			for node := 1; node <= 3; node++ {
				service := fmt.Sprintf("destination-%d", node)
				stalledBrokers = append(stalledBrokers, ownBroker(t, project+"-"+service+"-1", project, service))
			}
		} else {
			stalledBrokers = []*ownedBroker{ownBroker(t, stall, fixtureProject(project), "kafka-dr")}
		}
		destinationBrokers = os.Getenv("KAFKA_TEST_DEST_BROKERS")
		if destinationBrokers == "" || destinationBrokers == broker(t) {
			t.Fatal("destination stalls require a separate KAFKA_TEST_DEST_BROKERS cluster")
		}
		destinationAdmin = kadm.NewClient(fixtureClient(t, destinationBrokers))
	}
	src, dst := soakTopic(t, a, sourcePartitions, replicated), soakTopic(t, destinationAdmin, destinationPartitions, replicated)
	if endurance {
		// Retain all input/output until final reconciliation, even for seven-day runs.
		for _, fixture := range []struct {
			admin *kadm.Client
			topic string
		}{{a, src}, {destinationAdmin, dst}} {
			alterTopicConfig(t, fixture.admin, fixture.topic, map[string]string{"retention.ms": "-1", "retention.bytes": "-1"})
		}
	}
	group := src + "-soak"
	p := producer(t, kgo.MaxBufferedRecords(2048))
	reports := make(chan kgo.ProduceResult, 4096)
	c := fixtureClient(t, destinationBrokers, kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	assign(t, c, dst, destinationPartitions)
	statement := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s'), CONNECTION destination AS ('bootstrap.servers'='%s') INSERT INTO TOPIC '%s@destination' WITH (PARTITIONING = KEY) SELECT * FROM TOPIC '%s' WITH (GROUP='%s', START=EARLIEST) ", broker(t), destinationBrokers, dst, src, group)
	archive := filepath.Join(t.TempDir(), "captured")
	if archiveMode {
		report.Kind, report.DestinationPartitions = "capture-soak", 0
		statement = fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s') INSERT INTO ARCHIVE '%s' SELECT * FROM TOPIC '%s' WITH (GROUP='%s', START=EARLIEST) ", broker(t), archive, src, group)
	}
	configPath := filepath.Join(t.TempDir(), "soak.toml")
	require.NoError(t, os.WriteFile(configPath, []byte("[worker]\nrecord_cap=1024\npartition_record_cap=256\nprogress_interval='0'\n"), 0600))
	ctx, cancel := context.WithTimeout(context.Background(), duration+soakDrainDuration+4*time.Minute)
	defer cancel()
	process := startCLI(t, ctx, testBinary(t), "", "run", "-e", statement, "-c", configPath)
	report.cli = process
	waitMembers(t, a, group, 1, sourcePartitions, nil)
	committedPartitions(t, a, group, src, sourcePartitions)
	records := newPartitionedStreamRecords(src, time.Now().Truncate(time.Millisecond), sourcePartitions)
	records.nonblocking = endurance
	if endurance {
		records.keyRouting = kgo.StickyKeyPartitioner(nil).ForTopic(dst)
		records.destinationPartitions = destinationPartitions
	}
	if !archiveMode {
		records.observer = newDeliveryObserver(time.Now)
	}
	start := time.Now()
	scheduledEnd := start.Add(duration)
	lastPublish := time.Time{}
	nextPublish, nextSample := start, start.Add(time.Second)
	nextBurst, nextStall := start.Add(10*time.Second), start.Add(30*time.Second)
	bursts := 0
	stalls := &destinationStalls{brokers: stalledBrokers, next: nextStall}
	monitor := soakMonitor{next: nextSample, kafka: !archiveMode, endurance: report.Endurance}
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
	for time.Now().Before(consumeOnlyStart(scheduledEnd, lastPublish).Add(soakDrainDuration)) {
		published := false
		select {
		case <-process.done:
			t.Fatalf("soak process exited early: %v %s", process.wait(), process.logs())
		case <-ctx.Done():
			t.Fatal("soak did not catch up")
		default:
		}
		now := time.Now()
		wasPaused := stalls.paused()
		stalls.step(t, now, now.Sub(start) < duration)
		if endurance && wasPaused && !stalls.paused() {
			report.Endurance.recoveryAt = time.Now()
		}
		if endurance {
			e := report.Endurance
			if !e.endRecorded && now.Sub(start) >= duration {
				_, loadedFinal, _ := memoryEnvelope(t, monitor.rss, 20)
				e.LoadedFinalMedianRSSBytes = loadedFinal * 1024
			}
			require.NoError(t, e.check(now.Sub(start), duration, records.published, records.acknowledged))
			require.Zero(t, records.publishErrors, "endurance producer rejected input")
			if !e.recoveryAt.IsZero() {
				require.NoError(t, e.checkRecovery(time.Since(e.recoveryAt), records.acknowledged-records.unique, rate))
			}
		}
		if endurance && now.Sub(start) < duration+5*time.Second {
			e := report.Endurance
			if records.published-records.receipts.contiguous() >= 8192 {
				e.VerifierGates++
			}
			if records.published-records.acknowledged >= 8192 {
				e.ProducerGates++
			}
			count := min(512, enduranceScheduled(min(now.Sub(start), duration))-records.published,
				8192-(records.published-records.receipts.contiguous()), 8192-(records.published-records.acknowledged))
			if count > 0 {
				publishStart := time.Now()
				records.publish(t, ctx, p, reports, count)
				lastPublish = time.Now()
				monitor.idleSamples = 0
				published = true
				e.ProducerSeconds += time.Since(publishStart).Seconds()
			}
			bursts = max(0, (enduranceScheduled(min(now.Sub(start), duration))-int(min(now.Sub(start), duration)*200/time.Second))/400)
		}
		if !endurance && now.Sub(start) < duration && !now.Before(nextPublish) && (archiveMode || records.published-records.receipts.contiguous() < 8192) && records.published-records.acknowledged < 8192 {
			count := batch
			if !now.Before(nextBurst) {
				count = min(2048, rate*2)
				bursts++
				nextBurst = now.Add(time.Minute)
			}
			records.publish(t, ctx, p, reports, count)
			lastPublish = time.Now()
			monitor.idleSamples = 0
			published = true
			nextPublish = now.Add(period)
		}
		verifyStart := time.Now()
		if archiveMode {
			records.drainReports(t, reports)
			time.Sleep(5 * time.Millisecond)
		} else {
			records.catchUp(t, c, reports)
		}
		if endurance {
			report.Endurance.VerifierSeconds += time.Since(verifyStart).Seconds()
		}
		sampleStart := time.Now()
		var destinationEnds map[int32]int64
		if !archiveMode && !stalls.paused() && !now.Before(monitor.next) {
			destinationEnds = destinationOffsets(t, destinationAdmin, dst, destinationPartitions)
		}
		monitor.sample(t, now, process.pid(), a, group, src, records, destinationEnds,
			!time.Now().Before(consumeOnlyStart(scheduledEnd, lastPublish)) && !published)
		if endurance {
			report.Endurance.SamplingSeconds += time.Since(sampleStart).Seconds()
		}
	}

	// When the full consume-only period ends, stop copying and reconcile the final output.
	if endurance {
		require.Equal(t, records.published, records.unique, "backlog remains after the consume-only period")
	}
	shutdown := time.Now()
	require.NoError(t, process.signal(os.Interrupt))
	select {
	case <-process.done:
		if err := process.wait(); err != nil {
			t.Fatalf("soak shutdown: %v %s", err, process.logs())
		}
	case <-time.After(30 * time.Second):
		t.Fatal("soak shutdown timed out after 30s")
	}
	shutdownTime := time.Since(shutdown)
	if endurance {
		require.True(t, report.Endurance.LoadValidated, "load verification did not finish")
		require.Equal(t, 1+int((duration-30*time.Second-time.Nanosecond)/time.Minute), stalls.count,
			"destination stall schedule was not completed")
		require.Equal(t, stalls.count, report.Endurance.Recoveries, "not all stalls recovered")
	}
	report.ElapsedSeconds = time.Since(start).Seconds()
	if archiveMode {
		identity := brokerSourceIdentity(t, a, src)
		scanArchive(t, archive, func(record core.Record) {
			records.verifyArchive(t, record, identity)
		})
	} else {
		ends := destinationOffsets(t, destinationAdmin, dst, destinationPartitions)
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
	require.NotNil(t, delivery.Integrity.Matched)
	require.True(t, *delivery.Integrity.Matched, "submitted and received key/value/header fingerprints differ: %+v", delivery.Integrity)
	t.Logf("integrity=%s submitted_sha256=%s received_sha256=%s", delivery.Integrity.Algorithm, delivery.Integrity.SubmittedSHA256, delivery.Integrity.ReceivedSHA256)
	if delivery.Observed != nil {
		require.Zero(t, delivery.Observed.Backlog, "unreceived acknowledged records")
		require.Zero(t, delivery.Observed.Unsettled, "unsettled delivery timings")
		require.Equal(t, delivery.UniqueReceived, delivery.Observed.TimedReceipts, "every unique receipt is timed")
		if delivery.UniqueReceived > 0 {
			require.NotNil(t, delivery.Observed.MaxLatencySeconds)
			t.Logf("observed_peak_backlog=%d timed_receipts=%d max_delivery_seconds=%.6f p95=%.6f p99=%.6f p99.9=%.6f",
				delivery.Observed.PeakBacklog, delivery.Observed.TimedReceipts, *delivery.Observed.MaxLatencySeconds,
				*delivery.Observed.P95LatencySeconds, *delivery.Observed.P99LatencySeconds, *delivery.Observed.P999LatencySeconds)
		}
	}

	// Then shutdown saves all progress, preserves fidelity and stays within work/RSS caps.
	committed := 0
	for _, part := range committedPartitions(t, a, group, src, sourcePartitions) {
		require.Equal(t, int64((records.published+sourcePartitions-1-int(part.Partition))/sourcePartitions), part.At,
			"shutdown saved the wrong next offset for source partition %d", part.Partition)
		committed += int(part.At)
	}
	require.Equal(t, records.published, committed, "shutdown did not commit all verified records")
	summary := summaryFrom(t, process.logs())
	report.PeakUncommittedRecords, report.PeakUncommittedBytes = summary.PeakRecords, summary.PeakBytes
	if summary.Submitted != records.published || summary.Confirmed != records.published || summary.PeakRecords > 1024 || summary.PeakBytes > 128<<20 {
		t.Fatal("soak counts or uncommitted caps violated", summary)
	}
	require.NoError(t, monitor.checkIdleRSS(5))
	warm, final, peak := memoryEnvelope(t, monitor.rss, 5)
	peak = max(peak, monitor.peak)
	require.LessOrEqual(t, peak, int64(512<<10), "peak RSS exceeds 512 MiB")
	report.SampledPeakRSSBytes, report.WarmMedianRSSBytes, report.FinalMedianRSSBytes = peak*1024, warm*1024, final*1024
	t.Logf("duration=%s bursts=%d destination_stalls=%d records=%d records/s=%.0f child_cpu=%s peak_RSS_MiB=%.1f warm_median_RSS_MiB=%.1f final_median_RSS_MiB=%.1f max_sampled_commit_lag=%d shutdown=%s peak_uncommitted_records=%d peak_uncommitted_bytes=%d", duration, bursts, stalls.count, records.published, float64(records.published)/time.Since(start).Seconds(), process.cpu(), float64(peak)/1024, float64(warm)/1024, float64(final)/1024, monitor.maxLag, shutdownTime, summary.PeakRecords, summary.PeakBytes)
}

type destinationStalls struct {
	brokers     []*ownedBroker
	until, next time.Time
	count       int
}

func (s *destinationStalls) paused() bool {
	for _, broker := range s.brokers {
		if broker.paused() {
			return true
		}
	}
	return false
}

func (s *destinationStalls) step(t *testing.T, now time.Time, publishing bool) {
	t.Helper()
	if len(s.brokers) > 0 && !s.paused() && publishing && !now.Before(s.next) {
		for _, broker := range s.brokers {
			require.NoError(t, broker.pause(t))
		}
		s.count++
		s.until = time.Now().Add(time.Second)
		s.next = s.next.Add(time.Minute)
	}
	if s.paused() && !now.Before(s.until) {
		for _, broker := range s.brokers {
			require.NoError(t, broker.recover())
		}
	}
}

type soakMonitor struct {
	endurance                  *enduranceEvidence
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
	idleSamples                int
}

func (m *soakMonitor) recordRSS(rss int64, idle bool) {
	m.samples++
	if len(m.rss) < 50 {
		m.rss = append(m.rss, rss)
	} else {
		copy(m.rss[30:49], m.rss[31:50])
		m.rss[49] = rss
	}
	m.peak = max(m.peak, rss)
	if idle {
		m.idleSamples++
	} else {
		m.idleSamples = 0
	}
}

func (m *soakMonitor) checkIdleRSS(count int) error {
	if m.idleSamples < count {
		return fmt.Errorf("not enough idle RSS samples: got %d, need %d", m.idleSamples, count)
	}
	return nil
}

func (m *soakMonitor) sample(t *testing.T, now time.Time, pid int, admin *kadm.Client, group, source string, records *streamRecords, destinationEnds map[int32]int64, idle bool) {
	t.Helper()
	if now.Before(m.next) {
		return
	}
	rss, err := rssSample(pid)
	require.NoError(t, err, "RSS sampling failed")
	m.recordRSS(rss, idle)
	committed := 0
	for _, part := range committedPartitions(t, admin, group, source, len(records.receipts.next)) {
		committed += int(part.At)
	}
	lag := records.published - committed
	if lag < 0 {
		t.Fatal("committed work never published to the source")
	}
	sample := map[string]any{"at": now.UTC(), "rss_kib": rss, "commit_lag": lag, "submitted": records.published, "delivery": records.delivery(false)}
	if m.endurance != nil {
		sample["endurance"] = m.endurance
	}
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
