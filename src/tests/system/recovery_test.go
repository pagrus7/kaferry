//go:build integration && fault

package compat

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func waitRecoveryProgress(t *testing.T, process *cliProcess, submitted, confirmed int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		data, err := os.ReadFile(process.output)
		require.NoError(t, err)
		for _, line := range strings.Split(string(data), "\n") {
			var progress copySummary
			if json.Unmarshal([]byte(line), &progress) == nil && progress.Message == "progress" &&
				progress.Submitted >= submitted && progress.Confirmed >= confirmed {
				return
			}
		}
		require.False(t, process.finished(), "copier exited before the required progress")
		require.True(t, time.Now().Before(deadline), "copier never reported the required progress")
		time.Sleep(20 * time.Millisecond)
	}
}

func pauseRecoveryBroker(t *testing.T, container string) func() {
	t.Helper()
	broker := ownBroker(t, container, fixtureProject("kaferry-compat"), "")
	require.NoError(t, broker.pause(t))
	return func() { require.NoError(t, broker.recover()) }
}

func TestRecoveryBlockedOutput(t *testing.T) {
	if os.Getenv("KAFKA_FAULT_DEST_CONTAINER") == "" {
		t.Skip("requires separate owned source/destination brokers")
	}
	_ = broker(t) // Missing prerequisites skip the parent, never a successful empty cycle.
	require.NotEmpty(t, os.Getenv("KAFKA_TEST_DEST_BROKERS"))
	require.NotEmpty(t, os.Getenv("KAFKA_FAULT_CONTAINER"))
	duration := time.Duration(0)
	if value := os.Getenv("KAFERRY_RECOVERY_DURATION"); value != "" {
		var err error
		duration, err = time.ParseDuration(value)
		require.NoError(t, err)
		require.GreaterOrEqual(t, duration, time.Minute)
		require.LessOrEqual(t, duration, 24*time.Hour)
	}
	started := time.Now()
	cycles := 0
	defer func() {
		if directory := os.Getenv("KAFERRY_REPORT_DIR"); directory != "" {
			data, err := json.MarshalIndent(map[string]any{"completed_cycles": cycles, "elapsed_seconds": time.Since(started).Seconds(),
				"requested_seconds": duration.Seconds(), "passed": !t.Failed(), "continuous_uptime": false}, "", "  ")
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(directory, "recovery.json"), data, 0600))
		}
	}()
	for {
		for _, failure := range []string{"kill", "signal", "delivery-timeout", "source-outage-shutdown", "ownership-loss"} {
			if !t.Run(fmt.Sprintf("cycle-%d/%s", cycles, failure), func(t *testing.T) {
				qualified := false
				for attempt := 0; attempt < 3 && !qualified; attempt++ {
					if !t.Run(fmt.Sprintf("attempt-%d", attempt), func(t *testing.T) {
						qualified = recoveryBlockedOutput(t, failure)
					}) {
						return
					}
				}
				require.True(t, qualified, "source commits completed before every attempted injection")
			}) {
				return
			}
		}
		cycles++
		if time.Since(started) >= duration {
			return
		}
	}
}

func recoveryBlockedOutput(t *testing.T, failure string) bool {
	// Given a live copier with a durable prefix on two partitions.
	a := admin(t)
	destination := os.Getenv("KAFKA_TEST_DEST_BROKERS")
	require.NotEmpty(t, destination)
	require.NotEqual(t, broker(t), destination)
	da := kadm.NewClient(fixtureClient(t, destination))
	src, dst := topic(t, a, 2), topic(t, da, 2)
	group := src + "-recovery"
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	oracle := startFaultOracle(t, "recovery/"+failure, broker(t), destination, src, dst, group, 2, false)
	statement := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s','session.timeout.ms'='%d','heartbeat.interval.ms'='1000'), CONNECTION destination AS ('bootstrap.servers'='%s') INSERT INTO TOPIC '%s@destination' SELECT * FROM TOPIC '%s' WITH (GROUP='%s')",
		broker(t), oracle.timeouts["session.timeout.ms"].Milliseconds(), destination, dst, src, group)
	deadlines := fmt.Sprintf("shutdown_timeout=%q\ndelivery_timeout=%q\n", oracle.timeouts["shutdown_timeout"], oracle.timeouts["delivery_timeout"])
	settings := "record_cap=32\npartition_record_cap=16\nbyte_cap=8192\npartition_byte_cap=4096\nprogress_interval='20ms'\n" + deadlines
	byteCap, recordCap := int64(8192), 32
	if failure == "source-outage-shutdown" {
		// Leave room for the whole batch so capacity cannot commit each delivery.
		settings = "record_cap=1024\npartition_record_cap=512\nbyte_cap=134217728\npartition_byte_cap=16777216\nprogress_interval='20ms'\n" + deadlines
		byteCap, recordCap = 134217728, 1024
	}
	config := configuration(t, settings)
	process := startCLI(t, ctx, testBinary(t), "", "run", "-e", statement, "-c", config)
	oracle.writer(process)
	waitMembers(t, a, group, 1, 2, nil)
	p := producer(t)
	ledger := recoveryLedger{expected: map[string]*kgo.Record{}, seen: map[string]bool{}}
	stamp := time.Now().Truncate(time.Millisecond)
	publishBatch := func(first, end int) {
		for id := first; id < end; id++ {
			record := &kgo.Record{Topic: src, Partition: int32(id % 2), Key: []byte(fmt.Sprint(id)),
				Value: []byte(strings.Repeat(fmt.Sprintf("%02d", id), 1024)), Timestamp: stamp,
				Headers: []kgo.RecordHeader{{Key: "dup", Value: nil}, {Key: "dup", Value: []byte{}}}}
			ledger.expected[string(record.Key)] = record
			publish(t, p, record)
		}
	}
	publishBatch(0, 2)
	waitTopicCount(t, da, dst, 2, 2)
	deadline := time.Now().Add(10 * time.Second)
	for {
		parts := committedPartitions(t, a, group, src, 2)
		if parts[0].At == 1 && parts[1].At == 1 {
			break
		}
		require.True(t, time.Now().Before(deadline), "initial prefix was not saved")
		time.Sleep(50 * time.Millisecond)
	}

	var replacement *cliProcess
	window := oracle.begin(t, failure, map[string]any{"worker": "original", "pid": process.pid(),
		"source_container": os.Getenv("KAFKA_FAULT_CONTAINER"), "destination_container": os.Getenv("KAFKA_FAULT_DEST_CONTAINER")}, true,
		oracle.timeouts,
		"both source partitions have saved prefix 1 and destination has two records")
	if failure == "ownership-loss" {
		replacement = startCLI(t, ctx, testBinary(t), "", "run", "-e", statement, "-c", config)
		oracle.writer(replacement)
		waitMembers(t, a, group, 2, 2, nil)
		oracle.injected(t, window, "member-join", map[string]int{"pid": replacement.pid()}, "peer joined; two workers own the two source partitions")
	}

	// When output is blocked, establish outstanding submissions. For the source
	// outage, keep output healthy and establish confirmed but unsaved work instead.
	restoreDestination, restoreSource := func() {}, func() {}
	if failure != "source-outage-shutdown" {
		restoreDestination = pauseRecoveryBroker(t, os.Getenv("KAFKA_FAULT_DEST_CONTAINER"))
		oracle.injected(t, window, "destination-pause", map[string]string{"container": os.Getenv("KAFKA_FAULT_DEST_CONTAINER")}, "Docker destination pause succeeded")
	}
	if failure == "source-outage-shutdown" {
		// Freeze the copier before inspecting progress, so host scheduling cannot
		// move it across the one-second commit boundary while we pause the broker.
		pending := false
		for attempt := 0; attempt < 5; attempt++ {
			first := len(ledger.expected)
			publishBatch(first, first+16)
			waitRecoveryProgress(t, process, first+16, first+16)
			require.NoError(t, process.signal(syscall.SIGSTOP))
			oracle.injected(t, window, "process-suspend", map[string]int{"pid": process.pid()}, "SIGSTOP succeeded before saved-offset inspection")
			parts := committedPartitions(t, a, group, src, 2)
			for _, part := range parts {
				pending = pending || part.At < int64((first+16)/2)
			}
			if pending {
				restoreSource = pauseRecoveryBroker(t, os.Getenv("KAFKA_FAULT_CONTAINER"))
				oracle.injected(t, window, "source-pause", map[string]string{"container": os.Getenv("KAFKA_FAULT_CONTAINER")}, "worker suspended with confirmed unsaved work; Docker source pause succeeded")
			}
			require.NoError(t, process.signal(syscall.SIGCONT))
			oracle.injected(t, window, "process-resume", map[string]int{"pid": process.pid()}, "SIGCONT succeeded after saved-offset inspection")
			if pending {
				break
			}
			t.Log("batch already saved before suspension; retrying fault setup")
		}
		require.True(t, pending, "could not establish unsaved work for source fault")
	} else {
		publishBatch(2, 18)
		waitRecoveryProgress(t, process, 3, 0)
		for _, part := range committedPartitions(t, a, group, src, 2) {
			require.EqualValues(t, 1, part.At, "blocked delivery must not advance progress")
		}
	}
	stop := time.Now()
	switch failure {
	case "kill", "ownership-loss":
		require.NoError(t, process.command.Process.Kill())
	case "signal", "source-outage-shutdown":
		require.NoError(t, process.signal(syscall.SIGTERM))
	}
	select {
	case <-process.done:
	case <-time.After(8 * time.Second):
		t.Fatal("failed copier did not terminate within its budget")
	}
	oracle.injected(t, window, failure, map[string]int{"pid": process.pid()}, "original worker terminated with blocked output or saved-progress outage")
	if failure == "source-outage-shutdown" && process.wait() == nil {
		restoreSource()
		// SIGSTOP cannot retract a request already sent to Kafka. Only classify
		// this as a missed injection when independent offsets prove a safe exit.
		for _, part := range committedPartitions(t, a, group, src, 2) {
			require.EqualValues(t, len(ledger.expected)/2, part.At,
				"successful shutdown must have saved all confirmed input")
		}
		t.Log("in-flight commit completed before broker suspension; retrying setup")
		// This is a measured setup attempt, not successful fault qualification.
		// No case receipt is published; the next attempt must exercise the error.
		return false
	}
	require.Error(t, process.wait(), "blocked delivery must not report successful shutdown")
	if failure != "kill" && failure != "ownership-loss" {
		var exit *exec.ExitError
		require.ErrorAs(t, process.wait(), &exit)
		require.Equal(t, 1, exit.ExitCode())
	}
	t.Logf("failure=%s exit_seconds=%.3f", failure, time.Since(stop).Seconds())
	if failure == "source-outage-shutdown" {
		require.Contains(t, process.logs(), "shutdown timed out while saving progress")
	}
	restoreSource()
	for _, part := range committedPartitions(t, a, group, src, 2) {
		if failure == "source-outage-shutdown" {
			// An offset request may have reached the broker before its response was lost.
			require.LessOrEqual(t, part.At, int64(len(ledger.expected)/2))
		} else {
			require.EqualValues(t, 1, part.At, "failure saved unacknowledged output")
		}
	}
	restoreDestination()
	if replacement == nil {
		waitMembers(t, a, group, 0, 0, nil)
		replacement = startCLI(t, ctx, testBinary(t), "", "run", "-e", statement, "-c", config)
		oracle.writer(replacement)
	}
	waitMembers(t, a, group, 1, 2, nil)
	oracle.healed(t, window, "brokers restored; replacement owns both source partitions")
	reader := fixtureClient(t, destination)
	assign(t, reader, dst, 2)
	positions := map[int32]int64{}
	consume := func() {
		poll, done := context.WithTimeout(ctx, time.Second)
		defer done()
		fetches := reader.PollFetches(poll)
		for _, record := range fetches.Records() {
			require.NoError(t, ledger.observe(record))
			positions[record.Partition] = record.Offset + 1
		}
		require.NoError(t, ctx.Err(), "recovery failed to reconcile")
	}
	for len(ledger.seen) < len(ledger.expected) {
		consume()
	}
	require.NoError(t, replacement.signal(os.Interrupt))
	require.NoError(t, replacement.wait(), replacement.logs())

	// Then scan through final offsets, validating duplicates as well as first receipts.
	ends, err := da.ListEndOffsets(ctx, dst)
	require.NoError(t, err)
	require.NoError(t, ends.Error())
	for positions[0] < ends[dst][0].Offset || positions[1] < ends[dst][1].Offset {
		consume()
	}
	for _, part := range committedPartitions(t, a, group, src, 2) {
		require.EqualValues(t, len(ledger.expected)/2, part.At)
	}
	summary := summaryFrom(t, replacement.logs())
	require.LessOrEqual(t, summary.PeakBytes, byteCap)
	require.LessOrEqual(t, summary.PeakRecords, recordCap)
	t.Logf("unique=%d duplicates=%d missing=0 corrupt=0", len(ledger.seen), ledger.duplicates)
	oracle.finish(t)
	return true
}
