//go:build integration && fault && load

package compat

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/src/tests/system/faults"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestReplicatedBrokerFaults(t *testing.T) {
	for _, name := range []string{"source-leader", "destination-leader", "rolling-source", "rolling-destination", "destination-isr-loss"} {
		t.Run(name, func(t *testing.T) { replicatedBrokerFault(t, name) })
	}
}

type brokerObservation struct {
	At            time.Time                     `json:"at"`
	Saved         []int64                       `json:"saved"`
	Ends          []int64                       `json:"destination_ends"`
	Topology      map[string][]faults.Partition `json:"topology"`
	BrokerHealthy bool                          `json:"broker_healthy"`
	Pending       copySummary                   `json:"cli_progress"`
	Publishing    replicatedPublishing          `json:"publishing"`
}

type brokerCopy struct {
	t                                             *testing.T
	ctx                                           context.Context
	lab                                           *faults.Lab
	metadata                                      faults.KafkaMetadata
	src, dst, group, directory, config, statement string
	timeouts                                      map[string]time.Duration
	oracle                                        *faultOracle
	publisher                                     *replicatedPublisher
	workers                                       []*cliProcess
	memories                                      []func() rssPeak
	starts                                        []time.Time
	measurements                                  []map[string]any
	events                                        []map[string]any
}

func replicatedBrokerFault(t *testing.T, name string) {
	// Given the selected executable, one worker and steady offers across all 24
	// RF=3/minISR=2 partitions on independent publisher and observer routes.
	require.NotEmpty(t, os.Getenv("KAFERRY_FIXTURE_PROJECT"))
	require.NotEmpty(t, os.Getenv("KAFERRY_TOXIPROXY_API"))
	require.NotEmpty(t, os.Getenv("KAFERRY_TEST_BINARY"))
	require.NotEmpty(t, os.Getenv("KAFERRY_EXPECT_BINARY_SHA256"))
	timeouts := faultTimeouts(t, "broker/"+name)
	var recovery time.Duration
	for _, timeout := range timeouts {
		recovery += timeout
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute+recovery)
	defer cancel()
	directory := os.Getenv("KAFERRY_REPORT_DIR")
	if directory == "" {
		directory = t.TempDir()
	}
	directory, err := filepath.Abs(filepath.Join(directory, "broker-"+name))
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(directory, 0700))
	metadata, err := faults.NewMetadata()
	require.NoError(t, err)
	t.Cleanup(metadata.Close)
	s := &brokerCopy{t: t, ctx: ctx, metadata: metadata, timeouts: timeouts, directory: directory}
	s.lab = &faults.Lab{Project: os.Getenv("KAFERRY_FIXTURE_PROJECT"), API: os.Getenv("KAFERRY_TOXIPROXY_API"), Metadata: metadata}
	prefix := fmt.Sprintf("kaferry-broker-%d", time.Now().UnixNano())
	s.src, s.dst, s.group = prefix+"-source", prefix+"-destination", prefix+"-group"
	replicatedTopic(ctx, t, metadata["source"], s.src)
	replicatedTopic(ctx, t, metadata["destination"], s.dst)
	s.verifySettings()
	s.oracle = startFaultOracle(t, "broker/"+name, strings.Join(faults.Endpoints("source", 0), ","),
		strings.Join(faults.Endpoints("destination", 0), ","), s.src, s.dst, s.group, replicatedPartitions, false)
	s.config = configuration(t, zombieWorkerSettings(timeouts))
	s.statement = zombieWorkerSQL(timeouts, 1, prefix+"-worker", s.src, s.dst, s.group)
	s.start()
	producer := fixtureClient(t, strings.Join(faults.Endpoints("source", 0), ","), kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.MaxBufferedRecords(2*replicatedPartitions), kgo.RecordDeliveryTimeout(timeouts["delivery_timeout"]), kgo.AllowIdempotentProduceCancellation())
	s.publisher = startReplicatedBrokerPublisher(ctx, producer, s.src, timeouts["delivery_timeout"])
	t.Cleanup(func() { s.publisher.stop() })
	baseline := s.wait("nonempty saved prefix on every partition", false, func(_ context.Context, state brokerObservation) bool {
		return allAbove(state.Saved, make([]int64, replicatedPartitions), 0)
	})

	// When the registered fault is injected at an independently observed state,
	// retain its exact targets and require recovery before the next transition.
	switch name {
	case "destination-isr-loss":
		s.isrLoss(baseline)
	case "source-leader", "destination-leader":
		s.leaderLoss(strings.TrimSuffix(name, "-leader"), baseline)
	default:
		s.rolling(strings.TrimPrefix(name, "rolling-"))
	}
	publication := s.publisher.stop()
	require.NoError(t, publication.err, "every offered source write must resolve successfully before reconciliation")
	require.Equal(t, publication.Offered, publication.Acknowledged, "source offers remain uncertain")
	require.LessOrEqual(t, publication.MaxOfferGapSeconds, 2.0, "source offers did not maintain the steady schedule")
	s.wait("all acknowledged source input saved", false, func(_ context.Context, state brokerObservation) bool {
		return slices.Equal(state.Saved, publication.Acknowledged)
	})
	process := s.workers[len(s.workers)-1]
	require.NoError(t, process.signal(os.Interrupt))
	waitErr := process.wait()
	logs, err := process.currentLogs()
	require.NoError(t, err)
	require.NoError(t, waitErr, logs)
	s.measureLast("completed")

	// Then the shared independent observer completes every configured recovery
	// window, reconciles every retained record and rejects unsafe saved prefixes.
	s.oracle.finish(t)
	safetyPath := filepath.Join(filepath.Dir(s.oracle.log), "observations", "case.json")
	safety, err := os.ReadFile(safetyPath)
	require.NoError(t, err)
	relative, err := filepath.Rel(filepath.Dir(directory), safetyPath)
	require.NoError(t, err)
	data, err := json.MarshalIndent(map[string]any{"id": "broker/" + name, "source_partitions": replicatedPartitions,
		"source_topic": s.src, "destination_topic": s.dst, "group": s.group, "baseline": baseline, "events": s.events,
		"publishing": publication, "worker_measurements": s.measurements, "restarts": len(s.workers) - 1, "restart_scope": "same selected binary, SQL, group and worker1 route",
		"timeout_milliseconds": durationMilliseconds(timeouts), "safety_case": map[string]string{"observations": relative, "sha256": fmt.Sprintf("%x", sha256.Sum256(safety))}}, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(directory, "replicated-broker.json"), append(data, '\n'), 0600))
}

func durationMilliseconds(timeouts map[string]time.Duration) map[string]int64 {
	values := make(map[string]int64, len(timeouts))
	for name, value := range timeouts {
		values[name] = value.Milliseconds()
	}
	return values
}

func (s *brokerCopy) verifySettings() {
	for side, admin := range s.metadata {
		configs, err := admin.DescribeBrokerConfigs(s.ctx, 1, 2, 3)
		require.NoError(s.t, err)
		require.Len(s.t, configs, 3)
		for _, broker := range configs {
			require.NoError(s.t, broker.Err)
			require.Equal(s.t, fmt.Sprint(s.timeouts["replica.lag.time.max.ms"].Milliseconds()), configValue(broker.Configs, "replica.lag.time.max.ms"), side)
		}
		topic := s.src
		if side == "destination" {
			topic = s.dst
		}
		configs, err = admin.DescribeTopicConfigs(s.ctx, topic)
		require.NoError(s.t, err)
		require.Len(s.t, configs, 1)
		require.NoError(s.t, configs[0].Err)
		require.Equal(s.t, "2", configValue(configs[0].Configs, "min.insync.replicas"))
	}
	require.Equal(s.t, 10*time.Second, s.timeouts["progress_save_timeout"], "product periodic-save context")
}

func configValue(configs []kadm.Config, key string) string {
	for _, config := range configs {
		if config.Key == key {
			return config.MaybeValue()
		}
	}
	return ""
}

func (s *brokerCopy) start() {
	process := startCLI(s.t, s.ctx, testBinary(s.t), "", "run", "-e", s.statement, "-c", s.config)
	s.workers = append(s.workers, process)
	s.starts = append(s.starts, time.Now().UTC())
	s.memories = append(s.memories, watchReplicatedRSS(s.t, process))
	s.oracle.writer(process)
	logPath := filepath.Join(s.directory, fmt.Sprintf("worker-%d.cli.log", len(s.workers)))
	s.t.Cleanup(func() {
		if !process.finished() {
			_ = process.command.Process.Kill()
		}
		_ = process.wait()
		logs, err := process.currentLogs()
		if err != nil {
			s.t.Error("read selected worker log: ", err)
			return
		}
		if err := os.WriteFile(logPath, []byte(logs), 0600); err != nil {
			s.t.Error("retain selected worker log: ", err)
		}
	})
	waitMembers(s.t, s.metadata["source"], s.group, 1, replicatedPartitions, nil)
}

func (s *brokerCopy) measureLast(reason string) {
	i := len(s.workers) - 1
	process := s.workers[i]
	require.True(s.t, process.finished())
	memory := s.memories[i]()
	require.Positive(s.t, memory.samples)
	require.Zero(s.t, memory.failures)
	require.LessOrEqual(s.t, memory.peak*1024, int64(512<<20))
	logs, err := process.currentLogs()
	require.NoError(s.t, err)
	_, found, err := latestCopyProgress(logs)
	require.NoError(s.t, err)
	require.True(s.t, found, "completed broker worker omitted progress evidence")
	summary := summaryFrom(s.t, logs)
	require.LessOrEqual(s.t, summary.PeakRecords, 1024)
	require.LessOrEqual(s.t, summary.PeakBytes, int64(134217728))
	terminal, _ := brokerTerminalFailure(logs)
	s.measurements = append(s.measurements, map[string]any{"pid": process.pid(), "started_at": s.starts[i], "stopped_at": time.Now().UTC(),
		"binary_sha256": os.Getenv("KAFERRY_EXPECT_BINARY_SHA256"), "worker_route": 1,
		"terminal_error": terminal,
		"exit_code":      process.command.ProcessState.ExitCode(), "reason": reason, "sampled_peak_rss_bytes": memory.peak * 1024, "rss_samples": memory.samples,
		"rss_failures": memory.failures, "record_cap": 1024, "byte_cap": 134217728, "peak_uncommitted_records": summary.PeakRecords, "peak_uncommitted_bytes": summary.PeakBytes})
	require.NoError(s.t, os.WriteFile(filepath.Join(s.directory, fmt.Sprintf("worker-%d.cli.log", i+1)), []byte(logs), 0600))
}

func (s *brokerCopy) live(allowFailure bool) {
	process := s.workers[len(s.workers)-1]
	if !process.finished() {
		return
	}
	logs, err := process.currentLogs()
	require.NoError(s.t, err)
	require.True(s.t, allowFailure, "CLI exited before recovery: %s", logs)
	require.Error(s.t, process.wait())
	require.Equal(s.t, 1, process.command.ProcessState.ExitCode(), logs)
	// Only the product's existing transport/coordinator/deadline failures may
	// use this case's recorded restart. Other failures remain qualification errors.
	_, known := brokerTerminalFailure(logs)
	require.True(s.t, known, "broker fault produced an unexplained CLI failure: %s", logs)
}

func brokerTerminalFailure(logs string) (string, bool) {
	var terminal []string
	for _, line := range strings.Split(logs, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || json.Valid([]byte(line)) {
			continue
		}
		known := false
		for _, message := range []string{"context deadline exceeded", "REQUEST_TIMED_OUT", "NOT_COORDINATOR", "COORDINATOR_NOT_AVAILABLE", "UNKNOWN_MEMBER_ID", "ILLEGAL_GENERATION", "NOT_LEADER_OR_FOLLOWER", "BROKER_NOT_AVAILABLE", "connection refused", "i/o timeout", "EOF", "delivery timeout", "outstanding writes did not finish within 10 seconds", "shutdown timed out while saving progress", "shutdown timed out while finishing writes"} {
			known = known || strings.Contains(line, message)
		}
		terminal = append(terminal, line)
		if !known {
			return strings.Join(terminal, "\n"), false
		}
	}
	return strings.Join(terminal, "\n"), len(terminal) > 0
}

func TestBrokerFaultRestartRequiresClassifiedTerminalFailure(t *testing.T) {
	// Given a transient warning in structured logs, when an unrelated terminal
	// failure follows, then that earlier transport warning cannot authorize restart.
	warning := `{"msg":"transient source error","error":"EOF"}` + "\n"
	for _, terminal := range []string{"unexpected invariant failure", "context deadline exceeded\nunexpected invariant failure", ""} {
		_, known := brokerTerminalFailure(warning + terminal)
		require.False(t, known, terminal)
	}
	// When the terminal error itself names the existing timeout/save contract,
	// then retain that exact failure separately from informational log records.
	failure, known := brokerTerminalFailure(warning + "context deadline exceeded\n")
	require.True(t, known)
	require.Equal(t, "context deadline exceeded", failure)
}

func (s *brokerCopy) observe(ctx context.Context) (brokerObservation, bool) {
	state := brokerObservation{At: time.Now().UTC(), Publishing: s.publisher.snapshot()}
	state.Topology = make(map[string][]faults.Partition)
	for side, topic := range map[string]string{"source": s.src, "destination": s.dst} {
		var ok bool
		state.Topology[side], ok = s.topology(ctx, side, topic)
		if !ok {
			return state, false
		}
	}
	var err error
	var found bool
	state.Pending, found, err = currentCopyProgress(s.workers[len(s.workers)-1])
	require.NoError(s.t, err, "read current broker worker progress")
	if !found {
		return state, false
	}
	parts, err := s.metadata["source"].FetchOffsetsForTopics(ctx, s.group, s.src)
	if err != nil || parts.Error() != nil || len(parts[s.src]) != replicatedPartitions {
		return state, false
	}
	state.Saved = make([]int64, replicatedPartitions)
	for partition, part := range parts[s.src] {
		state.Saved[partition] = part.At
	}
	ends, err := s.metadata["destination"].ListEndOffsets(ctx, s.dst)
	if err != nil || ends.Error() != nil || len(ends[s.dst]) != replicatedPartitions {
		return state, false
	}
	state.Ends = make([]int64, replicatedPartitions)
	for partition, part := range ends[s.dst] {
		state.Ends[partition] = part.Offset
	}
	return state, true
}

func currentCopyProgress(process *cliProcess) (copySummary, bool, error) {
	logs, err := process.currentLogs()
	if err != nil {
		return copySummary{}, false, err
	}
	return latestCopyProgress(logs)
}

func latestCopyProgress(logs string) (copySummary, bool, error) {
	var result copySummary
	found := false
	// A live writer may be between writing a JSON record and its newline. Only
	// complete rows are evidence; currentLogs rejects this suffix after exit.
	complete := logs[:strings.LastIndexByte(logs, '\n')+1]
	for _, line := range strings.Split(complete, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "{") {
			continue // Terminal command errors are plain text, not progress rows.
		}
		progress, ok, err := decodeCopyProgress(line)
		if err != nil {
			return copySummary{}, false, err
		}
		if ok {
			result, found = progress, true
		}
	}
	return result, found, nil
}

func decodeCopyProgress(line string) (copySummary, bool, error) {
	var header struct {
		Message string `json:"msg"`
	}
	if err := json.Unmarshal([]byte(line), &header); err != nil {
		return copySummary{}, false, fmt.Errorf("decode complete CLI log row: %w", err)
	}
	if header.Message != "progress" {
		return copySummary{}, false, nil
	}
	var counts struct {
		Submitted *int `json:"submitted"`
		Confirmed *int `json:"confirmed"`
	}
	if err := json.Unmarshal([]byte(line), &counts); err != nil {
		return copySummary{}, false, fmt.Errorf("decode CLI progress counts: %w", err)
	}
	if counts.Submitted == nil || counts.Confirmed == nil || *counts.Submitted < 0 || *counts.Confirmed < 0 || *counts.Confirmed > *counts.Submitted {
		return copySummary{}, false, fmt.Errorf("CLI progress requires nonnegative submitted and confirmed counts with confirmed at most submitted")
	}
	var progress copySummary
	if err := json.Unmarshal([]byte(line), &progress); err != nil {
		return copySummary{}, false, fmt.Errorf("decode CLI progress record: %w", err)
	}
	return progress, true, nil
}

func (s *brokerCopy) topology(ctx context.Context, side, topic string) ([]faults.Partition, bool) {
	metadata, err := s.metadata[side].Metadata(ctx, topic)
	if err != nil || metadata.Topics[topic].Err != nil || len(metadata.Topics[topic].Partitions) != replicatedPartitions {
		return nil, false
	}
	var result []faults.Partition
	for partition := int32(0); partition < replicatedPartitions; partition++ {
		part := metadata.Topics[topic].Partitions[partition]
		if part.Err != nil {
			return nil, false
		}
		result = append(result, faults.Partition{Leader: part.Leader, ISR: part.ISR})
	}
	return result, true
}

func (s *brokerCopy) wait(label string, allowFailure bool, condition func(context.Context, brokerObservation) bool) brokerObservation {
	deadline, done := context.WithTimeout(s.ctx, s.timeouts["replica.lag.time.max.ms"]+30*time.Second)
	defer done()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		s.live(allowFailure)
		require.NoError(s.t, s.publisher.snapshot().err, "steady source publisher failed")
		query, cancel := context.WithTimeout(deadline, 2*time.Second)
		state, ok := s.observe(query)
		ready := ok && condition(query, state)
		cancel()
		if ready {
			return state
		}
		select {
		case <-deadline.Done():
			s.t.Fatal("observed-state deadline expired: ", label)
		case <-tick.C:
		}
	}
}

func allAbove(values, before []int64, delta int64) bool {
	if len(values) != replicatedPartitions || len(before) != replicatedPartitions {
		return false
	}
	for partition, value := range values {
		if value <= before[partition]+delta {
			return false
		}
	}
	return true
}

func fullyReplicated(state brokerObservation, side string) bool {
	if len(state.Topology[side]) != replicatedPartitions {
		return false
	}
	for _, partition := range state.Topology[side] {
		if len(partition.ISR) != 3 {
			return false
		}
	}
	return true
}

func (s *brokerCopy) healthy(ctx context.Context, targets ...faults.Target) bool {
	args := []string{"inspect", "--format", `{{.State.Running}} {{.State.Health.Status}}`}
	for _, target := range targets {
		args = append(args, target.Container)
	}
	data, err := faults.Docker(ctx, args...)
	if err != nil {
		return false
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != len(targets) {
		return false
	}
	for _, line := range lines {
		if line != "true healthy" {
			return false
		}
	}
	return true
}

func (s *brokerCopy) inject(role faults.Role, kind string) (*faults.Injection, int) {
	window := s.oracle.begin(s.t, kind, role, kind != "cut", s.timeouts, "nonempty saved prefix on every source partition; steady acknowledged source input")
	injection, err := s.lab.Inject(s.ctx, role, faults.Fault{Kind: kind})
	if injection != nil {
		s.t.Cleanup(func() {
			cleanup, done := context.WithTimeout(context.Background(), 30*time.Second)
			defer done()
			if healErr := injection.Heal(cleanup); healErr != nil {
				s.t.Error("restore resolved broker fault: ", healErr)
			}
			data, writeErr := json.MarshalIndent(map[string]any{"event": injection.Event, "timeout_milliseconds": durationMilliseconds(s.timeouts), "required_recovery_seconds": s.oracle.recovery.Seconds(), "case_failed": s.t.Failed()}, "", "  ")
			if writeErr == nil {
				writeErr = os.WriteFile(filepath.Join(s.directory, fmt.Sprintf("fault-%d.json", window)), append(data, '\n'), 0600)
			}
			if writeErr != nil {
				s.t.Error("retain resolved broker event: ", writeErr)
			}
		})
	}
	require.NoError(s.t, err)
	s.oracle.injected(s.t, window, kind, injection.Event, "owned Docker/Toxiproxy operation completed on the resolved targets")
	return injection, window
}

func (s *brokerCopy) heal(injection *faults.Injection, window int, before brokerObservation) brokerObservation {
	require.NoError(s.t, injection.Heal(s.ctx))
	allowFailure := injection.Event.Fault.Kind != "cut"
	health := s.wait("restored broker health and all 24 ISR sets", allowFailure, func(query context.Context, state brokerObservation) bool {
		return s.healthy(query, injection.Event.Targets[0]) && fullyReplicated(state, injection.Event.Role.Cluster)
	})
	health.BrokerHealthy = true
	if s.workers[len(s.workers)-1].finished() {
		s.measureLast("broker fault fail-fast exit; restarted after health and ISR recovery")
		s.start()
	}
	after := s.wait("post-heal publication and saved progress on every partition", false, func(_ context.Context, state brokerObservation) bool {
		return fullyReplicated(state, injection.Event.Role.Cluster) && allAbove(state.Saved, before.Saved, 2) && allAbove(state.Publishing.Acknowledged, before.Publishing.Acknowledged, 2)
	})
	after.BrokerHealthy = health.BrokerHealthy
	s.oracle.healed(s.t, window, "original targets restored; broker health, ISR3, publishing and saved progress observed")
	return after
}

func (s *brokerCopy) leaderLoss(side string, before brokerObservation) {
	topic := s.src
	if side == "destination" {
		topic = s.dst
	}
	injection, window := s.inject(faults.Role{Kind: "leader", Cluster: side, Topic: topic, Partition: 0}, "kill")
	broker := injection.Event.Targets[0].Broker
	after := s.wait("replacement partition leader while original broker is killed", true, func(_ context.Context, state brokerObservation) bool {
		part := state.Topology[side][0]
		return part.Leader != broker && len(part.ISR) == 2 && allAbove(state.Publishing.Acknowledged, before.Publishing.Acknowledged, 2)
	})
	healed := s.heal(injection, window, before)
	s.events = append(s.events, map[string]any{"fault": injection.Event, "failover": after, "recovered": healed})
}

func (s *brokerCopy) rolling(side string) {
	for broker := int32(1); broker <= 3; broker++ {
		before := s.wait("healthy prefix before next rolling broker", false, func(_ context.Context, state brokerObservation) bool { return fullyReplicated(state, side) })
		injection, window := s.inject(faults.Role{Kind: "broker", Cluster: side, Broker: broker}, "restart")
		after := s.heal(injection, window, before)
		s.events = append(s.events, map[string]any{"fault": injection.Event, "before": before, "recovered": after})
	}
}

func (s *brokerCopy) isrLoss(before brokerObservation) {
	injection, window := s.inject(faults.Role{Kind: "leader", Cluster: "destination", Topic: s.dst, Partition: 0, Listener: "internal"}, "cut")
	broker := injection.Event.Targets[0].Broker
	controllers, err := s.lab.Resolve(s.ctx, faults.Role{Kind: "cluster", Cluster: "destination", Listener: "controller"})
	require.NoError(s.t, err)
	require.Len(s.t, controllers, 3)
	isr := s.wait("actual ISR1 with the leader and controller quorum alive", false, func(query context.Context, state brokerObservation) bool {
		part := state.Topology["destination"][0]
		if part.Leader != broker || !slices.Equal(part.ISR, []int32{broker}) {
			return false
		}
		return s.healthy(query, controllers...) && s.destinationMinISR(query) == "2"
	})
	isr.BrokerHealthy = true
	stalled := s.wait("new source input cannot advance destination or saved target prefix below min ISR", false, func(_ context.Context, state brokerObservation) bool {
		part := state.Topology["destination"][0]
		if part.Leader != broker || len(part.ISR) != 1 {
			return false
		}
		require.Equal(s.t, isr.Ends[0], state.Ends[0], "destination acknowledged output below min ISR2")
		require.LessOrEqual(s.t, state.Saved[0], isr.Ends[0], "saved progress passed acknowledged destination prefix")
		return state.Publishing.Acknowledged[0] > isr.Publishing.Acknowledged[0]+3 && state.Pending.Submitted > state.Pending.Confirmed
	})
	s.oracle.command(s.t, map[string]any{"command": "sample", "reason": "observed-isr1-stall"})
	pid := s.workers[0].pid()
	healed := s.heal(injection, window, stalled)
	require.Len(s.t, s.workers, 1, "ISR loss requires retry by the same process")
	require.Equal(s.t, pid, s.workers[0].pid())
	s.events = append(s.events, map[string]any{"fault": injection.Event, "before": before, "isr_lost": isr, "stalled": stalled, "recovered": healed, "min_insync_replicas": 2, "controller_targets": controllers})
}

func (s *brokerCopy) destinationMinISR(ctx context.Context) string {
	configs, err := s.metadata["destination"].DescribeTopicConfigs(ctx, s.dst)
	if err != nil || len(configs) != 1 || configs[0].Err != nil {
		return ""
	}
	return configValue(configs[0].Configs, "min.insync.replicas")
}
