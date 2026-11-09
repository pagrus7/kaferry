//go:build integration && fault && load

package compat

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
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
)

func TestReplicatedBrokerFaults(t *testing.T) {
	for _, name := range []string{"source-leader", "destination-leader", "rolling-source", "rolling-destination", "destination-isr-loss"} {
		t.Run(name, func(t *testing.T) { replicatedBrokerFault(t, name) })
	}
}

type brokerObservation struct {
	Worker        *brokerProgressWitness        `json:"worker"`
	At            time.Time                     `json:"at"`
	Saved         []int64                       `json:"saved"`
	Ends          []int64                       `json:"destination_ends"`
	Topology      map[string][]faults.Partition `json:"topology"`
	BrokerHealthy bool                          `json:"broker_healthy"`
	Pending       copySummary                   `json:"cli_progress"`
	Publishing    replicatedPublishing          `json:"publishing"`
	QueryErrors   []string                      `json:"query_errors,omitempty"`
}

type brokerCopy struct {
	t                                             *testing.T
	ctx                                           context.Context
	lab                                           *faults.Lab
	metadata                                      faults.KafkaMetadata
	src, dst, group, directory, config, statement string
	caseID                                        string
	exits                                         map[*cliProcess]*brokerRetainedExit
	timeouts                                      map[string]time.Duration
	oracle                                        *faultOracle
	publisher                                     *replicatedPublisher
	workers                                       []*cliProcess
	memories                                      []func() rssPeak
	identities                                    []brokerLifetime
	measurements                                  []map[string]any
	events                                        []map[string]any
	active                                        *faults.Injection
	activeWindow                                  int
	beforeFault                                   brokerObservation
	polls                                         []brokerPoll
	omittedPolls                                  int
	healthChecks                                  []brokerHealthCheck
	conditionQueries                              []string
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
	s := &brokerCopy{caseID: "broker/" + name, t: t, ctx: ctx, metadata: metadata, timeouts: timeouts, directory: directory}
	t.Cleanup(s.retainPolls)
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
	producer := replicatedBrokerProducer(t, strings.Join(faults.Endpoints("source", 0), ","), timeouts["delivery_timeout"])
	s.publisher = startReplicatedBrokerPublisher(ctx, producer, s.src, timeouts["delivery_timeout"])
	t.Cleanup(func() { s.publisher.stop() })
	baseline := s.wait("nonempty saved prefix on every partition", false, func(_ context.Context, state brokerObservation) bool {
		return brokerHealthyCopy(state)
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
	require.NoError(t, process.signal(os.Interrupt, "completed copy requested by driver"))
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
	exactCaps := true
	for _, worker := range s.measurements {
		exactCaps = exactCaps && worker["lifetime_highwater_available"] == true
	}
	data, err := json.MarshalIndent(map[string]any{"id": "broker/" + name, "source_partitions": replicatedPartitions,
		"source_topic": s.src, "destination_topic": s.dst, "group": s.group, "baseline": baseline, "events": s.events,
		"publishing": publication, "worker_measurements": s.measurements, "restarts": len(s.workers) - 1, "restart_scope": "same selected binary, SQL, group and worker1 route",
		"exact_lifetime_work_caps_qualified": exactCaps,
		"timeout_milliseconds":               durationMilliseconds(timeouts), "safety_case": map[string]string{"observations": relative, "sha256": fmt.Sprintf("%x", sha256.Sum256(safety))}}, "", "  ")
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
	s.identities = append(s.identities, brokerLifetime{Number: len(s.workers), PID: process.pid(), Started: time.Now().UTC(), Binary: os.Getenv("KAFERRY_EXPECT_BINARY_SHA256")})
	s.memories = append(s.memories, watchReplicatedRSS(s.t, process))
	s.oracle.writer(process)
	logPath := filepath.Join(s.directory, fmt.Sprintf("worker-%d.cli.log", len(s.workers)))
	s.t.Cleanup(func() {
		if !process.finished() {
			_ = process.signal(os.Kill, "broker test cleanup")
		}
		_ = process.wait()
		_, err := s.retainExit(process)
		if err != nil {
			s.t.Error("retain worker lifecycle: ", err)
		}
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
	exit, err := s.retainExit(process)
	require.NoError(s.t, err)
	memory := s.memories[i]()
	require.Positive(s.t, memory.samples)
	require.Zero(s.t, memory.failures)
	require.LessOrEqual(s.t, memory.peak*1024, int64(512<<20))
	logs, err := process.currentLogs()
	require.NoError(s.t, err)
	work, err := brokerWork(logs)
	require.NoError(s.t, err)
	terminal := ""
	proof := s.failureProof(process)
	if process.command.ProcessState.ExitCode() != 0 {
		terminal, err = completedBrokerFailure(process, true, proof)
		require.NoError(s.t, err)
	}
	available := work.Summary != nil
	require.True(s.t, available || brokerForcedCopyPhase(terminal) != "", "CLI omitted copy summary without an eligible documented forced-copy exit")
	measurement := map[string]any{"lifetime": i + 1, "lifecycle": exit.Reference, "unavailable_highwater_phase": "", "pid": process.pid(), "started_at": s.identities[i].Started, "stopped_at": process.stopped,
		"binary_sha256": s.identities[i].Binary, "worker_route": 1,
		"terminal_error": terminal, "failure_context": proof, "copy_summary": work.Summary,
		"exit_code": process.command.ProcessState.ExitCode(), "reason": reason, "sampled_peak_rss_bytes": memory.peak * 1024, "rss_samples": memory.samples,
		"rss_failures": memory.failures, "record_cap": 1024, "byte_cap": 134217728,
		"lifetime_highwater_available": available, "peak_uncommitted_records": nil, "peak_uncommitted_bytes": nil,
		"progress_observations": work.ProgressSamples, "sampled_max_pending_entries": work.SampledPending,
		"sampled_max_pending_estimated_bytes": work.SampledPendingBytes, "cap_measurement_scope": brokerSampledWorkScope}
	if !available {
		measurement["unavailable_highwater_phase"] = brokerForcedCopyPhase(terminal)
	}
	if available {
		measurement["peak_uncommitted_records"], measurement["peak_uncommitted_bytes"] = work.Summary.PeakRecords, work.Summary.PeakBytes
		measurement["cap_measurement_scope"] = brokerExactWorkScope
	}
	s.measurements = append(s.measurements, measurement)
	require.NoError(s.t, os.WriteFile(filepath.Join(s.directory, fmt.Sprintf("worker-%d.cli.log", i+1)), []byte(logs), 0600))
}

func (s *brokerCopy) live(allowFailure bool) {
	process := s.workers[len(s.workers)-1]
	if !process.finished() {
		return
	}
	if s.directory != "" {
		_, err := s.retainExit(process)
		require.NoError(s.t, err)
	}
	_, err := completedBrokerFailure(process, allowFailure, s.failureProof(process))
	require.NoError(s.t, err)
}

func completedBrokerFailure(process *cliProcess, allowFailure bool, proof ...*brokerFailureProof) (string, error) {
	logs, err := process.currentLogs()
	if err != nil {
		return "", err
	}
	if !allowFailure {
		return "", fmt.Errorf("CLI exited before recovery: %s", logs)
	}
	if process.wait() == nil || process.command.ProcessState.ExitCode() != 1 {
		return "", fmt.Errorf("broker fault requires exit 1 before restart: %s", logs)
	}
	// Only the product's existing transport/coordinator/deadline failures may
	// use this case's recorded restart. Other failures remain qualification errors.
	terminal, known := brokerTerminalFailure(logs)
	if !known && len(proof) == 1 {
		terminal, known = brokerContextualFailure(logs, proof[0])
	}
	if !known {
		return "", fmt.Errorf("broker fault produced an unexplained CLI failure: %s", logs)
	}
	return terminal, nil
}

func restartBrokerWorker(process *cliProcess, allowFailure bool, restart func(), proof ...*brokerFailureProof) error {
	if _, err := completedBrokerFailure(process, allowFailure, proof...); err != nil {
		return err
	}
	restart()
	return nil
}

func brokerTerminalFailure(logs string) (string, bool) {
	var terminal []string
	for _, line := range strings.Split(logs, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || json.Valid([]byte(line)) {
			continue
		}
		if strings.HasPrefix(line, "shutdown timed out while ") {
			return line, false
		}
		known := false
		for _, message := range []string{"context deadline exceeded", "REQUEST_TIMED_OUT", "NOT_COORDINATOR", "COORDINATOR_NOT_AVAILABLE", "UNKNOWN_MEMBER_ID", "ILLEGAL_GENERATION", "NOT_LEADER_OR_FOLLOWER", "BROKER_NOT_AVAILABLE", "connection refused", "i/o timeout", "EOF", "delivery timeout", "outstanding writes did not finish within 10 seconds"} {
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
		var err error
		state.Topology[side], err = s.topology(ctx, side, topic)
		if err != nil {
			state.QueryErrors = append(state.QueryErrors, brokerDiagnosticText(err.Error()))
			return state, false
		}
	}
	var err error
	var found bool
	state.Pending, state.Worker, found, err = s.currentProgress(s.workers[len(s.workers)-1])
	require.NoError(s.t, err, "read current broker worker progress")
	if !found {
		state.QueryErrors = append(state.QueryErrors, "CLI has not emitted complete progress")
		return state, false
	}
	parts, err := s.metadata["source"].FetchOffsetsForTopics(ctx, s.group, s.src)
	if err != nil || parts.Error() != nil || len(parts[s.src]) != replicatedPartitions {
		state.QueryErrors = append(state.QueryErrors, brokerDiagnosticText(fmt.Sprintf("source saved offsets: request=%v partitions=%v count=%d", err, parts.Error(), len(parts[s.src]))))
		return state, false
	}
	state.Saved = make([]int64, replicatedPartitions)
	for partition, part := range parts[s.src] {
		state.Saved[partition] = part.At
	}
	ends, err := s.metadata["destination"].ListEndOffsets(ctx, s.dst)
	if err != nil || ends.Error() != nil || len(ends[s.dst]) != replicatedPartitions {
		state.QueryErrors = append(state.QueryErrors, brokerDiagnosticText(fmt.Sprintf("destination end offsets: request=%v partitions=%v count=%d", err, ends.Error(), len(ends[s.dst]))))
		return state, false
	}
	state.Ends = make([]int64, replicatedPartitions)
	for partition, part := range ends[s.dst] {
		state.Ends[partition] = part.Offset
	}
	return state, true
}

func (s *brokerCopy) topology(ctx context.Context, side, topic string) ([]faults.Partition, error) {
	metadata, err := s.metadata[side].Metadata(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("%s topology request: %w", side, err)
	}
	if err := metadata.Topics[topic].Err; err != nil {
		return nil, fmt.Errorf("%s topology topic: %w", side, err)
	}
	if len(metadata.Topics[topic].Partitions) != replicatedPartitions {
		return nil, fmt.Errorf("%s topology partition_count=%d", side, len(metadata.Topics[topic].Partitions))
	}
	var result []faults.Partition
	for partition := int32(0); partition < replicatedPartitions; partition++ {
		part := metadata.Topics[topic].Partitions[partition]
		if part.Err != nil {
			return nil, fmt.Errorf("%s topology partition %d: %w", side, partition, part.Err)
		}
		result = append(result, faults.Partition{Leader: part.Leader, ISR: part.ISR})
	}
	return result, nil
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
		started := time.Now()
		s.healthChecks = nil
		s.conditionQueries = nil
		state, ok := s.observe(query)
		ready := ok && condition(query, state)
		poll := brokerPoll{Label: label, DurationMS: time.Since(started).Milliseconds(), Complete: ok, State: state, Health: s.healthChecks, Conditions: s.conditionQueries}
		if err := query.Err(); err != nil {
			poll.QueryError = err.Error()
		}
		s.recordPoll(poll)
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
	check := brokerHealthCheck{At: time.Now().UTC()}
	defer func() { s.healthChecks = append(s.healthChecks, check) }()
	args := []string{"inspect", "--format", brokerHealthFormat}
	for _, target := range targets {
		args = append(args, target.Container)
		check.Containers = append(check.Containers, target.Container)
	}
	run := brokerHealthDocker
	if s.lab != nil && s.lab.Docker != nil {
		run = s.lab.Docker
	}
	data, err := run(ctx, args...)
	check.Output = brokerDiagnosticText(string(data))
	if err != nil {
		check.Diagnostic = "unavailable"
		if errors.Is(err, errBrokerHealthTruncated) {
			check.Diagnostic = "truncated"
		}
		check.Error = brokerDiagnosticText(err.Error())
		return false
	}
	check.Details, check.Diagnostic, err = brokerHealthSnapshots(data, len(targets))
	if err != nil {
		check.Error = brokerDiagnosticText(err.Error())
		return false
	}
	var status strings.Builder
	ready := true
	for _, detail := range check.Details {
		fmt.Fprintf(&status, "%t %s\n", *detail.Running, detail.Health.Status)
		ready = ready && *detail.Running && detail.Health.Status == "healthy"
	}
	check.Output, check.Passed = status.String(), ready
	return ready
}

func (s *brokerCopy) inject(role faults.Role, kind string, before brokerObservation) (*faults.Injection, int) {
	require.True(s.t, brokerHealthyCopy(before), "fault requires healthy verified plaintext copying before injection")
	process := s.workers[len(s.workers)-1]
	require.False(s.t, process.finished(), "fault requires a live current worker")
	require.NotNil(s.t, before.Worker)
	require.Equal(s.t, s.lifetime(process), before.Worker.Lifetime, "fault baseline must belong to this worker lifetime")
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
	s.active, s.activeWindow, s.beforeFault = injection, window, before
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
		// The worker may exit during the successful health query, after wait's
		// liveness check. Classify that completed lifetime at the restart boundary.
		require.NoError(s.t, restartBrokerWorker(s.workers[len(s.workers)-1], allowFailure, func() {
			s.measureLast("broker fault fail-fast exit; restarted after health and ISR recovery")
			s.start()
		}, s.failureProof(s.workers[len(s.workers)-1])))
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
	injection, window := s.inject(faults.Role{Kind: "leader", Cluster: side, Topic: topic, Partition: 0}, "kill", before)
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
		before := s.wait("healthy prefix before next rolling broker", false, func(_ context.Context, state brokerObservation) bool { return brokerHealthyCopy(state) })
		injection, window := s.inject(faults.Role{Kind: "broker", Cluster: side, Broker: broker}, "restart", before)
		after := s.heal(injection, window, before)
		s.events = append(s.events, map[string]any{"fault": injection.Event, "before": before, "recovered": after})
	}
}

func (s *brokerCopy) isrLoss(before brokerObservation) {
	injection, window := s.inject(faults.Role{Kind: "leader", Cluster: "destination", Topic: s.dst, Partition: 0, Listener: "internal"}, "cut", before)
	broker := injection.Event.Targets[0].Broker
	controllers, err := s.lab.Resolve(s.ctx, faults.Role{Kind: "cluster", Cluster: "destination", Listener: "controller"})
	require.NoError(s.t, err)
	require.Len(s.t, controllers, 3)
	partition := -1
	isr := s.wait("actual ISR1 with the leader and controller quorum alive", false, func(query context.Context, state brokerObservation) bool {
		selected, ok := brokerISRPartition(injection.Event, before, state)
		if !ok || !s.healthy(query, controllers...) || s.destinationMinISR(query) != "2" {
			return false
		}
		partition = selected
		return true
	})
	isr.BrokerHealthy = true
	stalled := s.wait("new source input cannot advance destination or saved target prefix below min ISR", false, func(_ context.Context, state brokerObservation) bool {
		part := state.Topology["destination"][partition]
		if part.Leader != broker || len(part.ISR) != 1 {
			return false
		}
		require.Equal(s.t, isr.Ends[partition], state.Ends[partition], "destination acknowledged output below min ISR2")
		require.LessOrEqual(s.t, state.Saved[partition], isr.Ends[partition], "saved progress passed acknowledged destination prefix")
		return state.Publishing.Acknowledged[partition] > isr.Publishing.Acknowledged[partition]+3 && state.Pending.Submitted > state.Pending.Confirmed
	})
	s.oracle.command(s.t, map[string]any{"command": "sample", "reason": "observed-isr1-stall"})
	pid := s.workers[0].pid()
	healed := s.heal(injection, window, stalled)
	require.Len(s.t, s.workers, 1, "ISR loss requires retry by the same process")
	require.Equal(s.t, pid, s.workers[0].pid())
	s.events = append(s.events, map[string]any{"fault": injection.Event, "before": before, "isr_partition": partition, "isr_lost": isr, "stalled": stalled, "recovered": healed, "min_insync_replicas": 2, "controller_targets": controllers})
}

func (s *brokerCopy) destinationMinISR(ctx context.Context) string {
	configs, err := s.metadata["destination"].DescribeTopicConfigs(ctx, s.dst)
	if err != nil || len(configs) != 1 || configs[0].Err != nil {
		s.conditionQueries = append(s.conditionQueries, brokerDiagnosticText(fmt.Sprintf("destination minISR: request=%v config=%v", err, configs)))
		return ""
	}
	value := configValue(configs[0].Configs, "min.insync.replicas")
	s.conditionQueries = append(s.conditionQueries, "destination minISR: "+value)
	return value
}
