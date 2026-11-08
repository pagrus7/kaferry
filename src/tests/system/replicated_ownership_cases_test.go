//go:build integration && fault && load

package compat

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/src/tests/system/faults"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestReplicatedOwnershipCases(t *testing.T) {
	for _, name := range []string{"worker-kill-rebalance", "coordinator-kill-rebalance", "worker-scale"} {
		t.Run(name, func(t *testing.T) { replicatedOwnershipCase(t, name) })
	}
}

type ownershipCaseObservation struct {
	Started         time.Time            `json:"started"`
	Finished        time.Time            `json:"finished"`
	State           string               `json:"state"`
	Coordinator     int32                `json:"coordinator"`
	Members         []ownershipMember    `json:"members"`
	Saved           []int64              `json:"saved"`
	Generations     map[string]int32     `json:"accepted_generations,omitempty"`
	PriorGeneration int32                `json:"prior_accepted_generation,omitempty"`
	Publishing      replicatedPublishing `json:"publishing"`
	Alive           map[int]bool         `json:"processes_alive"`
	Error           string               `json:"error,omitempty"`
	SourceTopology  []faults.Partition   `json:"source_topology,omitempty"`
}

type ownershipCaseWorker struct {
	route       int
	process     *cliProcess
	started     time.Time
	exited      <-chan time.Time
	memory      func() rssPeak
	reason      string
	terminal    string
	measurement map[string]any
}

type ownershipCase struct {
	t                                   *testing.T
	ctx                                 context.Context
	lab                                 *faults.Lab
	metadata                            faults.KafkaMetadata
	id, src, dst, group, prefix, config string
	directory                           string
	timeouts                            map[string]time.Duration
	oracle                              *faultOracle
	audit                               *replicatedAudit
	publisher                           *replicatedPublisher
	workers                             []*ownershipCaseWorker
	states                              map[string]ownershipCaseObservation
	events                              []map[string]any
	failedAfter                         time.Time
	safety                              map[string]string
	complete                            bool
}

func replicatedOwnershipCase(t *testing.T, name string) {
	// Given the exact selected CLI, independent publisher/oracle routes, and
	// 24 retained RF=3/minISR=2 partitions on each side of the owned fixture.
	for _, key := range []string{"KAFERRY_FIXTURE_PROJECT", "KAFERRY_TOXIPROXY_API", "KAFERRY_TEST_BINARY", "KAFERRY_EXPECT_BINARY_SHA256"} {
		require.NotEmpty(t, os.Getenv(key), key)
	}
	id := "ownership/" + name
	timeouts := faultTimeouts(t, id)
	want := map[string]time.Duration{"session.timeout.ms": 6 * time.Second, "socket.timeout.ms": 8 * time.Second,
		"max.poll.interval.ms": 30 * time.Second, "progress_save_timeout": 10 * time.Second,
		"delivery_timeout": 60 * time.Second, "shutdown_timeout": 5 * time.Second}
	if name == "coordinator-kill-rebalance" {
		want["replica.lag.time.max.ms"] = 30 * time.Second
	}
	require.Equal(t, want, timeouts, "register the actual SQL, worker and broker bounds")
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
	directory, err := filepath.Abs(filepath.Join(directory, "ownership-"+name))
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(directory, 0700))
	metadata, err := faults.NewMetadata()
	require.NoError(t, err)
	t.Cleanup(metadata.Close)
	s := &ownershipCase{t: t, ctx: ctx, id: id, directory: directory, timeouts: timeouts, metadata: metadata,
		states: make(map[string]ownershipCaseObservation), prefix: fmt.Sprintf("kaferry-ownership-%d", time.Now().UnixNano())}
	s.lab = &faults.Lab{Project: os.Getenv("KAFERRY_FIXTURE_PROJECT"), API: os.Getenv("KAFERRY_TOXIPROXY_API"), Metadata: metadata}
	s.src, s.dst, s.group = s.prefix+"-source", s.prefix+"-destination", s.prefix+"-group"
	replicatedTopic(ctx, t, metadata["source"], s.src)
	replicatedTopic(ctx, t, metadata["destination"], s.dst)
	if name == "coordinator-kill-rebalance" {
		configs, err := metadata["source"].DescribeBrokerConfigs(ctx, 1, 2, 3)
		require.NoError(t, err)
		require.Len(t, configs, 3)
		for _, broker := range configs {
			require.NoError(t, broker.Err)
			require.Equal(t, fmt.Sprint(timeouts["replica.lag.time.max.ms"].Milliseconds()), configValue(broker.Configs, "replica.lag.time.max.ms"))
		}
	}
	s.oracle = startFaultOracle(t, id, strings.Join(faults.Endpoints("source", 0), ","),
		strings.Join(faults.Endpoints("destination", 0), ","), s.src, s.dst, s.group, replicatedPartitions, false)
	s.audit = startReplicatedAudit(ctx, t, s.lab, directory)
	s.audit.capture(ctx, t, "baseline")
	settings := zombieWorkerSettings(timeouts)
	s.config = configuration(t, settings)
	require.NoError(t, os.WriteFile(filepath.Join(directory, "worker-settings.toml"), []byte("[worker]\n"+settings), 0600))
	t.Cleanup(s.retain)
	s.start(1)
	producer := fixtureClient(t, strings.Join(faults.Endpoints("source", 0), ","), kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.MaxBufferedRecords(2*replicatedPartitions), kgo.RecordDeliveryTimeout(timeouts["delivery_timeout"]), kgo.AllowIdempotentProduceCancellation())
	s.publisher = startReplicatedBrokerPublisher(ctx, producer, s.src, timeouts["delivery_timeout"])
	t.Cleanup(func() { s.publisher.stop() })
	s.stable("baseline", []int{1}, nil, 0)

	// When membership changes under continuous publication, the kill cases
	// hold the old worker to expose an actual broker-observed rebalance.
	if name == "worker-scale" {
		s.scale()
	} else {
		s.killDuringRebalance(name == "coordinator-kill-rebalance")
	}

	// Then every acknowledged record is saved, every writer has an observed
	// OS exit, and the shared librdkafka oracle covers the complete horizon.
	publication := s.publisher.stop()
	require.NoError(t, publication.err)
	require.Equal(t, publication.Offered, publication.Acknowledged, "source publications must all resolve")
	require.LessOrEqual(t, publication.MaxOfferGapSeconds, 2.0)
	s.poll("all acknowledged input saved", false, func(_ context.Context, state ownershipCaseObservation) bool {
		return slices.Equal(state.Saved, publication.Acknowledged)
	})
	for _, worker := range s.workers {
		if worker.reason == "" {
			s.stop(worker, "completed", os.Interrupt)
		}
	}
	s.oracle.finish(t)
	s.audit.mark(ctx, t, "final")
	s.audit.capture(ctx, t, "final")
	safetyPath := filepath.Join(filepath.Dir(s.oracle.log), "observations", "case.json")
	data, err := os.ReadFile(safetyPath)
	require.NoError(t, err)
	relative, err := filepath.Rel(filepath.Dir(directory), safetyPath)
	require.NoError(t, err)
	s.safety = map[string]string{"observations": relative, "sha256": fmt.Sprintf("%x", sha256.Sum256(data))}
	s.complete = true
}

func (s *ownershipCase) start(route int) *ownershipCaseWorker {
	marker := fmt.Sprintf("%s-worker%d", s.prefix, route)
	statement := zombieWorkerSQL(s.timeouts, route, marker, s.src, s.dst, s.group)
	statement = strings.Replace(statement, "'heartbeat.interval.ms'='1000'", fmt.Sprintf("'heartbeat.interval.ms'='1000','max.poll.interval.ms'='%d'", s.timeouts["max.poll.interval.ms"].Milliseconds()), 1)
	path := filepath.Join(s.directory, fmt.Sprintf("worker-route-%d.sql", route))
	if previous, err := os.ReadFile(path); err == nil {
		require.Equal(s.t, statement, string(previous), "restart must use the exact same SQL")
	} else {
		require.ErrorIs(s.t, err, os.ErrNotExist)
		require.NoError(s.t, os.WriteFile(path, []byte(statement), 0600))
	}
	worker := &ownershipCaseWorker{route: route, started: time.Now().UTC()}
	worker.process = startCLI(s.t, s.ctx, testBinary(s.t), "", "run", "-e", statement, "-c", s.config)
	worker.exited, worker.memory = observeReplicatedExit(worker.process), watchReplicatedRSS(s.t, worker.process)
	s.workers = append(s.workers, worker)
	s.oracle.writer(worker.process)
	return worker
}

func (s *ownershipCase) observe(ctx context.Context) ownershipCaseObservation {
	state := ownershipCaseObservation{Started: time.Now().UTC(), Publishing: s.publisher.snapshot(), Alive: make(map[int]bool)}
	for _, worker := range s.workers {
		state.Alive[worker.process.pid()] = !worker.process.finished()
	}
	groups, err := s.metadata["source"].DescribeGroups(ctx, s.group)
	if err == nil {
		group, ok := groups[s.group]
		if !ok {
			err = fmt.Errorf("independent group describe omitted the selected group")
		} else if group.Err != nil {
			err = group.Err
		} else {
			state.State = group.State
			for _, member := range group.Members {
				owned := ownershipMember{ClientID: member.ClientID, MemberID: member.MemberID}
				if assignment, ok := member.Assigned.AsConsumer(); ok {
					for _, topic := range assignment.Topics {
						if topic.Topic != s.src {
							err = fmt.Errorf("group member owns unexpected topic %q", topic.Topic)
						}
						owned.Partitions = append(owned.Partitions, topic.Partitions...)
					}
				}
				slices.Sort(owned.Partitions)
				state.Members = append(state.Members, owned)
			}
		}
	}
	if err == nil {
		state.Coordinator, err = s.metadata.Coordinator(ctx, "source", s.group)
	}
	if err == nil {
		parts, queryErr := s.metadata["source"].FetchOffsetsForTopics(ctx, s.group, s.src)
		err = queryErr
		if err == nil {
			err = parts.Error()
		}
		if err == nil && len(parts[s.src]) == replicatedPartitions {
			state.Saved = make([]int64, replicatedPartitions)
			for partition, part := range parts[s.src] {
				state.Saved[partition] = part.At
			}
		}
	}
	if err != nil {
		state.Error = err.Error()
	}
	state.Finished = time.Now().UTC()
	return state
}

func (s *ownershipCase) poll(label string, allowFaultExit bool, condition func(context.Context, ownershipCaseObservation) bool) ownershipCaseObservation {
	deadline, cancel := context.WithTimeout(s.ctx, time.Minute)
	defer cancel()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		s.live(allowFaultExit)
		require.NoError(s.t, s.publisher.snapshot().err, "steady independent publisher failed")
		query, done := context.WithTimeout(deadline, 2*time.Second)
		state := s.observe(query)
		s.journal(state)
		ready := state.Error == "" && condition(query, state)
		done()
		if ready {
			state.Finished = time.Now().UTC()
			return state
		}
		select {
		case <-deadline.Done():
			s.t.Fatalf("required observed state never proved: %s; last=%+v", label, state)
		case <-tick.C:
		}
	}
}

func ownershipCaseMembers(state ownershipCaseObservation, markers map[int]string) (map[int]ownershipMember, bool) {
	if state.State != "Stable" || len(state.Members) != len(markers) {
		return nil, false
	}
	members, partitions := make(map[int]ownershipMember), make(map[int32]bool)
	for _, member := range state.Members {
		matched := 0
		for route, marker := range markers {
			if strings.HasSuffix(member.ClientID, marker) {
				if _, exists := members[route]; exists {
					return nil, false
				}
				members[route], matched = member, route
			}
		}
		if matched == 0 || member.MemberID == "" || len(member.Partitions) == 0 {
			return nil, false
		}
		for _, partition := range member.Partitions {
			if partition < 0 || partition >= replicatedPartitions || partitions[partition] {
				return nil, false
			}
			partitions[partition] = true
		}
	}
	return members, len(partitions) == replicatedPartitions
}

func (s *ownershipCase) stable(label string, routes []int, before []int64, generation int32) ownershipCaseObservation {
	markers := make(map[int]string, len(routes))
	for _, route := range routes {
		markers[route] = fmt.Sprintf("%s-worker%d", s.prefix, route)
	}
	var accepted ownershipCaseObservation
	s.poll(label, false, func(query context.Context, state ownershipCaseObservation) bool {
		members, ok := ownershipCaseMembers(state, markers)
		if !ok || !allAbove(state.Saved, ownershipCaseMinimum(before), 2) {
			return false
		}
		state.Generations = make(map[string]int32)
		minimum := make(map[int32]int64)
		for partition, offset := range ownershipCaseMinimum(before) {
			minimum[int32(partition)] = offset + 2
		}
		var current int32
		for _, member := range members {
			observed, ready, err := s.audit.ready(query, member.ClientID, s.group, member.MemberID, member.Partitions, minimum)
			require.NoError(s.t, err, "actual accepted generation audit unavailable")
			if !ready || observed <= generation || current != 0 && current != observed {
				return false
			}
			current = observed
			state.Generations[member.ClientID] = observed
		}
		if before != nil && !allAbove(state.Publishing.Acknowledged, s.states["baseline"].Publishing.Acknowledged, 2) {
			return false
		}
		state.Finished = time.Now().UTC()
		accepted = state
		return true
	})
	s.states[label] = accepted
	s.journal(accepted)
	return accepted
}

func ownershipCaseMinimum(before []int64) []int64 {
	if before == nil {
		return make([]int64, replicatedPartitions)
	}
	return before
}

func ownershipCaseGeneration(state ownershipCaseObservation) int32 {
	var result int32
	for _, generation := range state.Generations {
		result = max(result, generation)
	}
	return result
}

func (s *ownershipCase) rebalance(ctx context.Context) (ownershipCaseObservation, bool) {
	state := s.observe(ctx)
	state.PriorGeneration = ownershipCaseGeneration(s.states["baseline"])
	if state.Error != "" || state.State != "PreparingRebalance" || len(state.Members) < 2 {
		return state, false
	}
	baseline := s.states["baseline"].Members[0]
	old, peer := false, false
	for _, member := range state.Members {
		old = old || member.MemberID == baseline.MemberID && member.ClientID == baseline.ClientID
		peer = peer || strings.HasSuffix(member.ClientID, s.prefix+"-worker2") && member.MemberID != baseline.MemberID
	}
	return state, old && peer
}

func (s *ownershipCase) killDuringRebalance(coordinator bool) {
	baseline := s.states["baseline"]
	role := faults.Role{Kind: "coordinator", Cluster: "source", Group: s.group}
	window := s.oracle.begin(s.t, "held-worker-rebalance", map[string]any{"held_worker": s.workers[0].process.pid(), "coordinator_role": role}, coordinator,
		s.timeouts, "one current-generation worker has accepted nonempty saved progress on all 24 partitions; SIGSTOP precedes the peer join")
	s.audit.faultAttempted = true
	old := s.workers[0]
	s.signal(old, syscall.SIGSTOP, "hold old member while a selected-binary peer joins")
	s.poll("OS confirms the original worker stopped", false, func(query context.Context, _ ownershipCaseObservation) bool {
		state, err := exec.CommandContext(query, "ps", "-o", "state=", "-p", fmt.Sprint(old.process.pid())).Output()
		require.NoError(s.t, err)
		if !strings.Contains(string(state), "T") {
			return false
		}
		s.events = append(s.events, map[string]any{"pid": old.process.pid(), "route": old.route,
			"command": []string{"ps", "-o", "state=", "-p", fmt.Sprint(old.process.pid())}, "stdout": string(state),
			"os_process_state": strings.TrimSpace(string(state)), "observed_at": time.Now().UTC(), "reason": "OS confirms held worker"})
		return true
	})
	s.start(2)
	var trigger ownershipCaseObservation
	s.poll("actual preparing rebalance with the held original member and selected peer", false, func(query context.Context, _ ownershipCaseObservation) bool {
		var ready bool
		trigger, ready = s.rebalance(query)
		return ready
	})
	s.states["preparing_rebalance"] = trigger
	s.journal(trigger)
	if coordinator {
		s.killCoordinator(window, old)
	} else {
		// Recheck independently immediately before the OS kill; elapsed time is
		// never accepted as proof that the group still has an active rebalance.
		query, done := context.WithTimeout(s.ctx, 2*time.Second)
		atKill, ready := s.rebalance(query)
		done()
		require.True(s.t, ready, "rebalance ended before worker kill: %+v", atKill)
		s.live(false)
		s.states["at_kill"] = atKill
		s.journal(atKill)
		s.stop(old, "SIGKILL during independently observed PreparingRebalance", syscall.SIGKILL)
		s.oracle.injected(s.t, window, "worker-sigkill", map[string]any{"pid": old.process.pid(), "route": 1, "state": atKill}, "OS wait proves SIGKILL after the independent rebalance observation")
		s.stable("recovered", []int{2}, baseline.Saved, ownershipCaseGeneration(baseline))
		s.oracle.healed(s.t, window, "selected peer owns all 24 partitions and saves in the independently observed later generation")
	}
}

func (s *ownershipCase) killCoordinator(window int, old *ownershipCaseWorker) {
	// The native injector resolves the live coordinator. Intercept only its
	// final kill command to prove the same coordinator and rebalance still hold.
	s.lab.Docker = func(ctx context.Context, args ...string) ([]byte, error) {
		if len(args) > 0 && args[0] == "kill" {
			atKill, ready := s.rebalance(ctx)
			if !ready {
				return nil, fmt.Errorf("coordinator kill lost required rebalance: %+v", atKill)
			}
			data, err := faults.Docker(ctx, "inspect", "--format", `{{index .Config.Labels "com.docker.compose.service"}}`, args[len(args)-1])
			if err != nil || strings.TrimSpace(string(data)) != fmt.Sprintf("source-%d", atKill.Coordinator) {
				return nil, fmt.Errorf("resolved target is no longer the independently observed coordinator: %w", err)
			}
			s.live(false)
			s.states["at_kill"] = atKill
			s.journal(atKill)
			s.failedAfter = time.Now().UTC()
		}
		data, err := faults.Docker(ctx, args...)
		if len(args) > 0 && args[0] == "kill" && err == nil {
			s.events = append(s.events, map[string]any{"coordinator_kill_started": s.failedAfter, "coordinator_kill_finished": time.Now().UTC(), "container": args[len(args)-1]})
		}
		return data, err
	}
	injection, err := s.lab.Inject(s.ctx, faults.Role{Kind: "coordinator", Cluster: "source", Group: s.group}, faults.Fault{Kind: "kill"})
	if injection != nil {
		s.events = append(s.events, map[string]any{"fault": &injection.Event})
		s.t.Cleanup(func() {
			cleanup, done := context.WithTimeout(context.Background(), 30*time.Second)
			defer done()
			if err := injection.Heal(cleanup); err != nil {
				s.t.Error("restore original resolved coordinator: ", err)
			}
		})
	}
	require.NoError(s.t, err)
	require.NotEmpty(s.t, injection.Event.Targets)
	s.signal(old, syscall.SIGCONT, "resume held worker after coordinator kill")
	s.oracle.injected(s.t, window, "coordinator-kill", injection.Event, "native Docker kill completed on the rechecked actual coordinator during PreparingRebalance")
	killed := injection.Event.Targets[0]
	s.states["coordinator_replacement"] = s.poll("different coordinator while the original broker remains stopped", true,
		func(query context.Context, state ownershipCaseObservation) bool {
			return state.Coordinator != killed.Broker && state.Coordinator > 0 &&
				allAbove(state.Publishing.Acknowledged, s.states["at_kill"].Publishing.Acknowledged, 2) &&
				s.brokerInspection(query, killed, false, "coordinator-stopped.inspect.json")
		})
	require.NoError(s.t, injection.Heal(s.ctx))
	s.poll("restored broker API readiness before scoped logging", true, func(query context.Context, _ ownershipCaseObservation) bool {
		return (&brokerCopy{}).healthy(query, killed)
	})
	// Reapply scoped logging before the metadata used for recovery proof, as
	// well as the accepted post-recovery generations and final capture guards.
	require.NoError(s.t, s.audit.level(s.ctx, killed.Broker, requestLogger, "DEBUG"))
	require.NoError(s.t, s.audit.level(s.ctx, killed.Broker, receivedLogger, "TRACE"))
	s.events = append(s.events, map[string]any{"broker": killed.Broker, "audit_logging_reenabled_at": time.Now().UTC()})
	var restoredTopology []faults.Partition
	s.states["broker_restored"] = s.poll("restored coordinator broker health and all 24 ISR sets", true,
		func(query context.Context, _ ownershipCaseObservation) bool {
			probe := &brokerCopy{metadata: s.metadata, src: s.src, dst: s.dst}
			parts, ok := probe.topology(query, "source", s.src)
			if !ok || !probe.healthy(query, killed) {
				return false
			}
			for _, partition := range parts {
				if len(partition.ISR) != 3 {
					return false
				}
			}
			restoredTopology = parts
			return s.brokerInspection(query, killed, true, "coordinator-restored.inspect.json")
		})
	restored := s.states["broker_restored"]
	restored.SourceTopology = restoredTopology
	s.states["broker_restored"] = restored
	// A coordinator error may terminate a periodic save under the existing
	// product contract. Restart each affected route at most once, after health
	// and ISR recovery, with its exact binary, SQL, configuration and group.
	var restart []int
	for _, worker := range s.workers {
		if worker.reason == "coordinator fault fail-fast exit" {
			restart = append(restart, worker.route)
		}
	}
	for _, route := range restart {
		s.start(route)
	}
	s.poll("new source publication after broker restoration", false, func(_ context.Context, state ownershipCaseObservation) bool {
		return allAbove(state.Publishing.Acknowledged, restored.Publishing.Acknowledged, 2)
	})
	s.stable("recovered", []int{1, 2}, restored.Saved, ownershipCaseGeneration(s.states["baseline"]))
	s.events = append(s.events, map[string]any{"restart_routes": restart,
		"restart_scope": "one restart per affected route after broker health and ISR3; same selected executable, SQL, group and configuration"})
	s.oracle.healed(s.t, window, "original broker restored with ISR3; both selected routes own and save in a later accepted generation")
}

func (s *ownershipCase) brokerInspection(ctx context.Context, target faults.Target, running bool, name string) bool {
	format := `{"id":{{json .Id}},"running":{{json .State.Running}},"health":{{json .State.Health.Status}},"project":{{json (index .Config.Labels "com.docker.compose.project")}},"service":{{json (index .Config.Labels "com.docker.compose.service")}}}`
	started := time.Now().UTC()
	data, err := faults.Docker(ctx, "inspect", "--format", format, target.Container)
	if err != nil {
		return false
	}
	var observed struct {
		ID, Health, Project, Service string
		Running                      bool
	}
	require.NoError(s.t, json.Unmarshal(data, &observed))
	if observed.ID != target.Container || observed.Project != s.lab.Project || observed.Service != target.Service ||
		observed.Running != running || running && observed.Health != "healthy" {
		return false
	}
	require.NoError(s.t, os.WriteFile(filepath.Join(s.directory, name), data, 0600))
	s.events = append(s.events, map[string]any{"broker_inspection": name, "started": started, "finished": time.Now().UTC(), "container": target.Container})
	return true
}

func (s *ownershipCase) scale() {
	baseline := s.states["baseline"]
	window := s.oracle.begin(s.t, "scale-up", map[string]any{"from": 1, "to": 3}, false, s.timeouts, "one selected worker owns and saves all source partitions")
	s.audit.faultAttempted = true
	s.start(2)
	s.start(3)
	s.oracle.injected(s.t, window, "worker-start", map[string]any{"routes": []int{1, 2, 3}}, "two additional exact-binary workers started while publication continues")
	up := s.stable("scaled_up", []int{1, 2, 3}, baseline.Saved, ownershipCaseGeneration(baseline))
	s.oracle.healed(s.t, window, "three distinct current-generation workers own disjoint nonempty assignments and save every source partition")
	window = s.oracle.begin(s.t, "scale-down", map[string]any{"from": 3, "to": 1}, false, s.timeouts, "independent three-worker membership and accepted current-generation progress")
	for _, worker := range s.workers[1:] {
		s.signal(worker, os.Interrupt, "scale down to the original selected worker")
	}
	for _, worker := range s.workers[1:] {
		s.await(worker, "scale-down", false)
	}
	s.oracle.injected(s.t, window, "worker-stop", map[string]any{"routes": []int{2, 3}}, "both removed workers completed successful OS waits")
	down := s.stable("scaled_down", []int{1}, up.Saved, ownershipCaseGeneration(up))
	require.True(s.t, allAbove(down.Publishing.Acknowledged, up.Publishing.Acknowledged, 2), "publishing must continue through scale down")
	s.states["recovered"] = down
	s.oracle.healed(s.t, window, "original process owns all 24 partitions and saves in a later generation; no worker restarted")
}

func (s *ownershipCase) signal(worker *ownershipCaseWorker, signal os.Signal, reason string) {
	require.False(s.t, worker.process.finished(), "worker exited before expected signal")
	started := time.Now().UTC()
	require.NoError(s.t, worker.process.signal(signal))
	s.events = append(s.events, map[string]any{"signal": signal.String(), "signal_number": int(signal.(syscall.Signal)), "pid": worker.process.pid(), "route": worker.route,
		"started": started, "finished": time.Now().UTC(), "reason": reason})
}

func (s *ownershipCase) stop(worker *ownershipCaseWorker, reason string, signal os.Signal) {
	s.signal(worker, signal, reason)
	s.await(worker, reason, signal == syscall.SIGKILL)
}

func (s *ownershipCase) await(worker *ownershipCaseWorker, reason string, killed bool) {
	select {
	case <-worker.process.done:
	case <-time.After(s.timeouts["shutdown_timeout"] + time.Second):
		s.t.Fatal("selected CLI did not stop within its configured shutdown bound")
	}
	if killed {
		require.Error(s.t, worker.process.wait())
		status, ok := worker.process.command.ProcessState.Sys().(syscall.WaitStatus)
		require.True(s.t, ok)
		require.Equal(s.t, syscall.SIGKILL, status.Signal(), "OS wait must prove the requested kill")
	} else {
		require.NoError(s.t, worker.process.wait(), worker.process.logs())
	}
	worker.reason = reason
	s.measure(worker, killed)
}

func (s *ownershipCase) live(allowFaultExit bool) {
	for _, worker := range s.workers {
		if worker.reason != "" || !worker.process.finished() {
			continue
		}
		require.True(s.t, allowFaultExit && !s.failedAfter.IsZero(), "unexplained or premature CLI exit: %s", worker.process.logs())
		require.Equal(s.t, 1, worker.process.command.ProcessState.ExitCode(), worker.process.logs())
		terminal, known := ownershipCoordinatorFailure(worker.process.logs())
		require.True(s.t, known, "coordinator fault produced an unexplained terminal failure: %s", worker.process.logs())
		worker.reason, worker.terminal = "coordinator fault fail-fast exit", terminal
		s.measure(worker, false)
		require.False(s.t, worker.measurement["stopped_at"].(time.Time).Before(s.failedAfter), "exit preceded actual coordinator kill")
	}
	for _, worker := range s.workers {
		if worker.reason == "" {
			logs, err := worker.process.currentLogs()
			require.NoError(s.t, err)
			progress, _, _, err := ownershipCaseWork(logs)
			require.NoError(s.t, err)
			require.LessOrEqual(s.t, progress.PeakRecords, 1024)
			require.LessOrEqual(s.t, progress.PeakBytes, int64(134217728))
		}
	}
}

func ownershipCoordinatorFailure(logs string) (string, bool) {
	var terminal []string
	for _, line := range strings.Split(logs, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || json.Valid([]byte(line)) {
			continue
		}
		known := false
		for _, cause := range []string{"NOT_COORDINATOR", "COORDINATOR_NOT_AVAILABLE", "COORDINATOR_LOAD_IN_PROGRESS", "UNKNOWN_MEMBER_ID", "ILLEGAL_GENERATION", "REBALANCE_IN_PROGRESS", "consumer group ownership unavailable for commit", "context deadline exceeded"} {
			known = known || strings.Contains(line, cause)
		}
		if !known {
			return strings.Join(append(terminal, line), "\n"), false
		}
		terminal = append(terminal, line)
	}
	return strings.Join(terminal, "\n"), len(terminal) > 0
}

func ownershipCaseWork(logs string) (copySummary, int, bool, error) {
	var peak copySummary
	observations, summary := 0, false
	complete := logs[:strings.LastIndexByte(logs, '\n')+1]
	for _, line := range strings.Split(complete, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "{") {
			continue
		}
		var row struct {
			Message        string `json:"msg"`
			Submitted      *int   `json:"submitted"`
			Confirmed      *int   `json:"confirmed"`
			PeakRecords    *int   `json:"peak_uncommitted_records"`
			PeakBytes      *int64 `json:"peak_uncommitted_bytes"`
			PendingRecords *int   `json:"pending_entries"`
			PendingBytes   *int64 `json:"pending_estimated_bytes"`
		}
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return copySummary{}, 0, false, fmt.Errorf("decode complete CLI measurement: %w", err)
		}
		if row.Message != "progress" && row.Message != "copy summary" {
			continue
		}
		records, bytes := row.PeakRecords, row.PeakBytes
		if row.Message == "progress" {
			records, bytes = row.PendingRecords, row.PendingBytes
		}
		if row.Submitted == nil || row.Confirmed == nil || records == nil || bytes == nil ||
			*row.Submitted < 0 || *row.Confirmed < 0 || *row.Confirmed > *row.Submitted || *records < 0 || *bytes < 0 {
			return copySummary{}, 0, false, fmt.Errorf("CLI cap evidence requires complete nonnegative counts")
		}
		observations++
		peak.PeakRecords, peak.PeakBytes = max(peak.PeakRecords, *records), max(peak.PeakBytes, *bytes)
		summary = summary || row.Message == "copy summary"
	}
	return peak, observations, summary, nil
}

func (s *ownershipCase) measure(worker *ownershipCaseWorker, killed bool) {
	logs, err := worker.process.currentLogs()
	require.NoError(s.t, err)
	memory := worker.memory()
	require.Positive(s.t, memory.samples)
	require.Zero(s.t, memory.failures)
	require.LessOrEqual(s.t, memory.peak*1024, int64(512<<20))
	peak, samples, summary, err := ownershipCaseWork(logs)
	require.NoError(s.t, err)
	require.Positive(s.t, samples, "every selected worker needs cap measurements")
	if killed {
		require.False(s.t, summary, "killed worker had already ended its copy loop")
	} else {
		require.True(s.t, summary, "worker needs its final copy summary")
	}
	require.LessOrEqual(s.t, peak.PeakRecords, 1024)
	require.LessOrEqual(s.t, peak.PeakBytes, int64(134217728))
	worker.measurement = map[string]any{"pid": worker.process.pid(), "route": worker.route, "started_at": worker.started, "stopped_at": <-worker.exited,
		"wait_status":   int(worker.process.command.ProcessState.Sys().(syscall.WaitStatus)),
		"binary_sha256": os.Getenv("KAFERRY_EXPECT_BINARY_SHA256"), "exit_code": worker.process.command.ProcessState.ExitCode(), "reason": worker.reason, "terminal_error": worker.terminal,
		"sampled_peak_rss_bytes": memory.peak * 1024, "rss_samples": memory.samples, "rss_failures": memory.failures,
		"record_cap": 1024, "byte_cap": 134217728, "peak_uncommitted_records": peak.PeakRecords, "peak_uncommitted_bytes": peak.PeakBytes,
		"progress_observations": samples, "final_summary": summary, "forced_kill": killed}
	if killed {
		worker.measurement["cap_measurement_scope"] = "live progress observations before SIGKILL; forced exit cannot emit a final summary"
	}
}

func (s *ownershipCase) journal(state ownershipCaseObservation) {
	data, err := json.Marshal(state)
	require.NoError(s.t, err)
	file, err := os.OpenFile(filepath.Join(s.directory, "ownership-observations.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	require.NoError(s.t, err)
	_, err = file.Write(append(data, '\n'))
	require.NoError(s.t, err)
	require.NoError(s.t, file.Close())
}

func (s *ownershipCase) retain() {
	measurements := make([]map[string]any, 0, len(s.workers))
	for index, worker := range s.workers {
		if !worker.process.finished() {
			_ = worker.process.command.Process.Kill()
		}
		_ = worker.process.wait()
		if err := os.WriteFile(filepath.Join(s.directory, fmt.Sprintf("worker-%d-route-%d.cli.log", index+1, worker.route)), []byte(worker.process.logs()), 0600); err != nil {
			s.t.Error("retain worker log: ", err)
		}
		if worker.measurement == nil {
			worker.measurement = map[string]any{"pid": worker.process.pid(), "route": worker.route, "started_at": worker.started,
				"exit_code": worker.process.command.ProcessState.ExitCode(), "measurement_incomplete": true}
		}
		measurements = append(measurements, worker.measurement)
		worker.measurement["log"] = fmt.Sprintf("worker-%d-route-%d.cli.log", index+1, worker.route)
	}
	var publishing replicatedPublishing
	if s.publisher != nil {
		publishing = s.publisher.snapshot()
	}
	var artifacts []map[string]string
	entries, err := os.ReadDir(s.directory)
	if err == nil {
		for _, entry := range entries {
			if entry.IsDir() || entry.Name() == "replicated-ownership-case.json" {
				continue
			}
			data, readErr := os.ReadFile(filepath.Join(s.directory, entry.Name()))
			if readErr != nil {
				s.t.Error("checksum ownership evidence: ", readErr)
				continue
			}
			artifacts = append(artifacts, map[string]string{"observations": entry.Name(), "sha256": fmt.Sprintf("%x", sha256.Sum256(data))})
		}
	} else {
		s.t.Error("list ownership evidence: ", err)
	}
	data, err := json.MarshalIndent(map[string]any{"schema": 1, "id": s.id, "complete": s.complete && !s.t.Failed(), "case_failed": s.t.Failed(),
		"source_partitions": replicatedPartitions, "source_topic": s.src, "destination_topic": s.dst, "group": s.group,
		"binary_sha256": os.Getenv("KAFERRY_EXPECT_BINARY_SHA256"), "states": s.states, "events": s.events, "publishing": publishing,
		"timeout_milliseconds": durationMilliseconds(s.timeouts), "required_recovery_seconds": s.oracle.recovery.Seconds(),
		"worker_measurements": measurements, "safety_case": s.safety, "audit_guard": s.audit.guard, "audit_marks": s.audit.marks,
		"fixture_project": s.lab.Project, "artifacts": artifacts, "observations": "ownership-observations.jsonl"}, "", "  ")
	if err == nil {
		err = os.WriteFile(filepath.Join(s.directory, "replicated-ownership-case.json"), append(data, '\n'), 0600)
	}
	if err != nil {
		s.t.Error("retain ownership case: ", err)
	}
}

func TestOwnershipCaseRejectsUnprovedMembership(t *testing.T) {
	// Given a complete stable assignment, when a member or partition changes,
	// then a matching count cannot substitute for the selected worker identities.
	parts := make([]int32, replicatedPartitions)
	for index := range parts {
		parts[index] = int32(index)
	}
	state := ownershipCaseObservation{State: "Stable", Members: []ownershipMember{{ClientID: "kaferry-worker1", MemberID: "member1", Partitions: parts}}}
	_, valid := ownershipCaseMembers(state, map[int]string{1: "worker1"})
	require.True(t, valid)
	for _, change := range []func(*ownershipCaseObservation){
		func(s *ownershipCaseObservation) { s.State = "PreparingRebalance" },
		func(s *ownershipCaseObservation) { s.Members[0].MemberID = "" },
		func(s *ownershipCaseObservation) { s.Members[0].ClientID = "other" },
		func(s *ownershipCaseObservation) { s.Members[0].Partitions[23] = 22 },
		func(s *ownershipCaseObservation) { s.Members[0].Partitions = s.Members[0].Partitions[:23] },
	} {
		candidate := state
		candidate.Members = slices.Clone(state.Members)
		candidate.Members[0].Partitions = slices.Clone(parts)
		change(&candidate)
		_, valid := ownershipCaseMembers(candidate, map[int]string{1: "worker1"})
		require.False(t, valid, "%+v", candidate)
	}
	// When two selected workers divide the same 24 partitions, then only
	// disjoint complete ownership passes; counts cannot hide overlap or loss.
	state.Members[0].Partitions = slices.Clone(parts[:12])
	state.Members = append(state.Members, ownershipMember{ClientID: "kaferry-worker2", MemberID: "member2", Partitions: slices.Clone(parts[12:])})
	_, valid = ownershipCaseMembers(state, map[int]string{1: "worker1", 2: "worker2"})
	require.True(t, valid)
	state.Members[1].Partitions[0] = 0
	_, valid = ownershipCaseMembers(state, map[int]string{1: "worker1", 2: "worker2"})
	require.False(t, valid)
}

func TestOwnershipCoordinatorRestartRequiresTerminalCause(t *testing.T) {
	// Given structured coordinator warnings, when the terminal failure is
	// unrelated or absent, then a warning cannot authorize a worker restart.
	warning := `{"msg":"source warning","error":"NOT_COORDINATOR"}` + "\n"
	for _, terminal := range []string{"", "unexpected invariant failure", "context deadline exceeded\nunexpected invariant failure", "EOF"} {
		_, known := ownershipCoordinatorFailure(warning + terminal)
		require.False(t, known, terminal)
	}
	terminal, known := ownershipCoordinatorFailure(warning + "recoverable operation: ILLEGAL_GENERATION\n")
	require.True(t, known)
	require.Equal(t, "recoverable operation: ILLEGAL_GENERATION", terminal)
}

func TestOwnershipKilledWorkerKeepsEarlierPeak(t *testing.T) {
	// Given live progress before an abrupt kill, when later progress is lower,
	// then cap evidence retains the highest observed values without a summary.
	logs := `{"msg":"progress","submitted":2000,"confirmed":975,"pending_entries":1025,"pending_estimated_bytes":134217729}` + "\n" +
		`{"msg":"progress","submitted":2000,"confirmed":1998,"pending_entries":2,"pending_estimated_bytes":10}` + "\n"
	peak, samples, summary, err := ownershipCaseWork(logs)
	require.NoError(t, err)
	require.Equal(t, 1025, peak.PeakRecords)
	require.Equal(t, int64(134217729), peak.PeakBytes)
	require.Equal(t, 2, samples)
	require.False(t, summary)
	_, _, summary, err = ownershipCaseWork(logs + `{"msg":"copy summary","submitted":2000,"confirmed":2000,"peak_uncommitted_records":1025,"peak_uncommitted_bytes":134217729}` + "\n")
	require.NoError(t, err)
	require.True(t, summary)
	// When a later complete row loses required cap evidence, then earlier
	// valid observations cannot conceal the missing or malformed measurement.
	for _, row := range []string{`{"msg":"progress","submitted":2,"confirmed":1}`, `{"msg":"progress","peak_uncommitted_records":-1}`, `{"msg":"progress"`} {
		_, _, _, err := ownershipCaseWork(logs + row + "\n")
		require.Error(t, err, row)
	}
}
