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
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/src/tests/system/faults"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const ownershipKilledWorkScope = "sampled progress only; SIGKILL omits final lifetime highwater"

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
	Readiness       []brokerHealthCheck  `json:"broker_readiness,omitempty"`
}

type ownershipCaseWorker struct {
	lifetime    brokerLifetime
	route       int
	process     *cliProcess
	started     time.Time
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
	lifecycle                           *brokerCopy
	readiness                           *brokerCopy
	states                              map[string]ownershipCaseObservation
	events                              []map[string]any
	failedAfter                         time.Time
	safety                              map[string]string
	complete                            bool
	coordinatorFault                    *faults.Injection
	sourceID                            [16]byte
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
	// Reuse immutable sole-Wait capture. Ownership keeps its own raw fault
	// proof and signal rules; generic forced-copy exceptions do not apply.
	s.lifecycle = &brokerCopy{directory: directory}
	s.lab = &faults.Lab{Project: os.Getenv("KAFERRY_FIXTURE_PROJECT"), API: os.Getenv("KAFERRY_TOXIPROXY_API"), Metadata: metadata}
	s.src, s.dst, s.group = s.prefix+"-source", s.prefix+"-destination", s.prefix+"-group"
	replicatedTopic(ctx, t, metadata["source"].Client, s.src)
	replicatedTopic(ctx, t, metadata["destination"].Client, s.dst)
	source, err := metadata["source"].Metadata(ctx, s.src)
	require.NoError(t, err)
	s.sourceID = source.Topics[s.src].ID
	require.NotZero(t, s.sourceID, "the selected source topic must have an independent identity")
	if name == "coordinator-kill-rebalance" {
		s.readiness = &brokerCopy{lab: s.lab, metadata: metadata, src: s.src, dst: s.dst}
		for side, topic := range map[string]string{"source": s.src, "destination": s.dst} {
			_, topologyErr := s.readiness.topology(ctx, side, topic)
			require.NoError(t, topologyErr, "retain baseline cluster identity before the coordinator fault")
		}
		configs, configErr := metadata["source"].DescribeBrokerConfigs(ctx, 1, 2, 3)
		require.NoError(t, configErr)
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
	producer := replicatedBrokerProducer(t, strings.Join(faults.Endpoints("source", 0), ","), timeouts["delivery_timeout"])
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

func (s *ownershipCase) start(route int) {
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
	worker.lifetime = brokerLifetime{Number: len(s.workers) + 1, PID: worker.process.pid(), Started: worker.started, Binary: os.Getenv("KAFERRY_EXPECT_BINARY_SHA256")}
	worker.memory = watchReplicatedRSS(s.t, worker.process)
	s.workers = append(s.workers, worker)
	s.lifecycle.workers = append(s.lifecycle.workers, worker.process)
	s.lifecycle.identities = append(s.lifecycle.identities, worker.lifetime)
	s.oracle.writer(worker.process)
}

func (s *ownershipCase) observe(ctx context.Context) ownershipCaseObservation {
	state := ownershipCaseObservation{Started: time.Now().UTC(), Publishing: s.publisher.snapshot(), Alive: make(map[int]bool)}
	for _, worker := range s.workers {
		state.Alive[worker.process.pid()] = !worker.process.finished()
	}
	var err error
	state.Coordinator, err = s.metadata.Coordinator(ctx, "source", s.group)
	if err == nil {
		state.State, state.Members, err = s.groupMembers(ctx, state.Coordinator)
	}
	if err == nil {
		state.Saved, err = s.savedOffsets(ctx, state.Coordinator)
	}
	if err != nil {
		state.Error = err.Error()
	}
	state.Finished = time.Now().UTC()
	return state
}

func (s *ownershipCase) groupMembers(ctx context.Context, coordinator int32) (string, []ownershipMember, error) {
	request := kmsg.NewPtrDescribeGroupsRequest()
	request.Groups = []string{s.group}
	result, err := s.metadata["source"].RequestBroker(ctx, coordinator, request)
	if err != nil {
		return "", nil, err
	}
	groups := result.(*kmsg.DescribeGroupsResponse).Groups
	if len(groups) != 1 || groups[0].Group != s.group {
		return "", nil, fmt.Errorf("independent group describe omitted the selected group")
	}
	group := groups[0]
	if groupErr := kerr.ErrorForCode(group.ErrorCode); groupErr != nil {
		return "", nil, groupErr
	}
	if group.ProtocolType != "consumer" {
		return "", nil, fmt.Errorf("independent group describe returned protocol %q", group.ProtocolType)
	}
	var members []ownershipMember
	for _, member := range group.Members {
		owned := ownershipMember{ClientID: member.ClientID, MemberID: member.MemberID}
		owned.Partitions, err = ownershipAssignedPartitions(member.MemberAssignment, s.src)
		if err != nil {
			return "", nil, err
		}
		members = append(members, owned)
	}
	return group.State, members, nil
}

func ownershipAssignedPartitions(encoded []byte, source string) ([]int32, error) {
	if len(encoded) == 0 {
		return nil, nil
	}
	var assignment kmsg.ConsumerMemberAssignment
	if err := assignment.ReadFrom(encoded); err != nil {
		return nil, fmt.Errorf("decode independent group assignment: %w", err)
	}
	var partitions []int32
	for _, topic := range assignment.Topics {
		if topic.Topic != source {
			return nil, fmt.Errorf("group member owns unexpected topic %q", topic.Topic)
		}
		partitions = append(partitions, topic.Partitions...)
	}
	slices.Sort(partitions)
	return partitions, nil
}

func (s *ownershipCase) savedOffsets(ctx context.Context, coordinator int32) ([]int64, error) {
	request := kmsg.NewPtrOffsetFetchRequest()
	request.Group = s.group
	group := kmsg.NewOffsetFetchRequestGroup()
	group.Group = s.group
	request.Groups = []kmsg.OffsetFetchRequestGroup{group}
	result, err := s.metadata["source"].RequestBroker(ctx, coordinator, request)
	if err != nil {
		return nil, err
	}
	response := result.(*kmsg.OffsetFetchResponse)
	if err := kerr.ErrorForCode(response.ErrorCode); err != nil {
		return nil, err
	}
	if len(response.Groups) != 1 || response.Groups[0].Group != s.group {
		return nil, fmt.Errorf("independent offset fetch omitted the selected group")
	}
	observed := response.Groups[0]
	if err := kerr.ErrorForCode(observed.ErrorCode); err != nil {
		return nil, err
	}
	if len(observed.Topics) != 1 {
		return nil, nil
	}
	topic := observed.Topics[0]
	selected := topic.Topic == s.src
	if response.Version >= 10 {
		selected = topic.TopicID == s.sourceID
	}
	if !selected {
		return nil, fmt.Errorf("independent offset fetch returned an unexpected topic")
	}
	if len(topic.Partitions) != replicatedPartitions {
		return nil, nil
	}
	saved := make([]int64, replicatedPartitions)
	seen := make(map[int32]bool)
	for _, part := range topic.Partitions {
		if !ownershipDistinctParts([]int32{part.Partition}, seen) {
			return nil, fmt.Errorf("independent offset fetch returned invalid or repeated partition %d", part.Partition)
		}
		if err := kerr.ErrorForCode(part.ErrorCode); err != nil {
			return nil, err
		}
		saved[part.Partition] = part.Offset
	}
	return saved, nil
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
		ready := state.Error == "" && condition(query, state)
		if err := query.Err(); err != nil {
			state.Error = err.Error()
			ready = false
		}
		state.Finished = time.Now().UTC()
		s.journal(state)
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
		matched := ownershipMemberRoute(member.ClientID, markers)
		if matched == 0 || member.MemberID == "" || len(member.Partitions) == 0 {
			return nil, false
		}
		if _, exists := members[matched]; exists {
			return nil, false
		}
		members[matched] = member
		if !ownershipDistinctParts(member.Partitions, partitions) {
			return nil, false
		}
	}
	return members, len(partitions) == replicatedPartitions
}

func ownershipMemberRoute(client string, markers map[int]string) int {
	matched := 0
	for route, marker := range markers {
		if strings.HasSuffix(client, marker) {
			if matched != 0 {
				return 0
			}
			matched = route
		}
	}
	return matched
}

func ownershipDistinctParts(parts []int32, seen map[int32]bool) bool {
	for _, partition := range parts {
		if partition < 0 || partition >= replicatedPartitions || seen[partition] {
			return false
		}
		seen[partition] = true
	}
	return true
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
		state.Generations = s.acceptedGenerations(query, members, before, generation)
		if state.Generations == nil {
			return false
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

func (s *ownershipCase) acceptedGenerations(ctx context.Context, members map[int]ownershipMember, before []int64, generation int32) map[string]int32 {
	minimum := make(map[int32]int64)
	for partition, offset := range ownershipCaseMinimum(before) {
		minimum[int32(partition)] = offset + 2
	}
	run := s.lab.Docker
	if run == nil {
		run = faults.Docker
	}
	all, err := readAuditRequests(ctx, s.audit.targets, run)
	if ctx.Err() != nil {
		return nil
	}
	require.NoError(s.t, err, "actual accepted generation audit unavailable")
	rows, err := decodeOwnershipAudit(ctx, all)
	if ctx.Err() != nil {
		return nil
	}
	require.NoError(s.t, err, "actual accepted generation audit unavailable")
	generations := make(map[string]int32)
	var current int32
	for _, member := range members {
		observed, ready, err := currentGenerationSavedRows(rows, member.ClientID, s.group, member.MemberID, member.Partitions, minimum, time.Time{})
		require.NoError(s.t, err, "actual accepted generation audit unavailable")
		if ctx.Err() != nil {
			return nil
		}
		if !ready || observed <= generation || current != 0 && current != observed {
			return nil
		}
		current = observed
		generations[member.ClientID] = observed
	}
	return generations
}

func ownershipGenerationMembers() map[int]ownershipMember {
	members := make(map[int]ownershipMember)
	for route := 1; route <= 3; route++ {
		member := ownershipMember{ClientID: fmt.Sprintf("worker%d", route), MemberID: fmt.Sprintf("member%d", route)}
		for partition := (route - 1) * 8; partition < route*8; partition++ {
			member.Partitions = append(member.Partitions, int32(partition))
		}
		members[route] = member
	}
	return members
}

func ownershipGenerationRows(members map[int]ownershipMember, missing int, generation int32) string {
	var proof strings.Builder
	rows := readinessRows()
	for route, member := range members {
		rewrite := strings.NewReplacer(`"worker"`, fmt.Sprintf(`"%s"`, member.ClientID), `"member"`, fmt.Sprintf(`"%s"`, member.MemberID),
			`"generationId":2`, fmt.Sprintf(`"generationId":%d`, generation), `"generationIdOrMemberEpoch":2`, fmt.Sprintf(`"generationIdOrMemberEpoch":%d`, generation))
		proof.WriteString(rewrite.Replace(string(rows[0])))
		for index, partition := range member.Partitions {
			if route != missing || index != 0 {
				proof.WriteString(strings.ReplaceAll(rewrite.Replace(string(rows[1])), `"partitionIndex":0`, fmt.Sprintf(`"partitionIndex":%d`, partition)))
			}
		}
	}
	return proof.String()
}

func ownershipGenerationReader(query context.Context, proof string) (func(context.Context, ...string) ([]byte, error), map[string]int) {
	var lock sync.Mutex
	calls := make(map[string]int)
	return func(ctx context.Context, args ...string) ([]byte, error) {
		if ctx != query || len(args) != 5 || args[0] != "exec" {
			return nil, fmt.Errorf("audit did not share its existing query context")
		}
		lock.Lock()
		calls[args[1]]++
		repeated := calls[args[1]] != 1
		lock.Unlock()
		if repeated {
			return nil, fmt.Errorf("broker audit reread for another member")
		}
		if args[1] == "three" {
			return []byte(proof), nil
		}
		return []byte("KAFERRY_AUDIT_FILE kafka-request.log\n"), nil
	}, calls
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

func (s *ownershipCase) coordinatorCommand(ctx context.Context, args ...string) ([]byte, error) {
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

func (s *ownershipCase) killCoordinator(window int, old *ownershipCaseWorker) {
	// The native injector resolves the live coordinator. Intercept only its
	// final kill command to prove the same coordinator and rebalance still hold.
	s.lab.Docker = s.coordinatorCommand
	injection, err := s.lab.Inject(s.ctx, faults.Role{Kind: "coordinator", Cluster: "source", Group: s.group}, faults.Fault{Kind: "kill"})
	if injection != nil {
		s.coordinatorFault = injection
		s.events = append(s.events, map[string]any{"fault": &injection.Event})
		s.t.Cleanup(func() {
			cleanup, done := context.WithTimeout(context.Background(), 30*time.Second)
			defer done()
			if healErr := injection.Heal(cleanup); healErr != nil {
				s.t.Error("restore original resolved coordinator: ", healErr)
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
	probe := s.readiness
	apiReady := s.poll("restored broker API readiness before scoped logging", true, func(query context.Context, _ ownershipCaseObservation) bool {
		probe.healthChecks = nil
		return probe.healthy(query, killed) && brokerReadinessISR(probe.healthChecks, "source", -1, 3, -1)
	})
	apiReady.Readiness = slices.Clone(probe.healthChecks)
	s.states["broker_api_ready"] = apiReady
	s.journal(apiReady)
	// Reapply scoped logging before the metadata used for recovery proof, as
	// well as the accepted post-recovery generations and final capture guards.
	require.NoError(s.t, s.audit.level(s.ctx, killed.Broker, requestLogger, "DEBUG"))
	require.NoError(s.t, s.audit.level(s.ctx, killed.Broker, receivedLogger, "TRACE"))
	s.events = append(s.events, map[string]any{"broker": killed.Broker, "audit_logging_reenabled_at": time.Now().UTC()})
	var restoredTopology []faults.Partition
	s.states["broker_restored"] = s.poll("restored coordinator broker health and all 24 ISR sets", true,
		func(query context.Context, state ownershipCaseObservation) bool {
			probe.healthChecks = nil
			parts, topologyErr := probe.topology(query, "source", s.src)
			if topologyErr != nil {
				state.Error, state.Finished = topologyErr.Error(), time.Now().UTC()
				s.journal(state)
				return false
			}
			observed := brokerObservation{Topology: map[string][]faults.Partition{"source": parts}}
			if !probe.healthy(query, killed) || !fullyReplicated(observed, "source") || !brokerReadinessISR(probe.healthChecks, "source", -1, 3, -1) {
				return false
			}
			restoredTopology = parts
			return s.brokerInspection(query, killed, true, "coordinator-restored.inspect.json")
		})
	restored := s.states["broker_restored"]
	restored.SourceTopology = restoredTopology
	restored.Readiness = slices.Clone(probe.healthChecks)
	s.states["broker_restored"] = restored
	s.journal(restored)
	s.validateCoordinatorExits()
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
		observed.Running != running || running && !slices.Contains([]string{"starting", "healthy", "unhealthy"}, observed.Health) {
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
	require.NoError(s.t, worker.process.signal(signal, reason))
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
	_, retainErr := s.lifecycle.retainExit(worker.process)
	require.NoError(s.t, retainErr, "retain ownership sole-Wait evidence before exit assertions")
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
		exit, retainErr := s.lifecycle.retainExit(worker.process)
		require.NoError(s.t, retainErr, "retain ownership sole-Wait evidence before classification")
		require.True(s.t, allowFaultExit && !s.failedAfter.IsZero(), "unexplained or premature CLI exit: %s", worker.process.logs())
		require.Equal(s.t, 1, worker.process.command.ProcessState.ExitCode(), worker.process.logs())
		terminal, known := s.coordinatorFailure(worker, exit.Witness, worker.process.logs())
		require.True(s.t, known, "coordinator fault produced an unexplained terminal failure: %s", worker.process.logs())
		worker.reason, worker.terminal = "coordinator fault fail-fast exit", terminal
		s.measure(worker, false)
		require.False(s.t, worker.measurement["stopped_at"].(time.Time).Before(s.failedAfter), "exit preceded actual coordinator kill")
	}
	for _, worker := range s.workers {
		if worker.reason == "" {
			logs, err := worker.process.currentLogs()
			require.NoError(s.t, err)
			work, ready, err := ownershipCaseWork(logs)
			require.NoError(s.t, err)
			if ready {
				require.LessOrEqual(s.t, work.SampledPending, 1024)
				require.LessOrEqual(s.t, work.SampledPendingBytes, int64(134217728))
			}
		}
	}
}

func ownershipCaseWork(logs string) (brokerWorkEvidence, bool, error) {
	complete := logs[:strings.LastIndexByte(logs, '\n')+1]
	_, ready, err := latestCopyProgress(complete)
	if err != nil || !ready {
		return brokerWorkEvidence{}, false, err
	}
	work, err := brokerWork(complete)
	return work, true, err
}

func ownershipWorkMeasurement(work brokerWorkEvidence, killed bool) (map[string]any, error) {
	if work.ProgressSamples == 0 || killed && work.Summary != nil || !killed && work.Summary == nil {
		return nil, fmt.Errorf("ownership cap evidence requires live samples and a final summary unless SIGKILL prevented it")
	}
	evidence := map[string]any{"lifetime_highwater_available": work.Summary != nil, "copy_summary": work.Summary,
		"peak_uncommitted_records": nil, "peak_uncommitted_bytes": nil, "progress_observations": work.ProgressSamples,
		"sampled_max_pending_entries": work.SampledPending, "sampled_max_pending_estimated_bytes": work.SampledPendingBytes,
		"cap_measurement_scope": ownershipKilledWorkScope}
	if work.Summary != nil {
		evidence["peak_uncommitted_records"], evidence["peak_uncommitted_bytes"] = work.Summary.PeakRecords, work.Summary.PeakBytes
		evidence["cap_measurement_scope"] = brokerExactWorkScope
	}
	return evidence, nil
}

func (s *ownershipCase) measure(worker *ownershipCaseWorker, killed bool) {
	exit, retainErr := s.lifecycle.retainExit(worker.process)
	require.NoError(s.t, retainErr)
	logs, err := worker.process.currentLogs()
	require.NoError(s.t, err)
	memory := worker.memory()
	require.Positive(s.t, memory.samples)
	require.Zero(s.t, memory.failures)
	require.LessOrEqual(s.t, memory.peak*1024, int64(512<<20))
	work, ready, err := ownershipCaseWork(logs)
	require.NoError(s.t, err)
	require.True(s.t, ready, "every selected worker needs live cap measurements")
	evidence, err := ownershipWorkMeasurement(work, killed)
	require.NoError(s.t, err)
	worker.measurement = map[string]any{"lifetime": worker.lifetime.Number, "lifecycle": exit.Reference,
		"pid": worker.process.pid(), "route": worker.route, "started_at": worker.started, "stopped_at": worker.process.stopped,
		"wait_status":   int(worker.process.command.ProcessState.Sys().(syscall.WaitStatus)),
		"binary_sha256": os.Getenv("KAFERRY_EXPECT_BINARY_SHA256"), "exit_code": worker.process.command.ProcessState.ExitCode(), "reason": worker.reason, "terminal_error": worker.terminal,
		"sampled_peak_rss_bytes": memory.peak * 1024, "rss_samples": memory.samples, "rss_failures": memory.failures,
		"record_cap": 1024, "byte_cap": 134217728, "forced_kill": killed}
	for key, value := range evidence {
		worker.measurement[key] = value
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
	measurements, exactCaps := s.retainWorkers()
	var publishing replicatedPublishing
	if s.publisher != nil {
		publishing = s.publisher.snapshot()
	}
	artifacts := s.artifactChecksums()
	receipt := map[string]any{"schema": 1, "id": s.id, "complete": s.complete && !s.t.Failed(), "case_failed": s.t.Failed(),
		"source_partitions": replicatedPartitions, "source_topic": s.src, "destination_topic": s.dst, "group": s.group,
		"binary_sha256": os.Getenv("KAFERRY_EXPECT_BINARY_SHA256"), "states": s.states, "events": s.events, "publishing": publishing,
		"timeout_milliseconds": durationMilliseconds(s.timeouts), "required_recovery_seconds": s.oracle.recovery.Seconds(),
		"worker_measurements": measurements, "safety_case": s.safety, "audit_guard": s.audit.guard, "audit_marks": s.audit.marks,
		"exact_lifetime_work_caps_qualified": exactCaps,
		"fixture_project":                    s.lab.Project, "artifacts": artifacts, "observations": "ownership-observations.jsonl"}
	if s.readiness != nil {
		receipt["cluster_ids"], receipt["readiness_criterion"] = s.readiness.clusters, brokerReadinessCriterion
	}
	data, err := json.MarshalIndent(receipt, "", "  ")
	if err == nil {
		err = os.WriteFile(filepath.Join(s.directory, "replicated-ownership-case.json"), append(data, '\n'), 0600)
	}
	if err != nil {
		s.t.Error("retain ownership case: ", err)
	}
}

func (s *ownershipCase) retainWorkers() ([]map[string]any, bool) {
	measurements := make([]map[string]any, 0, len(s.workers))
	exactCaps := true
	for index, worker := range s.workers {
		if !worker.process.finished() {
			_ = worker.process.signal(os.Kill, "ownership test cleanup")
		}
		_ = worker.process.wait()
		if _, retainErr := s.lifecycle.retainExit(worker.process); retainErr != nil {
			s.t.Error("retain ownership lifecycle: ", retainErr)
		}
		if err := os.WriteFile(filepath.Join(s.directory, fmt.Sprintf("worker-%d-route-%d.cli.log", index+1, worker.route)), []byte(worker.process.logs()), 0600); err != nil {
			s.t.Error("retain worker log: ", err)
		}
		if worker.measurement == nil {
			worker.measurement = map[string]any{"pid": worker.process.pid(), "route": worker.route, "started_at": worker.started,
				"exit_code": worker.process.command.ProcessState.ExitCode(), "measurement_incomplete": true}
		}
		measurements = append(measurements, worker.measurement)
		exactCaps = exactCaps && worker.measurement["lifetime_highwater_available"] == true
		worker.measurement["log"] = fmt.Sprintf("worker-%d-route-%d.cli.log", index+1, worker.route)
	}
	return measurements, exactCaps
}

func (s *ownershipCase) artifactChecksums() []map[string]string {
	var artifacts []map[string]string
	entries, err := os.ReadDir(s.directory)
	if err != nil {
		s.t.Error("list ownership evidence: ", err)
		return nil
	}
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
	return artifacts
}
