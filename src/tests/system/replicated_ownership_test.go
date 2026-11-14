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
	"github.com/twmb/franz-go/pkg/kgo"
)

type ownershipMember struct {
	ClientID   string  `json:"client_id"`
	MemberID   string  `json:"member_id"`
	Partitions []int32 `json:"partitions"`
}

type ownershipSnapshot struct {
	Started  time.Time         `json:"started"`
	Finished time.Time         `json:"finished"`
	Members  []ownershipMember `json:"members"`
}

func observeOwnership(ctx context.Context, admin *kadm.Client, group, source string) (ownershipSnapshot, error) {
	result := ownershipSnapshot{Started: time.Now().UTC()}
	described, err := admin.DescribeGroups(ctx, group)
	result.Finished = time.Now().UTC()
	if err != nil {
		return result, err
	}
	detail, ok := described[group]
	if !ok || detail.Err != nil || detail.State != "Stable" {
		return result, fmt.Errorf("group is not stable: %+v", detail)
	}
	for _, member := range detail.Members {
		assignment, ok := member.Assigned.AsConsumer()
		if !ok {
			return result, fmt.Errorf("member lacks a consumer assignment")
		}
		owned := ownershipMember{ClientID: member.ClientID, MemberID: member.MemberID}
		for _, topic := range assignment.Topics {
			if topic.Topic != source {
				return result, fmt.Errorf("member owns an unexpected topic")
			}
			owned.Partitions = append(owned.Partitions, topic.Partitions...)
		}
		slices.Sort(owned.Partitions)
		result.Members = append(result.Members, owned)
	}
	return result, nil
}

func waitReplicated(ctx context.Context, t *testing.T, label string, publisher *replicatedPublisher, workers []*cliProcess, condition func(context.Context) bool) {
	t.Helper()
	deadline, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		for _, worker := range workers {
			if worker.finished() {
				t.Fatalf("selected CLI exited while waiting for %s: %v %s", label, worker.wait(), worker.logs())
			}
		}
		if publisher != nil {
			require.NoError(t, publisher.snapshot().err, "steady publisher failed")
		}
		query, done := context.WithTimeout(deadline, 2*time.Second)
		ready := condition(query)
		done()
		if ready {
			return
		}
		select {
		case <-deadline.Done():
			t.Fatal("observed-state deadline expired: ", label)
		case <-tick.C:
		}
	}
}

func replicatedTopic(ctx context.Context, t *testing.T, admin *kadm.Client, name string) {
	t.Helper()
	created, err := admin.CreateTopic(ctx, replicatedPartitions, 3,
		map[string]*string{"min.insync.replicas": kadm.StringPtr("2"), "retention.ms": kadm.StringPtr("-1")}, name)
	require.NoError(t, err)
	require.NoError(t, created.Err)
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 15*time.Second)
		defer done()
		deleted, err := admin.DeleteTopics(cleanup, name)
		if err != nil {
			t.Error("delete replicated fixture topic:", err)
		} else if err := deleted.Error(); err != nil {
			t.Error("delete replicated fixture topic:", err)
		}
	})
	waitTopicReady(t, admin, name, replicatedPartitions)
	waitReplicated(ctx, t, "all replicated topic replicas in sync", nil, nil, func(query context.Context) bool {
		metadata, err := admin.Metadata(query, name)
		if err != nil || len(metadata.Topics[name].Partitions) != replicatedPartitions {
			return false
		}
		for _, partition := range metadata.Topics[name].Partitions {
			if partition.Err != nil || len(partition.ISR) != 3 {
				return false
			}
		}
		return true
	})
}

func TestReplicatedZombieWorker(t *testing.T) {
	// Given two selected CLI workers, independent observers and steady input on
	// 24 replicated partitions. The case requires its owned dedicated fixture.
	require.NotEmpty(t, os.Getenv("KAFERRY_FIXTURE_PROJECT"))
	require.NotEmpty(t, os.Getenv("KAFERRY_TOXIPROXY_API"))
	require.NotEmpty(t, os.Getenv("KAFERRY_TEST_BINARY"), "select the exact CLI executable")
	require.NotEmpty(t, os.Getenv("KAFERRY_EXPECT_BINARY_SHA256"), "bind both workers to the selected executable digest")
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	directory := os.Getenv("KAFERRY_REPORT_DIR")
	if directory == "" {
		directory = t.TempDir()
	}
	directory, err := filepath.Abs(directory)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(directory, 0700))
	metadata, err := faults.NewMetadata()
	require.NoError(t, err)
	t.Cleanup(metadata.Close)
	lab := &faults.Lab{Project: os.Getenv("KAFERRY_FIXTURE_PROJECT"), API: os.Getenv("KAFERRY_TOXIPROXY_API"), Metadata: metadata}
	source, destination := metadata["source"].Client, metadata["destination"].Client
	prefix := fmt.Sprintf("kaferry-zombie-%d", time.Now().UnixNano())
	src, dst, group := prefix+"-source", prefix+"-destination", prefix+"-group"
	replicatedTopic(ctx, t, source, src)
	replicatedTopic(ctx, t, destination, dst)
	timeouts := faultTimeouts(t, "ownership/zombie-worker")
	fault, err := zombieCoordinatorFault(timeouts)
	require.NoError(t, err)
	oracle := startFaultOracle(t, "ownership/zombie-worker", strings.Join(faults.Endpoints("source", 0), ","),
		strings.Join(faults.Endpoints("destination", 0), ","), src, dst, group, replicatedPartitions, false)
	audit := startReplicatedAudit(ctx, t, lab, directory)
	audit.capture(ctx, t, "baseline")
	producer := fixtureClient(t, strings.Join(faults.Endpoints("source", 0), ","), kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.MaxBufferedRecords(2*replicatedPartitions), kgo.RecordDeliveryTimeout(timeouts["delivery_timeout"]),
		kgo.AllowIdempotentProduceCancellation())
	config := configuration(t, zombieWorkerSettings(timeouts))
	binary := testBinary(t)
	markers := []string{prefix + "-worker1", prefix + "-worker2"}
	clients := make([]string, 2)
	workers := make([]*cliProcess, 0, 2)
	memories := make([]func() rssPeak, 0, 2)
	starts := make([]time.Time, 0, 2)
	exits := make([]<-chan time.Time, 0, 2)
	for index, marker := range markers {
		worker := index + 1
		statement := zombieWorkerSQL(timeouts, worker, marker, src, dst, group)
		starts = append(starts, time.Now().UTC())
		process := launchCLI(t, ctx, binary, "run", "--file", "-", "--config", config)
		workers = append(workers, process)
		oracle.writer(process)
		memories = append(memories, watchReplicatedRSS(t, process))
		exits = append(exits, observeReplicatedExit(process))
		require.NoError(t, process.send(statement))
		waitMembers(t, source, group, worker, replicatedPartitions, nil)
		waitReplicated(ctx, t, "broker-observed worker client identity", nil, workers, func(query context.Context) bool {
			observed, queryErr := observeOwnership(query, source, group, src)
			if queryErr != nil {
				return false
			}
			for _, member := range observed.Members {
				if strings.HasSuffix(member.ClientID, marker) {
					clients[index] = member.ClientID
					return true
				}
			}
			return false
		})
	}
	// Start input after the initial join has settled. Startup ownership changes
	// must not create replay before this case's one declared fault window.
	publisher := startReplicatedPublisher(ctx, producer, src)
	t.Cleanup(func() { publisher.stop() })
	state := &zombieOwnership{admin: source, audit: audit, topic: src, group: group, clients: clients}
	waitReplicated(ctx, t, "both workers own and save every source partition", publisher, workers, state.baselineReady)
	waitReplicated(ctx, t, "accepted worker1 saves in its independently observed latest join generation", publisher, workers,
		func(query context.Context) bool { return state.generationReady(query, t) })
	before := publisher.snapshot()
	role := faults.Role{Kind: "worker-coordinator", Cluster: "source", Group: group, Worker: 1}
	window := oracle.begin(t, "worker-coordinator-blackhole", role, false,
		timeouts,
		"independent broker group describes two workers; every partition has saved nonempty progress")

	// When worker1's coordinator upstream drops packets, keep the live pending
	// owner exposed until the broker expires it and the peer actually saves.
	injection, err := lab.Inject(ctx, role, fault)
	if injection != nil {
		audit.faultAttempted = true
		retainZombieFault(t, injection, directory, oracle.recovery)
	}
	require.NoError(t, err)
	require.Len(t, injection.Event.Targets, 1)
	oracle.injected(t, window, "timeout", injection.Event, "owned worker1 coordinator upstream drops packets without immediate TCP close")
	waitReplicated(ctx, t, "expired worker1 member and complete peer ownership", publisher, workers, state.transferReady)
	aliveTransfer := []bool{!workers[0].finished(), !workers[1].finished()}
	waitReplicated(ctx, t, "accepted peer progress while worker1 coordinator route remains cut", publisher, workers,
		func(query context.Context) bool { return state.peerReady(query, t) })
	aliveBeforeHeal := []bool{!workers[0].finished(), !workers[1].finished()}
	require.Equal(t, []bool{true, true}, aliveTransfer, "both workers must live through independently observed expiry")
	require.Equal(t, []bool{true, true}, aliveBeforeHeal, "both workers must live through accepted peer progress before heal")
	healStarted := time.Now().UTC()
	require.NoError(t, injection.Heal(ctx))
	oracle.healed(t, window, "original resolved coordinator route restored after independently observed expiry and peer assignment")
	atHeal := publisher.snapshot()
	// A rejected late periodic save is intentionally fatal to worker1. Its
	// exit is accepted only by the final independent positive fencing audit.
	waitReplicated(ctx, t, "post-heal steady input and progress on every partition", publisher, workers[1:],
		func(query context.Context) bool { return state.healedReady(query, publisher, atHeal) })
	aliveHeal := []bool{!workers[0].finished(), !workers[1].finished()}
	publication := publisher.stop()
	require.NoError(t, publication.err)
	require.LessOrEqual(t, publication.MaxGapSeconds, 2.0, "source did not keep publishing steadily through the ownership fault")
	waitReplicated(ctx, t, "all acknowledged input saved before shutdown", nil, workers[1:],
		func(query context.Context) bool { return state.allSaved(query, publication) })
	measurements := stopZombieWorkers(t, directory, workers, memories, starts, exits)

	// Then independent safety samples and final librdkafka reconciliation cover
	// the full configured recovery window. The broker audit separately proves
	// that stale ownership never committed, distinguishing legitimate rejoins.
	oracle.finish(t)
	audit.mark(ctx, t, "final")
	audit.capture(ctx, t, "final")
	safetyPath := filepath.Join(filepath.Dir(oracle.log), "observations", "case.json")
	safety, err := os.ReadFile(safetyPath)
	require.NoError(t, err)
	safetyRelative, err := filepath.Rel(directory, safetyPath)
	require.NoError(t, err)
	data, err := json.MarshalIndent(map[string]any{"id": "ownership/zombie-worker", "source_partitions": replicatedPartitions,
		"source_topic": src, "destination_topic": dst, "group": group, "workers": clients, "audit_guard": audit.guard,
		"audit_marks":                    audit.marks,
		"worker_connection_milliseconds": zombieWorkerConnection(timeouts),
		"baseline":                       state.baseline, "baseline_generation": state.generation, "baseline_saved": state.saved, "transfer": state.transfer, "peer_progress": state.peerProgress, "peer_saved": state.peerSaved,
		"publishing_before_fault": before, "publishing_after_heal": atHeal, "healed": state.healed, "fault": injection.Event,
		"heal_started":                   healStarted,
		"processes_alive_after_transfer": aliveTransfer, "processes_alive_before_heal": aliveBeforeHeal, "processes_alive_after_heal": aliveHeal,
		"safety_case": map[string]string{"observations": safetyRelative, "sha256": fmt.Sprintf("%x", sha256.Sum256(safety))},
		"publishing":  publication, "worker_measurements": measurements}, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(directory, "replicated-ownership.json"), append(data, '\n'), 0600))
	verifyOwnershipAudit(ctx, t, directory)
}

// zombieOwnership keeps the independently queried transition observations
// together; each polling callback preserves the same acceptance conditions.
type zombieOwnership struct {
	admin                                    *kadm.Client
	audit                                    *replicatedAudit
	topic, group                             string
	clients                                  []string
	baseline, transfer, peerProgress, healed ownershipSnapshot
	saved, peerSaved                         []int64
	generation                               int32
}

func (s *zombieOwnership) offsets(ctx context.Context) ([]int64, bool) {
	parts, err := s.admin.FetchOffsetsForTopics(ctx, s.group, s.topic)
	if err != nil || parts.Error() != nil || len(parts[s.topic]) != replicatedPartitions {
		return nil, false
	}
	result := make([]int64, replicatedPartitions)
	for id, part := range parts[s.topic] {
		result[id] = part.At
	}
	return result, true
}

func (s *zombieOwnership) baselineReady(ctx context.Context) bool {
	var err error
	s.baseline, err = observeOwnership(ctx, s.admin, s.group, s.topic)
	if err != nil || len(s.baseline.Members) != 2 {
		return false
	}
	var ready bool
	s.saved, ready = s.offsets(ctx)
	if !ready {
		return false
	}
	for _, offset := range s.saved {
		if offset < 1 {
			return false
		}
	}
	return true
}

func (s *zombieOwnership) generationReady(ctx context.Context, t *testing.T) bool {
	t.Helper()
	for _, member := range s.baseline.Members {
		if member.ClientID == s.clients[0] {
			var ready bool
			var err error
			s.generation, ready, err = s.audit.ready(ctx, s.clients[0], s.group, member.MemberID, member.Partitions, nil, time.Time{})
			require.NoError(t, err)
			return ready
		}
	}
	return false
}

func (s *zombieOwnership) transferReady(ctx context.Context) bool {
	var err error
	s.transfer, err = observeOwnership(ctx, s.admin, s.group, s.topic)
	return err == nil && len(s.transfer.Members) == 1 && s.transfer.Members[0].ClientID == s.clients[1] &&
		len(s.transfer.Members[0].Partitions) == replicatedPartitions
}

func (s *zombieOwnership) peerReady(ctx context.Context, t *testing.T) bool {
	t.Helper()
	var err error
	s.peerProgress, err = observeOwnership(ctx, s.admin, s.group, s.topic)
	if err != nil || len(s.peerProgress.Members) != 1 || s.peerProgress.Members[0].ClientID != s.clients[1] ||
		len(s.peerProgress.Members[0].Partitions) != replicatedPartitions {
		return false
	}
	var ready bool
	s.peerSaved, ready = s.offsets(ctx)
	if !ready {
		return false
	}
	minimum := make(map[int32]int64, replicatedPartitions)
	for partition, offset := range s.saved {
		minimum[int32(partition)] = offset + 2
		if s.peerSaved[partition] <= offset+2 {
			return false
		}
	}
	peerGeneration, accepted, queryErr := s.audit.ready(ctx, s.clients[1], s.group, s.peerProgress.Members[0].MemberID,
		s.peerProgress.Members[0].Partitions, minimum, s.transfer.Finished)
	require.NoError(t, queryErr)
	return accepted && peerGeneration > s.generation
}

func (s *zombieOwnership) healedReady(ctx context.Context, publisher *replicatedPublisher, atHeal replicatedPublishing) bool {
	publication := publisher.snapshot()
	for partition, count := range publication.Acknowledged {
		if count <= atHeal.Acknowledged[partition]+24 {
			return false
		}
	}
	var err error
	s.healed, err = observeOwnership(ctx, s.admin, s.group, s.topic)
	if err != nil {
		return false
	}
	saved, ready := s.offsets(ctx)
	if !ready {
		return false
	}
	for partition, offset := range s.peerSaved {
		if saved[partition] <= offset+24 {
			return false
		}
	}
	return true
}

func (s *zombieOwnership) allSaved(ctx context.Context, publication replicatedPublishing) bool {
	saved, ready := s.offsets(ctx)
	if !ready {
		return false
	}
	for partition, count := range publication.Acknowledged {
		if saved[partition] != count {
			return false
		}
	}
	return true
}

func retainZombieFault(t *testing.T, injection *faults.Injection, directory string, recovery time.Duration) {
	t.Helper()
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 15*time.Second)
		defer done()
		if err := injection.Heal(cleanup); err != nil {
			t.Error("heal worker coordinator route:", err)
		}
		// Retain the actual resolved attempt even if a later assertion fails.
		// A required recovery horizon is not a claim that it was observed.
		event, err := json.MarshalIndent(map[string]any{"event": injection.Event, "case_failed": t.Failed(),
			"required_recovery_seconds": recovery.Seconds()}, "", "  ")
		if err == nil {
			err = os.WriteFile(filepath.Join(directory, "replicated-fault-window.json"), append(event, '\n'), 0600)
		}
		if err != nil {
			t.Error("retain resolved fault window:", err)
		}
	})
}

func stopZombieWorkers(t *testing.T, directory string, workers []*cliProcess, memories []func() rssPeak, starts []time.Time, exits []<-chan time.Time) []map[string]any {
	t.Helper()
	measurements := make([]map[string]any, len(workers))
	for index, process := range workers {
		memory := memories[index]()
		require.Positive(t, memory.samples)
		require.Zero(t, memory.failures, "RSS sampling must cover the live worker interval")
		require.LessOrEqual(t, memory.peak*1024, int64(512<<20))
		measurements[index] = map[string]any{"sampled_peak_rss_bytes": memory.peak * 1024, "rss_samples": memory.samples,
			"rss_failures": memory.failures, "record_cap": 1024, "byte_cap": 134217728,
			"started_at": starts[index], "pid": process.pid()}
		if !process.finished() {
			if err := process.signal(os.Interrupt); !errors.Is(err, os.ErrProcessDone) {
				require.NoError(t, err)
			}
		}
	}
	for index, process := range workers {
		waitErr := process.wait()
		code := process.command.ProcessState.ExitCode()
		if index == 1 || code != 1 {
			require.NoError(t, waitErr, process.logs())
		}
		measurements[index]["stopped_at"] = <-exits[index]
		measurements[index]["exit_code"] = code
		summary := summaryFrom(t, process.logs())
		require.LessOrEqual(t, summary.PeakRecords, 1024)
		require.LessOrEqual(t, summary.PeakBytes, int64(134217728))
		measurements[index]["peak_uncommitted_records"], measurements[index]["peak_uncommitted_bytes"] = summary.PeakRecords, summary.PeakBytes
		require.NoError(t, os.WriteFile(filepath.Join(directory, fmt.Sprintf("worker-%d.cli.log", index+1)), []byte(process.logs()), 0600))
	}
	return measurements
}
