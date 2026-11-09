//go:build integration && fault && load

package compat

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
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
	"github.com/twmb/franz-go/pkg/kmsg"
)

const ownershipEstablishedEOF = "established-source-operation"
const ownershipJoiningEOF = "pending-group-join"

type ownershipCoordinatorBrokerAudit struct {
	Target faults.Target `json:"target"`
	Raw    brokerFile    `json:"raw"`
}

type ownershipCoordinatorAudit struct {
	Started  time.Time                         `json:"started"`
	Finished time.Time                         `json:"finished"`
	Deadline time.Time                         `json:"deadline"`
	Brokers  []ownershipCoordinatorBrokerAudit `json:"brokers"`
}

func (s *ownershipCase) captureCoordinatorAudit() {
	query, cancel := context.WithTimeout(s.ctx, 2*time.Second)
	defer cancel()
	cut := &ownershipCoordinatorAudit{Started: time.Now().UTC(), Brokers: make([]ownershipCoordinatorBrokerAudit, len(s.audit.targets))}
	cut.Deadline, _ = query.Deadline()
	for index, target := range s.audit.targets {
		cut.Brokers[index] = ownershipCoordinatorBrokerAudit{Target: target, Raw: brokerFile{Path: fmt.Sprintf("source-%d.coordinator-pre-kill.requests.log", target.Broker)}}
	}
	_, err := readAuditRequests(query, s.audit.targets, func(ctx context.Context, args ...string) ([]byte, error) {
		data, readErr := faults.Docker(ctx, args...)
		if readErr != nil || ctx.Err() != nil || len(data) == 0 || len(data) > auditCaptureLimit {
			return data, readErr
		}
		for index := range cut.Brokers {
			entry := &cut.Brokers[index]
			if entry.Target.Container == args[1] {
				entry.Raw.SHA256 = brokerDigest(string(data))
				return data, writeBrokerImmutable(filepath.Join(s.directory, entry.Raw.Path), data)
			}
		}
		return nil, fmt.Errorf("coordinator audit target differs from owned source")
	})
	require.NoError(s.t, err, "retain pre-kill coordinator protocol proof")
	cut.Finished = time.Now().UTC()
	require.NoError(s.t, context.Cause(query), "pre-kill protocol proof exceeded its shared query")
	require.False(s.t, cut.Finished.After(cut.Deadline))
	s.coordinatorAudit = cut
}

func ownershipEOFTerminal(logs string, summary *copySummary) (string, string) {
	if summary == nil || summary.Submitted != summary.Confirmed {
		return "", ""
	}
	var terminal []string
	for _, line := range strings.Split(strings.TrimSuffix(logs, "\n"), "\n") {
		if !json.Valid([]byte(line)) {
			terminal = append(terminal, line)
		}
	}
	text := strings.Join(terminal, "\n")
	if summary.Submitted > 0 && brokerSourceOperationEOF(text) {
		return text, ownershipEstablishedEOF
	}
	join := "poll source: unable to join group session: " + brokerNegotiationEOF
	closeInput := "close Kafka input: " + brokerNegotiationEOF
	switch text {
	case join, join + "\n" + closeInput:
		if summary.Submitted == 0 {
			return text, ownershipJoiningEOF
		}
	}
	return "", ""
}

func ownershipPhaseMember(state ownershipCaseObservation, route int) (ownershipMember, bool) {
	for _, member := range state.Members {
		if strings.HasSuffix(member.ClientID, fmt.Sprintf("-worker%d", route)) {
			return member, member.MemberID != ""
		}
	}
	return ownershipMember{}, false
}

func (s *ownershipCase) coordinatorFailure(worker *ownershipCaseWorker, exit *brokerExitWitness, logs string) (string, string, bool) {
	terminal, known := ownershipCoordinatorFailure(logs)
	if known {
		return terminal, "", true
	}
	work, ready, err := ownershipCaseWork(logs)
	if err != nil || !ready {
		return "", "", false
	}
	terminal, phase := ownershipEOFTerminal(logs, work.Summary)
	if phase == "" || !s.coordinatorExitBound(worker, exit, phase) {
		return "", "", false
	}
	member, ok := ownershipPhaseMember(s.states["at_kill"], worker.route)
	if !ok {
		return "", "", false
	}
	data, err := s.coordinatorPrefix()
	if err != nil {
		return "", "", false
	}
	rows, err := ownershipPhaseRows(data, member.ClientID, worker.started, s.coordinatorAudit.Finished)
	if err != nil {
		return "", "", false
	}
	if phase == ownershipEstablishedEOF {
		ok = work.Summary.Submitted > 0 && s.establishedCoordinatorProof(worker, member, data, rows)
	} else {
		ok = work.Summary.Submitted == 0 && work.Summary.PeakRecords == 0 && work.Summary.PeakBytes == 0 &&
			ownershipZeroWork(logs) && ownershipJoiningProof(rows, member, s.group, s.timeouts)
	}
	return terminal, phase, ok
}

func (s *ownershipCase) coordinatorExitBound(worker *ownershipCaseWorker, exit *brokerExitWitness, phase string) bool {
	fault, cut := s.coordinatorFault, s.coordinatorAudit
	if s.id != "ownership/coordinator-kill-rebalance" || fault == nil || cut == nil || exit == nil ||
		fault.Event.Role != (faults.Role{Kind: "coordinator", Cluster: "source", Group: s.group}) || fault.Event.Fault.Kind != "kill" ||
		!sourceContainerTargets(fault.Event.Targets) || fault.Event.Targets[0].Broker != s.states["at_kill"].Coordinator ||
		exit.Lifetime != worker.lifetime || exit.ExitCode != 1 || exit.Signal != "" || !s.failedAfter.Before(exit.Stopped) ||
		(!fault.Event.Heal.IsZero() && !exit.Stopped.Before(fault.Event.Heal)) ||
		cut.Started.Before(s.states["preparing_rebalance"].Finished) || !cut.Started.Before(cut.Finished) || cut.Finished.After(fault.Event.Start) ||
		s.states["at_kill"].State != "PreparingRebalance" || len(s.states["at_kill"].Members) != 2 {
		return false
	}
	if s.failedAfter.IsZero() || s.coordinatorKillFinished().IsZero() || s.heldWorkerObserved().IsZero() ||
		cut.Deadline.IsZero() || cut.Finished.After(cut.Deadline) || cut.Deadline.After(cut.Started.Add(2*time.Second)) ||
		fault.Event.Start.After(s.states["at_kill"].Started) || s.states["at_kill"].Finished.After(s.failedAfter) {
		return false
	}
	if phase == ownershipJoiningEOF {
		return worker.route == 2 && len(exit.DriverIntent) == 0 && worker.started.After(s.heldWorkerObserved())
	}
	return worker.route == 1 && len(exit.DriverIntent) == 2 &&
		exit.DriverIntent[0].Signal == syscall.SIGSTOP.String() && exit.DriverIntent[0].Reason == "hold old member while a selected-binary peer joins" &&
		exit.DriverIntent[0].At.Before(s.states["preparing_rebalance"].Started) &&
		exit.DriverIntent[1].Signal == syscall.SIGCONT.String() && exit.DriverIntent[1].Reason == "resume held worker after coordinator kill" &&
		!exit.DriverIntent[1].At.Before(s.coordinatorKillFinished()) && s.coordinatorOwnedIntents(worker, exit)
}

func (s *ownershipCase) coordinatorOwnedIntents(worker *ownershipCaseWorker, exit *brokerExitWitness) bool {
	numbers := map[string]int{syscall.SIGSTOP.String(): int(syscall.SIGSTOP), syscall.SIGCONT.String(): int(syscall.SIGCONT)}
	for _, intent := range exit.DriverIntent {
		matched := 0
		for _, event := range s.events {
			start, started := event["started"].(time.Time)
			end, finished := event["finished"].(time.Time)
			if started && finished && event["pid"] == worker.lifetime.PID && event["route"] == worker.route &&
				event["signal_number"] == numbers[intent.Signal] &&
				event["signal"] == intent.Signal && event["reason"] == intent.Reason &&
				!intent.At.Before(start) && !intent.At.After(end) && !end.After(exit.Stopped) {
				matched++
			}
		}
		if matched != 1 {
			return false
		}
	}
	return true
}

func (s *ownershipCase) heldWorkerObserved() time.Time {
	for _, event := range s.events {
		if event["reason"] == "OS confirms held worker" {
			at, _ := event["observed_at"].(time.Time)
			return at
		}
	}
	return time.Time{}
}

func (s *ownershipCase) coordinatorKillFinished() time.Time {
	for _, event := range s.events {
		if at, ok := event["coordinator_kill_finished"].(time.Time); ok {
			return at
		}
	}
	return time.Time{}
}

func (s *ownershipCase) coordinatorPrefix() ([]byte, error) {
	cut := s.coordinatorAudit
	if cut == nil || len(cut.Brokers) != 3 {
		return nil, fmt.Errorf("coordinator protocol cut omitted a source broker")
	}
	seen := make(map[int32]bool)
	var selected []byte
	for _, entry := range cut.Brokers {
		target, file := entry.Target, entry.Raw
		if target.Cluster != "source" || target.Listener != "admin" || target.Broker < 1 || target.Broker > 3 || seen[target.Broker] ||
			target.Service != fmt.Sprintf("source-%d", target.Broker) || file.Path != fmt.Sprintf("source-%d.coordinator-pre-kill.requests.log", target.Broker) {
			return nil, fmt.Errorf("coordinator protocol cut target differs")
		}
		seen[target.Broker] = true
		data, err := os.ReadFile(filepath.Join(s.directory, file.Path))
		if err != nil || len(data) == 0 || len(data) > auditCaptureLimit || brokerDigest(string(data)) != file.SHA256 {
			return nil, fmt.Errorf("coordinator protocol cut is missing, changed or truncated")
		}
		if target.Broker == s.states["at_kill"].Coordinator {
			if !slices.Contains(s.coordinatorFault.Event.Targets, target) {
				return nil, fmt.Errorf("protocol cut differs from the physical coordinator target")
			}
			selected = data
		}
	}
	return selected, nil
}

func ownershipPhaseRows(data []byte, client string, birth, end time.Time) ([]ownershipAuditRecord, error) {
	var selected []ownershipAuditRecord
	listener := "WORKER2"
	if strings.HasSuffix(client, "-worker1") {
		listener = "WORKER1"
	}
	for _, line := range strings.Split(string(data), "\n") {
		row, found, err := decodeOwnershipAuditRecord(line)
		if err != nil {
			return nil, err
		}
		if !found || row.Header.ClientID != client {
			continue
		}
		prefix, _, _ := strings.Cut(line, "Completed request:")
		if len(prefix) >= 25 && prefix[0] == '[' && prefix[24] == ']' {
			row.Completed, _ = time.Parse("2006-01-02 15:04:05,000", prefix[1:24])
			elapsed, timingErr := auditRequestDuration(row.TotalTime)
			if timingErr != nil {
				return nil, timingErr
			}
			row.Received = row.Completed.Add(-elapsed)
		}
		if row.Completed.After(end) {
			continue
		}
		if row.Response == nil || row.Completed.IsZero() || row.Received.Before(birth) || row.Completed.Before(row.Received) ||
			row.Listener != listener || row.Connection == "" {
			return nil, fmt.Errorf("coordinator protocol does not bind its selected lifetime")
		}
		selected = append(selected, row)
	}
	return selected, nil
}

func (s *ownershipCase) establishedCoordinatorProof(worker *ownershipCaseWorker, member ownershipMember, data []byte, rows []ownershipAuditRecord) bool {
	baseline := s.states["baseline"]
	old, ok := ownershipPhaseMember(baseline, 1)
	if !ok || old.ClientID != member.ClientID || old.MemberID != member.MemberID || len(old.Partitions) != replicatedPartitions || !worker.started.Before(baseline.Started) {
		return false
	}
	generation, ready, err := currentGenerationSaved(data, member.ClientID, s.group, member.MemberID, old.Partitions, map[int32]int64{}, time.Time{})
	if err != nil || !ready || generation != baseline.Generations[member.ClientID] {
		return false
	}
	for _, row := range rows {
		if row.Listener == "WORKER1" && row.Header.RequestAPIKeyName == "SYNC_GROUP" && row.Request.GroupID == s.group &&
			row.Request.MemberID == member.MemberID && row.Request.SyncGeneration == generation && row.Response.ErrorCode == 0 &&
			ownershipAssignment(row.Response.Assignment, s.src, old.Partitions) {
			return true
		}
	}
	return false
}

func ownershipAssignment(encoded, topic string, owned []int32) bool {
	data, err := base64.StdEncoding.DecodeString(encoded)
	var assignment kmsg.ConsumerMemberAssignment
	if err != nil || base64.StdEncoding.EncodeToString(data) != encoded || assignment.ReadFrom(data) != nil || assignment.Version != 0 ||
		!bytes.Equal(assignment.AppendTo(nil), data) || len(assignment.Topics) != 1 || assignment.Topics[0].Topic != topic {
		return false
	}
	parts := slices.Clone(assignment.Topics[0].Partitions)
	slices.Sort(parts)
	return slices.Equal(parts, owned)
}

func ownershipJoiningProof(rows []ownershipAuditRecord, member ownershipMember, group string, timeouts map[string]time.Duration) bool {
	if len(member.Partitions) != 0 || !ownershipNoJoinedProgress(rows, group) {
		return false
	}
	for _, join := range rows {
		if join.Header.RequestAPIKeyName != "JOIN_GROUP" || join.Listener != "WORKER2" || join.Request.GroupID != group ||
			join.Request.MemberID != "" || join.Response.ErrorCode != 79 || join.Response.MemberID != member.MemberID ||
			join.Request.SessionTimeout != int32(timeouts["session.timeout.ms"].Milliseconds()) ||
			join.Request.RebalanceTimeout != int32(timeouts["max.poll.interval.ms"].Milliseconds()) {
			continue
		}
		for _, api := range rows {
			if api.Header.RequestAPIKeyName == "API_VERSIONS" && api.Listener == "WORKER2" && api.Response.ErrorCode == 0 &&
				api.Connection == join.Connection && !api.Completed.After(join.Received) {
				return true
			}
		}
	}
	return false
}

func ownershipNoJoinedProgress(rows []ownershipAuditRecord, group string) bool {
	for _, row := range rows {
		key := row.Header.RequestAPIKeyName
		if key != "JOIN_GROUP" && key != "SYNC_GROUP" && key != "OFFSET_COMMIT" {
			continue
		}
		if row.Request.GroupID != group {
			return false
		}
		if (row.Header.RequestAPIKeyName == "JOIN_GROUP" || row.Header.RequestAPIKeyName == "SYNC_GROUP") && row.Response.ErrorCode == 0 {
			return false
		}
		if row.Header.RequestAPIKeyName == "OFFSET_COMMIT" && ownershipAnyAcceptedCommit(row) {
			return false
		}
	}
	return true
}

func ownershipAnyAcceptedCommit(row ownershipAuditRecord) bool {
	for _, topic := range row.Response.Topics {
		for _, part := range topic.Partitions {
			if part.ErrorCode == 0 {
				return true
			}
		}
	}
	return false
}

func ownershipZeroWork(logs string) bool {
	for _, line := range strings.Split(logs, "\n") {
		if !json.Valid([]byte(line)) {
			continue
		}
		var row struct {
			Message              string `json:"msg"`
			Submitted, Confirmed int64
		}
		if json.Unmarshal([]byte(line), &row) != nil || row.Message == "partition assigned" ||
			row.Message == "progress" && (row.Submitted != 0 || row.Confirmed != 0) {
			return false
		}
	}
	return true
}

// Early admission is provisional: physical heal and all retained completions
// through the observed exit are checked before either route can restart.
func (s *ownershipCase) validateCoordinatorEOFExits() {
	var candidates []*ownershipCaseWorker
	for _, worker := range s.workers {
		if worker.coordinatorPhase != "" {
			candidates = append(candidates, worker)
		}
	}
	if len(candidates) == 0 {
		return
	}
	require.False(s.t, s.coordinatorFault.Event.Heal.IsZero())
	query, cancel := context.WithTimeout(s.ctx, 2*time.Second)
	defer cancel()
	all, err := readAuditRequests(query, s.audit.targets, faults.Docker)
	require.NoError(s.t, err, "post-heal coordinator exit audit unavailable")
	readFinished := time.Now().UTC()
	for _, worker := range candidates {
		exit, retainErr := s.lifecycle.retainExit(worker.process)
		require.NoError(s.t, retainErr)
		_, phase, valid := s.coordinatorFailure(worker, exit.Witness, worker.process.logs())
		require.True(s.t, valid && phase == worker.coordinatorPhase, "coordinator EOF exit failed conclusive physical-window validation")
		if phase == ownershipJoiningEOF {
			member, found := ownershipPhaseMember(s.states["at_kill"], worker.route)
			require.True(s.t, found)
			// No replacement has started: a late successful completion cannot
			// repair the pending interpretation of this dead client's requests.
			rows, rowsErr := ownershipPhaseRows(all, member.ClientID, worker.started, readFinished)
			require.NoError(s.t, rowsErr)
			require.True(s.t, ownershipNoJoinedProgress(rows, s.group), "joining lifetime actually joined, acquired an assignment or committed before exit")
		}
		require.NoError(s.t, context.Cause(query))
	}
}

func ownershipCoordinatorFixture(t *testing.T, phase string) (*ownershipCase, *ownershipCaseWorker, *brokerExitWitness, string) {
	t.Helper()
	at := func(ms int) time.Time {
		return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(ms) * time.Millisecond)
	}
	parts := make([]int32, replicatedPartitions)
	for p := range parts {
		parts[p] = int32(p)
	}
	one := ownershipMember{ClientID: "selected-worker1", MemberID: "member1", Partitions: parts}
	two := ownershipMember{ClientID: "selected-worker2", MemberID: "member2"}
	s := &ownershipCase{t: t, id: "ownership/coordinator-kill-rebalance", src: "source", group: "group", directory: t.TempDir(),
		timeouts: map[string]time.Duration{"session.timeout.ms": 6 * time.Second, "max.poll.interval.ms": 30 * time.Second},
		states: map[string]ownershipCaseObservation{
			"baseline":            {Started: at(4000), Finished: at(4200), Members: []ownershipMember{one}, Generations: map[string]int32{one.ClientID: 1}},
			"preparing_rebalance": {Started: at(6400), Finished: at(6500)},
			"at_kill":             {Started: at(6900), Finished: at(7000), State: "PreparingRebalance", Coordinator: 2, Members: []ownershipMember{one, two}}},
		failedAfter: at(7100), events: []map[string]any{{"reason": "OS confirms held worker", "observed_at": at(6050)}, {"coordinator_kill_finished": at(7200)}}}
	event := faults.Event{Role: faults.Role{Kind: "coordinator", Cluster: "source", Group: "group"}, Fault: faults.Fault{Kind: "kill"}, Start: at(6800), Heal: at(8500)}
	for _, listener := range []string{"internal", "controller", "admin", "worker1", "worker2", "worker3"} {
		event.Targets = append(event.Targets, faults.Target{Cluster: "source", Broker: 2, Listener: listener, Service: "source-2", Container: "owned-source-2", Proxy: "source-2-" + listener, ProxyContainer: "owned-proxy"})
	}
	s.coordinatorFault = &faults.Injection{Event: event}
	s.events = append(s.events, map[string]any{"pid": 101, "route": 1, "signal": syscall.SIGSTOP.String(), "signal_number": int(syscall.SIGSTOP), "reason": "hold old member while a selected-binary peer joins", "started": at(5999), "finished": at(6001)},
		map[string]any{"pid": 101, "route": 1, "signal": syscall.SIGCONT.String(), "signal_number": int(syscall.SIGCONT), "reason": "resume held worker after coordinator kill", "started": at(7299), "finished": at(7301)})
	assignment := kmsg.ConsumerMemberAssignment{Topics: []kmsg.ConsumerMemberAssignmentTopic{{Topic: "source", Partitions: parts}}}
	rows := ownershipCoordinatorFixtureRow(at(2000), one.ClientID, "JOIN_GROUP", map[string]any{"groupId": "group"}, map[string]any{"errorCode": 0, "memberId": "member1", "generationId": 1}, "old", "WORKER1")
	rows += ownershipCoordinatorFixtureRow(at(2100), one.ClientID, "SYNC_GROUP", map[string]any{"groupId": "group", "memberId": "member1", "generationId": 1}, map[string]any{"errorCode": 0, "assignment": base64.StdEncoding.EncodeToString(assignment.AppendTo(nil))}, "old", "WORKER1")
	var committed, accepted []map[string]any
	for p := range parts {
		committed = append(committed, map[string]any{"partitionIndex": p, "committedOffset": 6})
		accepted = append(accepted, map[string]any{"partitionIndex": p, "errorCode": 0})
	}
	rows += ownershipCoordinatorFixtureRow(at(2200), one.ClientID, "OFFSET_COMMIT", map[string]any{"groupId": "group", "memberId": "member1", "generationIdOrMemberEpoch": 1, "topics": []any{map[string]any{"partitions": committed}}}, map[string]any{"topics": []any{map[string]any{"partitions": accepted}}}, "old", "WORKER1")
	rows += ownershipCoordinatorFixtureRow(at(6200), two.ClientID, "API_VERSIONS", map[string]any{}, map[string]any{"errorCode": 0}, "peer-connection", "WORKER2")
	rows += ownershipCoordinatorFixtureRow(at(6300), two.ClientID, "JOIN_GROUP", map[string]any{"groupId": "group", "memberId": "", "sessionTimeoutMs": 6000, "rebalanceTimeoutMs": 30000}, map[string]any{"errorCode": 79, "memberId": "member2"}, "peer-connection", "WORKER2")
	s.coordinatorAudit = &ownershipCoordinatorAudit{Started: at(6600), Finished: at(6700), Deadline: at(8600)}
	for broker := int32(1); broker <= 3; broker++ {
		target := faults.Target{Cluster: "source", Broker: broker, Listener: "admin", Service: fmt.Sprintf("source-%d", broker), Container: fmt.Sprintf("owned-source-%d", broker), Proxy: fmt.Sprintf("source-%d-admin", broker), ProxyContainer: "owned-proxy"}
		data := "KAFERRY_AUDIT_FILE kafka-request.log\n"
		if broker == 2 {
			data += rows
		}
		file := brokerFile{Path: fmt.Sprintf("source-%d.coordinator-pre-kill.requests.log", broker), SHA256: brokerDigest(data)}
		require.NoError(t, os.WriteFile(filepath.Join(s.directory, file.Path), []byte(data), 0600))
		s.coordinatorAudit.Brokers = append(s.coordinatorAudit.Brokers, ownershipCoordinatorBrokerAudit{Target: target, Raw: file})
	}
	worker := &ownershipCaseWorker{route: 1, started: at(1000)}
	worker.lifetime = brokerLifetime{Number: 1, PID: 101, Started: worker.started, Binary: strings.Repeat("b", 64)}
	exit := &brokerExitWitness{Lifetime: worker.lifetime, Stopped: at(7400), ExitCode: 1, DriverIntent: []cliSignalIntent{{At: at(6000), Signal: syscall.SIGSTOP.String(), Reason: "hold old member while a selected-binary peer joins"}, {At: at(7300), Signal: syscall.SIGCONT.String(), Reason: "resume held worker after coordinator kill"}}}
	progress := `{"msg":"progress","submitted":10,"confirmed":8,"pending_entries":2,"pending_estimated_bytes":20}` + "\n"
	summary := `{"msg":"copy summary","submitted":10,"confirmed":10,"peak_uncommitted_records":2,"peak_uncommitted_bytes":20}` + "\n"
	terminal := "save progress: " + brokerNegotiationEOF + "\nsave progress: " + brokerNegotiationEOF + "\nclose Kafka input: " + brokerNegotiationEOF + "\n"
	if phase == ownershipJoiningEOF {
		worker.route, worker.started = 2, at(6100)
		worker.lifetime = brokerLifetime{Number: 2, PID: 102, Started: worker.started, Binary: strings.Repeat("b", 64)}
		exit = &brokerExitWitness{Lifetime: worker.lifetime, Stopped: at(7150), ExitCode: 1}
		progress = `{"msg":"progress","submitted":0,"confirmed":0,"pending_entries":0,"pending_estimated_bytes":0}` + "\n"
		summary = `{"msg":"copy summary","submitted":0,"confirmed":0,"peak_uncommitted_records":0,"peak_uncommitted_bytes":0}` + "\n"
		terminal = "poll source: unable to join group session: " + brokerNegotiationEOF + "\nclose Kafka input: " + brokerNegotiationEOF + "\n"
	}
	return s, worker, exit, progress + summary + terminal
}

func ownershipCoordinatorFixtureRow(at time.Time, client, key string, request, response map[string]any, connection, listener string) string {
	data, _ := json.Marshal(map[string]any{"connection": connection, "listener": listener, "totalTimeMs": 0,
		"requestHeader": map[string]any{"clientId": client, "requestApiKeyName": key}, "request": request, "response": response})
	return fmt.Sprintf("[%s] DEBUG Completed request:%s (kafka.request.logger)\n", at.Format("2006-01-02 15:04:05,000"), data)
}

func rewriteOwnershipCoordinatorPrefix(t *testing.T, s *ownershipCase, old, new string) {
	t.Helper()
	entry := &s.coordinatorAudit.Brokers[1]
	path := filepath.Join(s.directory, entry.Raw.Path)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Contains(t, string(data), old)
	changed := strings.Replace(string(data), old, new, 1)
	require.NoError(t, os.WriteFile(path, []byte(changed), 0600))
	entry.Raw.SHA256 = brokerDigest(changed)
}

func TestOwnershipCoordinatorEOFRequiresRawBoundPhase(t *testing.T) {
	ownershipCoordinatorChildOutput(t)
	ownershipCoordinatorFallbackMutations(t)
	save, closeInput := "save progress: "+brokerNegotiationEOF, "close Kafka input: "+brokerNegotiationEOF
	join := "poll source: unable to join group session: " + brokerNegotiationEOF
	cases := []struct{ name, phase, terminal string }{
		{"pending close failure", ownershipJoiningEOF, join + "\n" + closeInput},
		{"pending close success", ownershipJoiningEOF, join},
	}
	for index, terminal := range sourceExitTerminals() {
		cases = append(cases, struct{ name, phase, terminal string }{fmt.Sprintf("established form %d", index), ownershipEstablishedEOF, terminal})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Given the exact observed vector and lifetime-bound pre-kill phase,
			// when physical healing is known, then only its strict window qualifies.
			s, w, exit, logs := ownershipCoordinatorFixture(t, tc.phase)
			logs = strings.Replace(logs, strings.Join(brokerTerminals(logs), "\n"), tc.terminal, 1)
			_, observed, valid := s.coordinatorFailure(w, exit, logs)
			require.True(t, valid)
			require.Equal(t, tc.phase, observed)
			ownershipCoordinatorOwnedChild(t, logs, tc.phase)
			s.coordinatorFault.Event.Heal = time.Time{}
			_, _, valid = s.coordinatorFailure(w, exit, logs)
			require.True(t, valid, "early admission remains provisional")
			s.coordinatorFault.Event.Heal = exit.Stopped
			_, _, valid = s.coordinatorFailure(w, exit, logs)
			require.False(t, valid, "physical-heal equality must reject during conclusive revalidation")
		})
	}
	for _, phase := range []string{ownershipEstablishedEOF, ownershipJoiningEOF} {
		for _, terminal := range []string{join, join + "\n" + closeInput} {
			t.Run(phase+" cannot borrow opposite work phase "+strings.ReplaceAll(terminal, brokerNegotiationEOF, "EOF"), func(t *testing.T) {
				// Given an ambiguous session error and its original raw protocol,
				// when complete work counts switch phases, then neither proof can be borrowed.
				s, worker, exit, logs := ownershipCoordinatorFixture(t, phase)
				logs = strings.Replace(logs, strings.Join(brokerTerminals(logs), "\n"), terminal, 1)
				if phase == ownershipEstablishedEOF {
					logs = strings.NewReplacer(`"submitted":10`, `"submitted":0`, `"confirmed":8`, `"confirmed":0`,
						`"confirmed":10`, `"confirmed":0`, `"pending_entries":2`, `"pending_entries":0`,
						`"pending_estimated_bytes":20`, `"pending_estimated_bytes":0`, `"peak_uncommitted_records":2`, `"peak_uncommitted_records":0`,
						`"peak_uncommitted_bytes":20`, `"peak_uncommitted_bytes":0`).Replace(logs)
				} else {
					logs = strings.NewReplacer(`"submitted":0`, `"submitted":10`, `"confirmed":0`, `"confirmed":10`).Replace(logs)
				}
				_, _, valid := s.coordinatorFailure(worker, exit, logs)
				require.False(t, valid)
			})
		}
	}
	for name, change := range map[string]func(string) string{
		"missing summary": func(logs string) string {
			return strings.Join(slices.DeleteFunc(strings.Split(logs, "\n"), func(line string) bool { return strings.Contains(line, `"copy summary"`) }), "\n")
		},
		"missing count": func(logs string) string { return strings.ReplaceAll(logs, `,"confirmed":0`, "") },
		"negative counts": func(logs string) string {
			return strings.NewReplacer(`"submitted":0`, `"submitted":-1`, `"confirmed":0`, `"confirmed":-1`).Replace(logs)
		},
		"unconfirmed":        func(logs string) string { return strings.ReplaceAll(logs, `"submitted":0`, `"submitted":1`) },
		"pending final save": func(logs string) string { return strings.Replace(logs, closeInput, save+"\n"+closeInput, 1) },
	} {
		t.Run(name, func(t *testing.T) {
			// Given the full pending protocol proof, when its raw work or terminal
			// stages differ, then the contextual phase remains fail closed.
			s, worker, exit, logs := ownershipCoordinatorFixture(t, ownershipJoiningEOF)
			_, _, valid := s.coordinatorFailure(worker, exit, change(logs))
			require.False(t, valid)
		})
	}
	t.Run("established progress remains required", func(t *testing.T) {
		s, w, exit, logs := ownershipCoordinatorFixture(t, ownershipEstablishedEOF)
		rewriteOwnershipCoordinatorPrefix(t, s, `"committedOffset":6`, `"committedOffset":0`)
		_, _, valid := s.coordinatorFailure(w, exit, logs)
		require.False(t, valid, "one missing owned partition cannot authorize the established member")
	})
	t.Run("established summary remains all confirmed", func(t *testing.T) {
		s, w, exit, logs := ownershipCoordinatorFixture(t, ownershipEstablishedEOF)
		logs = strings.Replace(logs, `"confirmed":10`, `"confirmed":9`, 1)
		_, _, valid := s.coordinatorFailure(w, exit, logs)
		require.False(t, valid)
	})
	for name, edit := range ownershipCoordinatorExitMutations() {
		t.Run(name, func(t *testing.T) {
			// Given fully rebound evidence, when one decisive gate differs,
			// then rehashing cannot authorize the pending member's EOF.
			s, w, exit, logs := ownershipCoordinatorFixture(t, ownershipJoiningEOF)
			edit(t, s, w, exit, &logs)
			_, _, valid := s.coordinatorFailure(w, exit, logs)
			require.False(t, valid)
		})
	}
}

func ownershipCoordinatorFallbackMutations(t *testing.T) {
	t.Helper()
	variants := []struct {
		phase   string
		swapped bool
	}{{ownershipEstablishedEOF, false}, {ownershipEstablishedEOF, true}, {ownershipJoiningEOF, false}, {ownershipJoiningEOF, true}}
	for _, diagnostic := range []string{brokerNegotiationEOF, networkFirstResponseEOF} {
		for _, variant := range variants {
			for _, malformed := range []string{
				"poll source: unable to join group session: " + diagnostic + " context deadline exceeded",
				diagnostic + " ILLEGAL_GENERATION\ncontext deadline exceeded",
				"context deadline exceeded\n" + diagnostic + " UNKNOWN_MEMBER_ID",
			} {
				t.Run(fmt.Sprintf("%s/mixed EOF %s/swapped-%t", variant.phase, strings.ReplaceAll(malformed, diagnostic, "EOF"), variant.swapped), func(t *testing.T) {
					// Given malformed EOF text and a known-cause suffix, when the same
					// classifier used by live considers it, phase proof cannot be bypassed.
					s, worker, exit, logs := ownershipCoordinatorFixture(t, variant.phase)
					logs = strings.Replace(logs, strings.Join(brokerTerminals(logs), "\n"), malformed, 1)
					if !variant.swapped {
						s.coordinatorAudit = nil
					} else if variant.phase == ownershipEstablishedEOF {
						logs = strings.NewReplacer(`"submitted":10`, `"submitted":0`, `"confirmed":8`, `"confirmed":0`, `"confirmed":10`, `"confirmed":0`,
							`"pending_entries":2`, `"pending_entries":0`, `"pending_estimated_bytes":20`, `"pending_estimated_bytes":0`,
							`"peak_uncommitted_records":2`, `"peak_uncommitted_records":0`, `"peak_uncommitted_bytes":20`, `"peak_uncommitted_bytes":0`).Replace(logs)
					} else {
						logs = strings.NewReplacer(`"submitted":0`, `"submitted":10`, `"confirmed":0`, `"confirmed":10`).Replace(logs)
					}
					_, _, valid := s.coordinatorFailure(worker, exit, logs)
					require.False(t, valid, "a known-cause substring cannot authorize a reserved EOF without matching proof")
				})
			}
		}
		warning := fmt.Sprintf("{\"msg\":\"source warning\",\"error\":%q}\n", diagnostic)
		// Given an EOF only in structured diagnostics, a genuine generic terminal
		// cause still follows the unchanged known-cause path.
		s := &ownershipCase{}
		terminal, phase, valid := s.coordinatorFailure(nil, nil, warning+"recoverable operation: ILLEGAL_GENERATION\n")
		require.True(t, valid)
		require.Empty(t, phase)
		require.Equal(t, "recoverable operation: ILLEGAL_GENERATION", terminal)
	}
}

func ownershipCoordinatorChildOutput(t *testing.T) {
	t.Helper()
	if os.Getenv("KAFERRY_OWNERSHIP_COORDINATOR_CHILD") != "1" {
		return
	}
	_, err := io.WriteString(os.Stdout, os.Getenv("KAFERRY_OWNERSHIP_COORDINATOR_PROGRESS"))
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, os.Stdin)
	require.NoError(t, err)
	_, err = io.WriteString(os.Stdout, os.Getenv("KAFERRY_OWNERSHIP_COORDINATOR_EXIT"))
	require.NoError(t, err)
	os.Exit(1)
}

func ownershipCoordinatorOwnedChild(t *testing.T, logs, phase string) {
	t.Helper()
	// Given a controlled child, when it emits the complete phase log and exits,
	// the actual sole-Wait witness must retain the same grammar and work summary.
	// The surrounding fixture separately checks the complete raw fault binding.
	if phase == ownershipEstablishedEOF {
		logs = strings.NewReplacer(`"submitted":10`, `"submitted":1521`, `"confirmed":10`, `"confirmed":1521`,
			`"peak_uncommitted_records":2`, `"peak_uncommitted_records":344`, `"peak_uncommitted_bytes":20`, `"peak_uncommitted_bytes":787059`).Replace(logs)
	}
	progress, tail, found := strings.Cut(logs, "\n")
	require.True(t, found)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	child := &cliProcess{command: exec.CommandContext(ctx, os.Args[0], "-test.run=^TestOwnershipCoordinatorEOFRequiresRawBoundPhase$"), done: make(chan struct{})}
	child.command.Env = append(os.Environ(), "KAFERRY_OWNERSHIP_COORDINATOR_CHILD=1",
		"KAFERRY_OWNERSHIP_COORDINATOR_PROGRESS="+progress+"\n", "KAFERRY_OWNERSHIP_COORDINATOR_EXIT="+tail)
	var err error
	child.stdin, err = child.command.StdinPipe()
	require.NoError(t, err)
	birth := time.Now().UTC()
	child.start(t)
	require.Eventually(t, func() bool {
		data, readErr := child.currentLogs()
		return readErr == nil && data == progress+"\n"
	}, 3*time.Second, time.Millisecond)
	executable, err := os.ReadFile(child.command.Path)
	require.NoError(t, err)
	registry := &brokerCopy{t: t, caseID: "ownership/coordinator-kill-rebalance", directory: t.TempDir(), workers: []*cliProcess{child},
		identities: []brokerLifetime{{Number: 1, PID: child.pid(), Started: birth, Binary: brokerDigest(string(executable))}}}
	require.NoError(t, child.stdin.Close())
	require.Error(t, child.wait())
	exit, err := registry.retainExit(child)
	require.NoError(t, err)
	require.Equal(t, 1, exit.Witness.ExitCode)
	require.Empty(t, exit.Witness.Signal)
	require.Empty(t, exit.Witness.DriverIntent)
	require.Equal(t, brokerDigest(logs), exit.Witness.Log.SHA256)
	work, err := brokerWork(child.logs())
	require.NoError(t, err)
	require.NotNil(t, work.Summary)
	_, observed := ownershipEOFTerminal(child.logs(), work.Summary)
	require.Equal(t, phase, observed)
	require.Equal(t, work.Summary.Submitted, work.Summary.Confirmed)
	if phase == ownershipEstablishedEOF {
		require.Equal(t, 1521, work.Summary.Submitted)
		require.Equal(t, 344, work.Summary.PeakRecords)
		require.Equal(t, int64(787059), work.Summary.PeakBytes)
	} else {
		require.Zero(t, work.Summary.Submitted)
		require.True(t, ownershipZeroWork(child.logs()))
	}
}

func ownershipCoordinatorExitMutations() map[string]func(*testing.T, *ownershipCase, *ownershipCaseWorker, *brokerExitWitness, *string) {
	return map[string]func(*testing.T, *ownershipCase, *ownershipCaseWorker, *brokerExitWitness, *string){
		"padding": func(_ *testing.T, _ *ownershipCase, _ *ownershipCaseWorker, _ *brokerExitWitness, logs *string) {
			*logs = strings.Replace(*logs, "close Kafka input:", " close Kafka input:", 1)
		},
		"extra": func(_ *testing.T, _ *ownershipCase, _ *ownershipCaseWorker, _ *brokerExitWitness, logs *string) {
			*logs += "unknown failure\n"
		},
		"wrong case": func(_ *testing.T, s *ownershipCase, _ *ownershipCaseWorker, _ *brokerExitWitness, _ *string) {
			s.id = "network/destination-outage"
		},
		"wrong role": func(_ *testing.T, s *ownershipCase, _ *ownershipCaseWorker, _ *brokerExitWitness, _ *string) {
			s.coordinatorFault.Event.Role.Cluster = "destination"
		},
		"kill equality": func(_ *testing.T, s *ownershipCase, _ *ownershipCaseWorker, e *brokerExitWitness, _ *string) {
			e.Stopped = s.failedAfter
		},
		"signal": func(_ *testing.T, _ *ownershipCase, _ *ownershipCaseWorker, e *brokerExitWitness, _ *string) {
			e.Signal = "killed"
		},
		"intent": func(_ *testing.T, _ *ownershipCase, _ *ownershipCaseWorker, e *brokerExitWitness, _ *string) {
			e.DriverIntent = []cliSignalIntent{{Reason: "context cancellation"}}
		},
		"birth after protocol": func(_ *testing.T, _ *ownershipCase, w *ownershipCaseWorker, e *brokerExitWitness, _ *string) {
			w.started = w.started.Add(250 * time.Millisecond)
			w.lifetime.Started = w.started
			e.Lifetime = w.lifetime
		},
		"authentication response": func(t *testing.T, s *ownershipCase, _ *ownershipCaseWorker, _ *brokerExitWitness, _ *string) {
			rewriteOwnershipCoordinatorPrefix(t, s, `"errorCode":79`, `"errorCode":58`)
		},
		"wrong member": func(t *testing.T, s *ownershipCase, _ *ownershipCaseWorker, _ *brokerExitWitness, _ *string) {
			rewriteOwnershipCoordinatorPrefix(t, s, `"memberId":"member2"`, `"memberId":"unrelated"`)
		},
		"different connection": func(t *testing.T, s *ownershipCase, _ *ownershipCaseWorker, _ *brokerExitWitness, _ *string) {
			rewriteOwnershipCoordinatorPrefix(t, s, `"connection":"peer-connection"`, `"connection":"unrelated"`)
		},
		"positive progress": func(_ *testing.T, _ *ownershipCase, _ *ownershipCaseWorker, _ *brokerExitWitness, logs *string) {
			*logs = strings.Replace(*logs, `"submitted":0`, `"submitted":1`, 1)
		},
	}
}
