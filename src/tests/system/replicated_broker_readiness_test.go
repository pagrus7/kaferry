//go:build integration && fault && load

package compat

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/src/tests/system/faults"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const brokerReadinessCriterion = "target-admin-metadata-all-topics-v1"
const brokerMetadataCaptureLimit = 64 << 10

type brokerHealthCheck struct {
	Criterion  string                 `json:"criterion"`
	Deadline   time.Time              `json:"deadline"`
	Probes     []brokerAPIProbe       `json:"api_probes,omitempty"`
	At         time.Time              `json:"at"`
	Containers []string               `json:"containers"`
	Error      string                 `json:"error,omitempty"`
	Passed     bool                   `json:"passed"`
	Details    []brokerHealthSnapshot `json:"details,omitempty"`
}

func brokerDiagnosticText(value string) string {
	if len(value) > 2048 {
		return value[:2048] + " [truncated]"
	}
	return value
}

type brokerAPIProbe struct {
	faults.MetadataProbe
	Topic             string `json:"topic"`
	ClusterID         string `json:"cluster_id"`
	Response          string `json:"response_json"`
	ResponseSHA256    string `json:"response_sha256"`
	ResponseTruncated bool   `json:"response_truncated"`
}

func (s *brokerCopy) healthy(ctx context.Context, targets ...faults.Target) bool {
	check := brokerHealthCheck{At: time.Now().UTC(), Criterion: brokerReadinessCriterion}
	check.Deadline, _ = ctx.Deadline()
	defer func() { s.healthChecks = append(s.healthChecks, check) }()
	if err := s.inspectBrokerHealth(ctx, &check, targets); err != nil {
		check.Error = brokerDiagnosticText(err.Error())
		return false
	}
	// All targets share the caller's remaining budget and own their request.
	check.Probes = make([]brokerAPIProbe, len(targets))
	var reads sync.WaitGroup
	for index, target := range targets {
		reads.Go(func() { check.Probes[index] = s.probeBrokerAPI(ctx, target) })
	}
	reads.Wait()
	if ctx.Err() != nil {
		check.Error = ctx.Err().Error()
		return false
	}
	for _, probe := range check.Probes {
		if probe.Error != "" {
			check.Error = probe.Error
			return false
		}
	}
	check.Passed = true
	return true
}

func (s *brokerCopy) inspectBrokerHealth(ctx context.Context, check *brokerHealthCheck, targets []faults.Target) error {
	args := []string{"inspect", "--format", brokerHealthFormat}
	for _, target := range targets {
		args = append(args, target.Container)
		check.Containers = append(check.Containers, target.Container)
	}
	run := s.lab.Docker
	if run == nil {
		run = brokerHealthDocker
	}
	data, err := run(ctx, args...)
	if err != nil {
		return err
	}
	check.Details, err = brokerHealthSnapshots(data, len(targets))
	if err != nil {
		return err
	}
	for index, target := range targets {
		state := check.Details[index]
		if s.lab.Project == "" || state.ID != target.Container || state.Project != s.lab.Project || state.Service != target.Service || !*state.Running || state.StartedAt.After(check.At) {
			return errors.New("resolved broker is stopped or Docker identity differs from owned target")
		}
	}
	return nil
}

func (s *brokerCopy) probeBrokerAPI(ctx context.Context, target faults.Target) brokerAPIProbe {
	topic := s.src
	if target.Cluster == "destination" {
		topic = s.dst
	}
	probe := brokerAPIProbe{MetadataProbe: faults.MetadataProbe{Target: target, ClientCluster: target.Cluster, Route: "admin", APIKey: 3, AllTopics: true, StartedAt: time.Now().UTC()}, Topic: topic, ClusterID: s.clusters[target.Cluster]}
	client := s.metadata[target.Cluster]
	if client == nil {
		probe.Error = "selected metadata client is unavailable"
		probe.FinishedAt = time.Now().UTC()
		return probe
	}
	response, transport, err := client.ProbeMetadata(ctx, target)
	probe.MetadataProbe = transport
	if response != nil {
		err = errors.Join(err, probe.retainResponse(response))
	}
	if err == nil {
		err = brokerMetadataValid(response, probe.ClusterID, topic, client.AdminRoutes())
	}
	if err != nil {
		probe.Error = brokerDiagnosticText(err.Error())
	}
	return probe
}

func (p *brokerAPIProbe) retainResponse(response *kmsg.MetadataResponse) error {
	encoded, err := json.Marshal(response)
	if err != nil {
		return err
	}
	if len(encoded) > brokerMetadataCaptureLimit {
		p.ResponseTruncated = true
		return errors.New("metadata response exceeded 64 KiB")
	}
	p.Response = string(encoded)
	digest := sha256.Sum256(encoded)
	p.ResponseSHA256 = hex.EncodeToString(digest[:])
	return nil
}

func brokerMetadataValid(response *kmsg.MetadataResponse, cluster, topic string, routes map[int32]string) error {
	if response == nil || response.Version < 1 || response.ErrorCode != 0 || response.ClusterID == nil || cluster == "" || *response.ClusterID != cluster || len(response.Brokers) != 3 {
		return errors.New("metadata response omitted expected cluster identity or broker set")
	}
	seen := make(map[int32]bool)
	for _, broker := range response.Brokers {
		if seen[broker.NodeID] || routes[broker.NodeID] == "" || net.JoinHostPort(broker.Host, strconv.Itoa(int(broker.Port))) != routes[broker.NodeID] {
			return errors.New("metadata response differs from expected ADMIN routes")
		}
		seen[broker.NodeID] = true
	}
	detail, err := brokerMetadataTopic(response, topic)
	if err != nil {
		return err
	}
	return brokerMetadataPartitions(detail.Partitions, seen)
}

func brokerMetadataTopic(response *kmsg.MetadataResponse, topic string) (kmsg.MetadataResponseTopic, error) {
	var selected kmsg.MetadataResponseTopic
	matches := 0
	for _, detail := range response.Topics {
		if detail.Topic != nil && *detail.Topic == topic {
			selected = detail
			matches++
		}
	}
	if matches != 1 || selected.ErrorCode != 0 || len(selected.Partitions) != replicatedPartitions {
		return selected, errors.New("metadata all-topics response omitted unique complete workload topic")
	}
	return selected, nil
}

func brokerMetadataPartitions(partitions []kmsg.MetadataResponseTopicPartition, brokers map[int32]bool) error {
	seen := make(map[int32]bool)
	for _, part := range partitions {
		if part.ErrorCode != 0 || part.Partition < 0 || part.Partition >= replicatedPartitions || seen[part.Partition] || !brokers[part.Leader] || len(part.ISR) == 0 {
			return errors.New("metadata workload partition is unavailable or malformed")
		}
		seen[part.Partition] = true
		isr := make(map[int32]bool)
		for _, id := range part.ISR {
			if !brokers[id] || isr[id] {
				return errors.New("metadata ISR is malformed")
			}
			isr[id] = true
		}
		if !isr[part.Leader] {
			return errors.New("metadata leader is outside ISR")
		}
	}
	return nil
}

// Generic API readiness permits ISR loss; callers require their desired ISR
// from the same fresh responses, independently of ordinary topology snapshots.
func brokerReadinessISR(checks []brokerHealthCheck, cluster string, partition int32, size int, leader int32) bool {
	count := 0
	for _, check := range checks {
		if !check.Passed {
			return false
		}
		for _, probe := range check.Probes {
			if probe.Target.Cluster != cluster {
				continue
			}
			count++
			if !brokerProbeISR(probe, partition, size, leader) {
				return false
			}
		}
	}
	return count > 0
}

func brokerProbeISR(probe brokerAPIProbe, partition int32, size int, leader int32) bool {
	var response kmsg.MetadataResponse
	if err := json.Unmarshal([]byte(probe.Response), &response); err != nil {
		return false
	}
	topic, err := brokerMetadataTopic(&response, probe.Topic)
	if err != nil {
		return false
	}
	found := 0
	for _, part := range topic.Partitions {
		if partition >= 0 && part.Partition != partition {
			continue
		}
		found++
		if len(part.ISR) != size || (leader >= 0 && part.Leader != leader) {
			return false
		}
	}
	expected := replicatedPartitions
	if partition >= 0 {
		expected = 1
	}
	return found == expected
}

func TestBrokerReadinessRequiresFreshPinnedMetadataWithinSharedBudget(t *testing.T) {
	// Given a running owned broker, when its fresh API response is absent or
	// invalid, then a running container and a healthy peer cannot satisfy readiness.
	for _, mode := range []string{"running", "ISR1", "worker route blocked", "stopped", "missing running", "missing start", "new start", "wrong service", "wrong owner", "wrong container", "malformed Docker", "API error", "wrong cluster", "wrong routes", "missing topic", "bad partition", "blocked target", "cancelled", "shared deadline"} {
		t.Run(mode, func(t *testing.T) {
			testBrokerReadiness(t, mode)
		})
	}
}

func testBrokerReadiness(t *testing.T, mode string) {
	t.Helper()
	cluster := kfake.MustCluster(kfake.NumBrokers(4), kfake.ClusterID("source-id"))
	t.Cleanup(cluster.Close)
	routes := cluster.ListenAddrs()[1:]
	var nodes [4]atomic.Int32
	controlBrokerReadiness(cluster, routes, mode, &nodes)
	client, err := faults.NewMetadataClient("source", routes...)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	target := faults.Target{Cluster: "source", Broker: 2, Service: "source-2", Container: "owned-container", Listener: "worker1"}
	targets := []faults.Target{target}
	if mode == "shared deadline" {
		targets = nil
		for id := int32(1); id <= 3; id++ {
			targets = append(targets, faults.Target{Cluster: "source", Broker: id, Service: fmt.Sprintf("source-%d", id), Container: fmt.Sprintf("owned-%d", id)})
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	if mode == "cancelled" {
		cancel()
	}
	calls := 0
	s := &brokerCopy{metadata: faults.KafkaMetadata{"source": client}, clusters: map[string]string{"source": "source-id"}, src: "records", lab: &faults.Lab{Project: "owned-project"}}
	s.lab.Docker = brokerReadinessDocker(ctx, t, targets, mode, &calls)
	started := time.Now()
	ready := s.healthy(ctx, targets...)
	elapsed := time.Since(started)
	require.Equal(t, 1, calls)
	require.Len(t, s.healthChecks, 1)
	check := s.healthChecks[0]
	require.Equal(t, brokerReadinessCriterion, check.Criterion)
	deadline, _ := ctx.Deadline()
	require.Equal(t, deadline, check.Deadline)
	if mode == "running" || mode == "ISR1" || mode == "worker route blocked" {
		require.True(t, ready, check.Error)
		require.Len(t, check.Probes, 1)
		probe := check.Probes[0]
		require.Equal(t, routes[1], probe.RemoteAddress)
		require.Equal(t, target, probe.Target)
		require.Equal(t, "admin", probe.Route)
		require.NotEmpty(t, probe.ResponseSHA256)
		require.True(t, probe.AllTopics)
		require.Equal(t, int32(2), probe.TransportBroker)
		require.False(t, probe.ResponseAt.Before(check.At))
		require.False(t, probe.FinishedAt.After(check.Deadline))
		require.Equal(t, mode != "ISR1", brokerReadinessISR(s.healthChecks, "source", -1, 3, -1))
		// ADMIN success is explicit; the selected worker listener is not probed.
		require.Equal(t, "worker1", probe.Target.Listener)
		require.NotEqual(t, probe.Target.Listener, probe.Route)
	} else {
		require.False(t, ready)
		require.NotEmpty(t, check.Error)
	}
	if mode == "worker route blocked" {
		requireBlockedWorkerAPI(ctx, t)
		require.True(t, check.Passed, "ADMIN readiness does not establish worker-route recovery")
	}
	if mode == "blocked target" {
		require.Positive(t, nodes[2].Load())
		require.Len(t, check.Probes, 1)
		require.NotEmpty(t, check.Probes[0].Error)
	}
	if mode == "shared deadline" {
		require.Len(t, check.Probes, 3)
		for id := 1; id <= 3; id++ {
			require.Positive(t, nodes[id].Load(), "each selected broker must receive its own request before the shared deadline")
		}
		require.Less(t, elapsed, time.Second)
		for _, probe := range check.Probes {
			require.NotEmpty(t, probe.Error)
		}
	}
}

func requireBlockedWorkerAPI(ctx context.Context, t *testing.T) {
	t.Helper()
	worker := kfake.MustCluster(kfake.NumBrokers(1))
	t.Cleanup(worker.Close)
	worker.ControlKey(int16(kmsg.Metadata), func(kmsg.Request) (kmsg.Response, error, bool) { worker.KeepControl(); return nil, nil, true })
	client, err := kgo.NewClient(kgo.SeedBrokers(worker.ListenAddrs()...))
	require.NoError(t, err)
	t.Cleanup(client.Close)
	_, err = client.Request(ctx, kmsg.NewPtrMetadataRequest())
	require.ErrorIs(t, err, context.DeadlineExceeded, "independent worker-route API remains blocked despite successful ADMIN response")
}

func controlBrokerReadiness(cluster *kfake.Cluster, routes []string, mode string, nodes *[4]atomic.Int32) {
	cluster.ControlKey(int16(kmsg.Metadata), func(request kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		metadata := request.(*kmsg.MetadataRequest)
		node := cluster.CurrentNode()
		if metadata.Topics == nil {
			nodes[node].Add(1)
		}
		if metadata.Topics == nil && (mode == "shared deadline" || mode == "blocked target" && node == 2) {
			return nil, nil, true
		}
		response := brokerMetadataFixture(metadata.Version, routes)
		switch mode {
		case "API error":
			response.ErrorCode = kerr.ClusterAuthorizationFailed.Code
		case "wrong cluster":
			response.ClusterID = kmsg.StringPtr("other-cluster")
		case "wrong routes":
			response.Brokers[0].Port++
		case "missing topic":
			response.Topics = nil
		case "bad partition":
			response.Topics[0].Partitions[0].ErrorCode = kerr.NotLeaderForPartition.Code
		case "ISR1":
			response.Topics[0].Partitions[0].ISR = []int32{1}
		}
		return response, nil, true
	})
}

func brokerReadinessDocker(ctx context.Context, t *testing.T, targets []faults.Target, mode string, calls *int) faults.Command {
	t.Helper()
	return func(actual context.Context, args ...string) ([]byte, error) {
		*calls++
		require.Equal(t, ctx, actual)
		require.Equal(t, brokerHealthFormat, args[2])
		require.NotContains(t, brokerHealthFormat, ".Config.Env")
		if mode == "shared deadline" {
			select {
			case <-time.After(250 * time.Millisecond):
			case <-actual.Done():
				return nil, actual.Err()
			}
		}
		if mode == "malformed Docker" {
			return []byte("{"), nil
		}
		var rows []string
		for _, target := range targets {
			row := map[string]any{"Running": true, "StartedAt": "2026-11-08T22:55:50Z"}
			row["ID"], row["Project"], row["Service"] = target.Container, "owned-project", target.Service
			editBrokerDockerFixture(row, mode)
			data, encodeErr := json.Marshal(row)
			require.NoError(t, encodeErr)
			rows = append(rows, string(data))
		}
		return []byte(strings.Join(rows, "\n")), nil
	}
}

func editBrokerDockerFixture(row map[string]any, mode string) {
	switch mode {
	case "stopped":
		row["Running"] = false
	case "missing running":
		delete(row, "Running")
	case "missing start":
		delete(row, "StartedAt")
	case "new start":
		row["StartedAt"] = time.Now().Add(time.Hour).UTC()
	case "wrong service":
		row["Service"] = "foreign-service"
	case "wrong owner":
		row["Project"] = "other-project"
	case "wrong container":
		row["ID"] = "other-container"
	}
}

func brokerMetadataFixture(version int16, routes []string) *kmsg.MetadataResponse {
	response := kmsg.NewPtrMetadataResponse()
	response.Version = version
	response.ClusterID = kmsg.StringPtr("source-id")
	response.ControllerID = 1
	for index, address := range routes {
		host, port, _ := net.SplitHostPort(address)
		number, _ := strconv.Atoi(port)
		response.Brokers = append(response.Brokers, kmsg.MetadataResponseBroker{NodeID: int32(index + 1), Host: host, Port: int32(number)})
	}
	topic := kmsg.NewMetadataResponseTopic()
	topic.Topic = kmsg.StringPtr("records")
	for partition := int32(0); partition < replicatedPartitions; partition++ {
		part := kmsg.NewMetadataResponseTopicPartition()
		part.Partition = partition
		part.Leader = 1
		part.Replicas = []int32{1, 2, 3}
		part.ISR = []int32{1, 2, 3}
		topic.Partitions = append(topic.Partitions, part)
	}
	response.Topics = append(response.Topics, topic)
	return response
}
