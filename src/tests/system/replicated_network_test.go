//go:build integration && fault && load

package compat

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/src/tests/system/faults"
	"github.com/stretchr/testify/require"
)

func TestReplicatedNetworkFaults(t *testing.T) {
	for _, name := range []string{"destination-outage", "destination-leaders-outage", "latency", "bandwidth", "slow-close"} {
		t.Run(name, func(t *testing.T) { replicatedNetworkFault(t, name) })
	}
}

type networkInterval struct {
	Before brokerObservation `json:"before"`
	After  brokerObservation `json:"after"`
}

func (w networkInterval) seconds() float64 { return w.After.At.Sub(w.Before.At).Seconds() }

func networkTotal(values []int64) int64 {
	var total int64
	for _, value := range values {
		total += value
	}
	return total
}

func (w networkInterval) rate() float64 {
	return float64(networkTotal(w.After.Ends)-networkTotal(w.Before.Ends)) / w.seconds()
}

func networkBacklog(state brokerObservation) int64 {
	return max(0, networkTotal(state.Publishing.Acknowledged)-networkTotal(state.Ends))
}

func networkMeasurementReady(state brokerObservation) bool {
	publication := state.Publishing
	if !brokerHealthyCopy(state) || networkBacklog(state) > replicatedPartitions ||
		len(publication.Offered) != replicatedPartitions || publication.Started.IsZero() ||
		publication.Started.After(state.At) || publication.Observed.Before(state.At) {
		return false
	}
	due := int64(publication.Observed.Sub(publication.Started) / (100 * time.Millisecond))
	for partition, acknowledged := range publication.Acknowledged {
		offered := publication.Offered[partition]
		if acknowledged < due-1 || acknowledged > due || offered < acknowledged || offered > due || offered-acknowledged > 1 {
			return false
		}
	}
	return true
}

// Hold the actual configured interval while continuing independent observations.
// Returning early on the desired effect would shorten the claimed exposure.
func (s *brokerCopy) networkSpan(before brokerObservation, duration time.Duration, allowFailure bool) networkInterval {
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		query, cancel := context.WithTimeout(s.ctx, 2*time.Second)
		started := time.Now()
		after, ok := s.observe(query)
		poll := brokerPoll{Label: "network interval", DurationMS: time.Since(started).Milliseconds(), Complete: ok, State: after}
		if err := query.Err(); err != nil {
			poll.QueryError = err.Error()
		}
		s.recordPoll(poll)
		cancel()
		s.live(allowFailure)
		require.NoError(s.t, s.publisher.snapshot().err)
		require.True(s.t, ok, "independent routes must remain available throughout worker network faults")
		require.True(s.t, fullyReplicated(after, "source") && fullyReplicated(after, "destination"))
		if after.At.Sub(before.At) >= duration {
			result := networkInterval{before, after}
			rate := float64(networkTotal(after.Publishing.Acknowledged)-networkTotal(before.Publishing.Acknowledged)) / result.seconds()
			require.GreaterOrEqual(s.t, rate, .95*float64(replicatedPartitions)*10, "independent source publisher fell below its steady workload")
			return result
		}
		select {
		case <-s.ctx.Done():
			s.t.Fatal("network exposure exceeded case context: ", s.ctx.Err())
		case <-tick.C:
		}
	}
}

func (s *brokerCopy) networkInject(role faults.Role, fault faults.Fault, before brokerObservation) (*faults.Injection, int) {
	require.True(s.t, brokerHealthyCopy(before))
	process := s.workers[len(s.workers)-1]
	require.False(s.t, process.finished())
	require.NotNil(s.t, before.Worker)
	require.Equal(s.t, s.lifetime(process), before.Worker.Lifetime)
	window := s.oracle.begin(s.t, fault.Kind, role, false, s.timeouts,
		"healthy measured copying, saved prefixes on all 24 partitions and independently acknowledged steady input")
	injection, err := s.lab.Inject(s.ctx, role, fault)
	s.retainNetworkInjection(injection, fmt.Sprintf("fault-%d", window))
	require.NoError(s.t, err)
	s.active, s.activeWindow, s.beforeFault = injection, window, before
	s.oracle.injected(s.t, window, fault.Kind, injection.Event, "owned worker-only proxy operation completed on resolved targets")
	return injection, window
}

func (s *brokerCopy) retainNetworkInjection(injection *faults.Injection, name string) {
	if injection == nil {
		return
	}
	s.t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 30*time.Second)
		defer done()
		if err := injection.Heal(cleanup); err != nil {
			s.t.Error("heal original worker network route: ", err)
		}
		data, err := json.MarshalIndent(map[string]any{"event": injection.Event, "case_failed": s.t.Failed()}, "", "  ")
		if err == nil {
			err = os.WriteFile(filepath.Join(s.directory, name+".json"), append(data, '\n'), 0600)
		}
		if err != nil {
			s.t.Error("retain network injection: ", err)
		}
	})
}

func replicatedNetworkFault(t *testing.T, name string) {
	// Given one exact candidate CLI, 24 RF=3/minISR=2 partitions and independent
	// admin, publisher and continuous librdkafka routes, measure healthy output.
	s, baseline := startReplicatedFaultCopy(t, "network/"+name)
	span := s.timeouts["session.timeout.ms"]
	baseline = s.wait("source schedule caught up and output backlog drained before healthy measurement", false,
		func(_ context.Context, state brokerObservation) bool { return networkMeasurementReady(state) })
	healthy := s.networkSpan(baseline, span, false)
	require.GreaterOrEqual(t, healthy.rate(), .95*float64(replicatedPartitions)*10, "healthy fixture did not sustain scheduled input")
	role := faults.Role{Kind: "worker-destination", Cluster: "destination", Worker: 1}
	fault := faults.Fault{Kind: "cut"}
	switch name {
	case "destination-leaders-outage":
		role.Kind, role.Topic = "leader", s.dst
	case "latency":
		fault = faults.Fault{Kind: "latency", Stream: "upstream", Value: 500}
	case "bandwidth":
		fault = faults.Fault{Kind: "bandwidth", Stream: "upstream", Value: 32}
	case "slow-close":
		fault = faults.Fault{Kind: "slow_close", Stream: "downstream", Value: s.timeouts["delivery_timeout"].Milliseconds()}
	}
	// When the fault resolves the current worker destinations or current leader,
	// retain the original targets and the state immediately after injection.
	exited := observeReplicatedExit(s.workers[0])
	injection, window := s.networkInject(role, fault, healthy.After)
	injected := s.wait("independent state after worker route injection", false, func(_ context.Context, _ brokerObservation) bool { return true })
	affected, unaffected := make([]int, 0, replicatedPartitions), make([]int, 0, replicatedPartitions)
	for partition, part := range injected.Topology["destination"] {
		blocked := slices.ContainsFunc(injection.Event.Targets, func(target faults.Target) bool { return target.Broker == part.Leader })
		if blocked {
			affected = append(affected, partition)
		} else {
			unaffected = append(unaffected, partition)
		}
	}
	if name == "destination-leaders-outage" {
		require.NotEmpty(t, unaffected, "leader-only cut must leave independently observed healthy partitions")
	} else {
		require.Len(t, affected, replicatedPartitions)
	}
	var stimulus *faults.Injection
	var closeProof map[string]any
	var closeRead <-chan error
	var connection net.Conn
	if name == "slow-close" {
		// A slow-close toxic is dormant until EOF. A separate real TCP connection
		// on one of these same worker routes witnesses delayed EOF, while the
		// explicit upstream timeout supplies EOF to existing CLI connections too.
		target := injection.Event.Targets[0]
		address := faults.Endpoints("destination", 1)[target.Broker-1]
		var err error
		connection, err = (&net.Dialer{}).DialContext(s.ctx, "tcp", address)
		require.NoError(t, err)
		t.Cleanup(func() { _ = connection.Close() })
		stimulus, err = s.lab.CloseWorkerConnections(s.ctx, injection)
		s.retainNetworkInjection(stimulus, "close-stimulus")
		require.NoError(t, err)
		s.oracle.injected(t, window, stimulus.Event.Fault.Kind, stimulus.Event, "upstream 1ms timeout supplies real backend EOF only on the frozen slow-close worker routes")
		started := time.Now().UTC()
		require.NoError(t, connection.SetReadDeadline(started.Add(span)))
		read := make(chan error, 1)
		go func() { var value [1]byte; _, err := connection.Read(value[:]); read <- err }()
		closeRead = read
		closeProof = map[string]any{"proxy": target.Proxy, "started_at": started, "minimum_delay_seconds": span.Seconds()}
		injected = s.wait("independent state after EOF stimulus", false, func(_ context.Context, _ brokerObservation) bool { return true })
	}
	duration := span
	allowFailure := name == "destination-outage"
	if allowFailure {
		duration += s.timeouts["delivery_timeout"]
	}
	during := s.networkSpan(injected, duration, allowFailure)
	s.requireNetworkEffect(name, healthy, during, affected, unaffected)
	if closeRead != nil {
		err := <-closeRead
		var timeout net.Error
		require.ErrorAs(t, err, &timeout, "backend EOF arrived without the required slow-close delay")
		require.True(t, timeout.Timeout())
		closeProof["delayed_until"], closeProof["read_error"] = time.Now().UTC(), err.Error()
		require.Greater(t, during.After.Pending.Submitted, during.After.Pending.Confirmed)
	}
	if allowFailure {
		require.True(t, s.workers[0].finished(), "whole destination outage exceeded delivery timeout without fail-fast exit")
		s.live(true)
		exit := <-exited
		require.True(t, exit.After(injection.Event.Start))
		closeProof = map[string]any{"exit_observed_at": exit}
	}
	// Then remove only the recorded worker faults. Any recorded fail-fast exit
	// restarts with the same selected binary, SQL, group and worker1 route.
	if stimulus != nil {
		require.NoError(t, stimulus.Heal(s.ctx))
	}
	require.NoError(t, injection.Heal(s.ctx))
	if allowFailure {
		s.measureLast("destination network outage fail-fast exit; restarted after route recovery")
		s.start()
	}
	if connection != nil {
		require.NoError(t, connection.SetReadDeadline(time.Now().Add(span)))
		var value [1]byte
		_, err := connection.Read(value[:])
		require.ErrorIs(t, err, io.EOF, "removing the toxic must release delayed EOF")
		closeProof["eof_observed_at"] = time.Now().UTC()
		closeProof["stimulus"] = stimulus.Event
	}
	recovered := s.wait("every partition resumes saved progress after worker route healing", false, func(_ context.Context, state brokerObservation) bool {
		return allAbove(state.Saved, during.After.Saved, 2) && networkMeasurementReady(state)
	})
	recovery := s.networkSpan(recovered, span, false)
	require.GreaterOrEqual(t, recovery.rate(), .95*healthy.rate(), "measured output throughput did not recover after toxic removal")
	require.True(t, allAbove(recovery.After.Saved, recovery.Before.Saved, 2))
	s.oracle.healed(t, window, "original worker routes restored; all partitions save again and measured output throughput recovered")
	s.events = append(s.events, map[string]any{"fault": injection.Event, "healthy": healthy, "during": during, "recovery": recovery,
		"affected_partitions": affected, "unaffected_partitions": unaffected, "connection_effect": closeProof})
	publication := s.publisher.snapshot()
	require.LessOrEqual(t, publication.MaxGapSeconds, 2.0, "independent source acknowledgements stopped during a worker destination fault")
	// The shared finalizer stops all publishers/workers and requires the complete
	// registered oracle horizon, final librdkafka reconciliation and duplicate cuts.
	s.finishReplicatedFault("network/"+name, baseline, "replicated-network.json")
}

func (s *brokerCopy) requireNetworkEffect(name string, healthy, during networkInterval, affected, unaffected []int) {
	t := s.t
	require.True(t, allAbove(during.After.Publishing.Acknowledged, during.Before.Publishing.Acknowledged, 2))
	for _, partition := range affected {
		if name != "latency" && name != "bandwidth" {
			require.Equal(t, during.Before.Ends[partition], during.After.Ends[partition], "blocked destination partition acknowledged new output")
			require.LessOrEqual(t, during.After.Saved[partition], during.After.Ends[partition])
		}
	}
	for _, partition := range unaffected {
		require.Greater(t, during.After.Ends[partition], during.Before.Ends[partition]+2)
		require.Greater(t, during.After.Saved[partition], during.Before.Saved[partition]+2, "unaffected partition made no independently observed saved progress")
	}
	if name == "latency" || name == "bandwidth" {
		require.Greater(t, networkBacklog(during.After), networkBacklog(healthy.After)+replicatedPartitions, "toxic did not measurably increase output backlog")
		require.Greater(t, during.After.Pending.Submitted, during.After.Pending.Confirmed)
	}
	if name == "bandwidth" {
		require.Less(t, during.rate(), .75*healthy.rate(), "bandwidth toxic did not reduce actual output throughput")
	}
}
