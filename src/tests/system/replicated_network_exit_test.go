//go:build integration && fault && load

package compat

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/src/tests/system/faults"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const networkFirstResponseEOF = "broker closed the connection immediately after a request was issued, which often happens when SASL is required but not provided: is SASL missing?"

// Only the first lifetime of the complete owned destination cut has this scope.
// Callers separately require an exact known diagnostic and its work evidence.
func (p *brokerFailureProof) networkOutageExit(logs string) bool {
	if p == nil || p.Case != "network/destination-outage" || p.Window != 0 || p.Kind != "cut" || p.Transport != "plaintext" ||
		p.Role != (faults.Role{Kind: "worker-destination", Cluster: "destination", Worker: 1}) ||
		!brokerHealthyCopy(p.Before) || len(p.Before.Ends) != replicatedPartitions || !p.Before.At.Before(p.Start) || !p.Start.Before(p.ObservedExit) ||
		!p.Exit.binds(logs, p.Before) || p.Exit.Lifetime.Number != 1 || !p.Exit.Stopped.Equal(p.ObservedExit) ||
		(!p.Heal.IsZero() && !p.ObservedExit.Before(p.Heal)) || !networkOutageTargets(p.Targets) {
		return false
	}
	for partition, end := range p.Before.Ends {
		if p.Before.Saved[partition] > end {
			return false
		}
	}
	catalog, err := readFaultTimeoutCatalog()
	if err != nil || !reflect.DeepEqual(catalog[p.Case], p.Timeouts) {
		return false
	}
	var recovery int64
	for _, milliseconds := range p.Timeouts {
		recovery += milliseconds
	}
	rss, active := p.Exit.RSS, p.Exit.Window
	return recovery == p.RecoveryMS && rss != nil && rss.Samples > 0 && rss.Failures == 0 && rss.Peak > 0 && rss.Peak <= 512<<20 &&
		active != nil && active.Case == p.Case && active.Window == p.Window && active.Role == p.Role && active.Kind == p.Kind &&
		active.RecoveryMS == p.RecoveryMS && active.Start.Equal(p.Start) && active.ObservedExit.Equal(p.ObservedExit) &&
		reflect.DeepEqual(active.Before, p.Before) && reflect.DeepEqual(active.Targets, p.Targets) && reflect.DeepEqual(active.Timeouts, p.Timeouts) &&
		(active.Heal.IsZero() || active.Heal.Equal(p.Heal))
}

func (p *brokerFailureProof) negotiationEOFContext(logs string, failures int, diagnostic string) bool {
	if p != nil && strings.HasPrefix(p.Case, "network/") {
		if diagnostic != brokerNegotiationEOF && diagnostic != networkFirstResponseEOF {
			return false
		}
		exact := 0
		for _, line := range strings.Split(logs, "\n") {
			if line == diagnostic {
				exact++
			}
		}
		return p.networkOutageExit(logs) && exact == failures
	}
	return diagnostic == brokerNegotiationEOF && p.affected("destination")
}

func networkOutageTargets(targets []faults.Target) bool {
	if len(targets) != 3 {
		return false
	}
	brokers, containers := make(map[int32]bool), make(map[string]bool)
	for _, target := range targets {
		if target.Cluster != "destination" || target.Broker < 1 || target.Broker > 3 || brokers[target.Broker] ||
			target.Listener != "worker1" || target.Service != fmt.Sprintf("destination-%d", target.Broker) ||
			target.Proxy != fmt.Sprintf("destination-%d-worker1", target.Broker) || target.Container == "" || containers[target.Container] ||
			target.ProxyContainer == "" || target.ProxyContainer != targets[0].ProxyContainer {
			return false
		}
		brokers[target.Broker], containers[target.Container] = true, true
	}
	return true
}

func networkOutageChild(t *testing.T, tail string) (*brokerCopy, *cliProcess) {
	t.Helper()
	process := brokerLogChild(t, tail)
	binary, err := os.ReadFile(process.command.Path)
	require.NoError(t, err)
	s := &brokerCopy{t: t, caseID: "network/destination-outage", directory: t.TempDir(), workers: []*cliProcess{process},
		identities: []brokerLifetime{{Number: 1, PID: process.pid(), Started: time.Now().UTC().Add(-time.Second), Binary: brokerDigest(string(binary))}},
		memories:   []func() rssPeak{func() rssPeak { return rssPeak{peak: 4096, samples: 2} }}}
	s.timeouts = faultTimeouts(t, s.caseID)
	s.beforeFault = brokerEvidenceProof("destination").Before
	s.beforeFault.Ends = append([]int64(nil), s.beforeFault.Saved...)
	progress, prefix, found, err := s.currentProgress(process)
	require.NoError(t, err)
	require.True(t, found)
	s.beforeFault.At, s.beforeFault.Pending, s.beforeFault.Worker = time.Now().UTC(), progress, prefix
	event := faults.Event{Role: faults.Role{Kind: "worker-destination", Cluster: "destination", Worker: 1},
		Fault: faults.Fault{Kind: "cut"}, Start: s.beforeFault.At.Add(time.Nanosecond)}
	for broker := int32(1); broker <= 3; broker++ {
		event.Targets = append(event.Targets, faults.Target{Cluster: "destination", Broker: broker, Listener: "worker1",
			Service: fmt.Sprintf("destination-%d", broker), Container: fmt.Sprint(broker), Proxy: fmt.Sprintf("destination-%d-worker1", broker), ProxyContainer: "owned-proxy"})
	}
	s.active = &faults.Injection{Event: event}
	return s, process
}

func TestNetworkOutageForcedExitBindsOwnedCutAndRetainedRSS(t *testing.T) {
	for _, terminal := range []string{brokerForcedWritesExit, brokerForcedSaveExit, "unexpected invariant failure"} {
		t.Run(terminal, func(t *testing.T) {
			// Given an actual controlled child lifetime, retain its live progress,
			// sampled memory and frozen all-destination cut before classification.
			s, process := networkOutageChild(t, terminal)
			require.NoError(t, process.stdin.Close())
			require.Error(t, process.wait())
			proof := s.failureProof(process)
			logs, err := process.currentLogs()
			require.NoError(t, err)
			// When only the documented hard-exit phase is present, accept the
			// recorded restart while leaving exact lifetime peaks unavailable.
			require.Equal(t, &brokerExitRSS{Samples: 2, Peak: 4 << 20}, proof.Exit.RSS)
			data, err := os.ReadFile(s.directory + "/" + s.exits[process].Reference.Path)
			require.NoError(t, err)
			require.Contains(t, string(data), `"rss"`)
			failure, err := completedBrokerFailure(process, true, proof)
			if brokerForcedCopyPhase(terminal) == "" {
				require.Error(t, err, "unsupported exits still retain RSS before rejection")
				return
			}
			require.NoError(t, err)
			require.Equal(t, terminal, failure)
			// Then rehashed or internally consistent scope/timing alterations
			// cannot turn other cases, signals or missing evidence into this exit.
			networkOutageProofMutations(t, proof, logs)
			for _, input := range []string{logs + "unexpected failure\n", strings.ReplaceAll(logs, terminal, "authentication failed"), brokerWorkProgress + brokerWorkSummary + terminal + "\n"} {
				_, accepted := brokerContextualFailure(input, proof)
				require.False(t, accepted)
			}
		})
	}
}

func networkOutageProofMutations(t *testing.T, proof *brokerFailureProof, logs string) {
	t.Helper()
	mutations := []func(*brokerFailureProof){
		func(p *brokerFailureProof) { p.Case = "network/latency" },
		func(p *brokerFailureProof) { p.Case = "broker/destination-leader" },
		func(p *brokerFailureProof) { p.Role.Cluster = "source" },
		func(p *brokerFailureProof) { p.Role.Worker = 2 },
		func(p *brokerFailureProof) { p.Kind = "kill" },
		func(p *brokerFailureProof) { p.Window = 1 },
		func(p *brokerFailureProof) { p.Exit.Window = nil },
		func(p *brokerFailureProof) { p.Targets = p.Targets[:2] },
		func(p *brokerFailureProof) { p.Targets[0].Listener = "admin"; p.Exit.Window.Targets[0] = p.Targets[0] },
		func(p *brokerFailureProof) {
			p.Timeouts["shutdown_timeout"]++
			p.Exit.Window.Timeouts["shutdown_timeout"]++
		},
		func(p *brokerFailureProof) { p.Exit.DriverIntent = []cliSignalIntent{{Reason: "context cancellation"}} },
		func(p *brokerFailureProof) { p.Exit.Signal = "killed" },
		func(p *brokerFailureProof) { p.Exit.WaitError = "" },
		func(p *brokerFailureProof) { p.Start = p.ObservedExit.Add(time.Second) },
		func(p *brokerFailureProof) { p.Heal = p.ObservedExit.Add(-time.Second) },
		func(p *brokerFailureProof) { p.Exit.Lifetime.Number = 2 },
		func(p *brokerFailureProof) { p.Exit.Lifetime.Binary = strings.Repeat("f", 64) },
		func(p *brokerFailureProof) { p.Before.Worker.SHA256 = strings.Repeat("0", 64) },
		func(p *brokerFailureProof) { p.Before.Ends[0] = 0; p.Exit.Window.Before.Ends[0] = 0 },
		func(p *brokerFailureProof) { p.Exit.RSS = nil },
		func(p *brokerFailureProof) { p.Exit.RSS.Samples = 0 },
		func(p *brokerFailureProof) { p.Exit.RSS.Failures = 1 },
		func(p *brokerFailureProof) { p.Exit.RSS.Peak = 512<<20 + 1 },
		func(p *brokerFailureProof) { p.Exit.Window.Targets[0].Container = "another-container" },
	}
	for index, mutate := range mutations {
		data, err := json.Marshal(proof)
		require.NoError(t, err)
		var changed brokerFailureProof
		require.NoError(t, json.Unmarshal(data, &changed))
		mutate(&changed)
		_, accepted := brokerContextualFailure(logs, &changed)
		require.False(t, accepted, "mutation %d", index)
	}
}

func TestNetworkOutageNegotiationEOFRequiresCompleteBoundWriteFailures(t *testing.T) {
	t.Run("pinned first response diagnostic", func(t *testing.T) {
		// Given successful API negotiation, when the first metadata response
		// closes, then the pinned SDK reports its typed first-read EOF guess.
		cluster := kfake.MustCluster(kfake.NumBrokers(1))
		defer cluster.Close()
		cluster.ControlKey(int16(kmsg.Metadata), func(kmsg.Request) (kmsg.Response, error, bool) { return nil, io.EOF, true })
		client, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.RequestRetries(0))
		require.NoError(t, err)
		defer client.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		_, err = client.Request(ctx, kmsg.NewPtrMetadataRequest())
		var firstRead *kgo.ErrFirstReadEOF
		require.ErrorAs(t, err, &firstRead)
		require.ErrorIs(t, err, io.EOF)
		require.EqualError(t, err, networkFirstResponseEOF)
	})
	for _, tc := range []struct {
		diagnostic, other                 string
		submitted, confirmed, peak, bytes int
	}{
		{brokerNegotiationEOF, networkFirstResponseEOF, 4416, 4344, 415, 948515},
		{networkFirstResponseEOF, brokerNegotiationEOF, 5864, 5832, 401, 916518},
	} {
		t.Run(tc.diagnostic, func(t *testing.T) {
			// Given retained unconfirmed writes and exact final peaks,
			// reproduce a real child exit one second after the owned cut begins.
			summary := fmt.Sprintf(`{"msg":"copy summary","submitted":%d,"confirmed":%d,"peak_uncommitted_records":%d,"peak_uncommitted_bytes":%d}`+"\n", tc.submitted, tc.confirmed, tc.peak, tc.bytes)
			s, process := networkOutageChild(t, summary+strings.Repeat(tc.diagnostic+"\n", tc.submitted-tc.confirmed))
			time.Sleep(time.Second)
			require.NoError(t, process.stdin.Close())
			require.Error(t, process.wait())
			proof := s.failureProof(process)
			logs, err := process.currentLogs()
			require.NoError(t, err)
			// When the same strict whole-cut policy handles either post-injection wait,
			// accept exactly the known per-write failure, retaining complete exact peaks.
			terminal, err := completedBrokerFailure(process, true, proof)
			require.NoError(t, err)
			require.Equal(t, strings.TrimSuffix(strings.Repeat(tc.diagnostic+"\n", tc.submitted-tc.confirmed), "\n"), terminal)
			_, err = completedBrokerFailure(process, false, proof)
			require.Error(t, err, "pre-fault, other-case and replacement waits still reject exits")
			work, err := brokerWork(logs)
			require.NoError(t, err)
			require.NotNil(t, work.Summary)
			require.Equal(t, tc.peak, work.Summary.PeakRecords)
			require.EqualValues(t, tc.bytes, work.Summary.PeakBytes)
			// Then before-cut, no-cut, other-case, RSS and identity mutations still fail;
			// rehashing a missing/unknown/whitespace terminal cannot replace a failed write.
			networkOutageProofMutations(t, proof, logs)
			for _, changedLogs := range []string{
				strings.Replace(logs, tc.diagnostic+"\n", "", 1),
				strings.Replace(logs, tc.diagnostic, "authentication failed", 1),
				strings.Replace(logs, tc.diagnostic, tc.other, 1),
				strings.Replace(logs, tc.diagnostic, " "+tc.diagnostic, 1),
				strings.Replace(logs, tc.diagnostic, tc.diagnostic+" EOF", 1),
				strings.Replace(logs, tc.diagnostic, brokerNegotiationEOF+networkFirstResponseEOF, 1),
				strings.Replace(logs, summary, "", 1),
			} {
				data, err := json.Marshal(proof)
				require.NoError(t, err)
				var changed brokerFailureProof
				require.NoError(t, json.Unmarshal(data, &changed))
				changed.Exit.Log.SHA256, changed.Exit.LogBytes, changed.Exit.Terminal = brokerDigest(changedLogs), len(changedLogs), brokerTerminals(changedLogs)
				_, accepted := brokerContextualFailure(changedLogs, &changed)
				require.False(t, accepted)
			}
		})
	}
}
