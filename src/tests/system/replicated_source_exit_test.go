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

func brokerSourceOperationEOF(terminal string) bool {
	return terminal == "poll source: "+brokerNegotiationEOF || terminal == "save progress: "+brokerNegotiationEOF ||
		terminal == "poll source: save progress: "+brokerNegotiationEOF
}

func (p *brokerFailureProof) sourceOperationExit(logs, terminal string, summary *copySummary) bool {
	if !brokerSourceOperationEOF(terminal) || summary == nil || summary.Submitted <= 0 || summary.Submitted != summary.Confirmed {
		return false
	}
	if !p.forcedCopy(logs) || !p.affected("source") ||
		(!p.Heal.IsZero() && !p.ObservedExit.Before(p.Heal)) || !strings.Contains("\n"+logs, "\n"+terminal+"\n") ||
		!sourceContainerTargets(p.Targets) || !reflect.DeepEqual(p.Targets, p.Exit.Window.Targets) ||
		!reflect.DeepEqual(p.Timeouts, p.Exit.Window.Timeouts) || p.Role.Partition != 0 || p.Role.Worker != 0 || p.Role.Listener != "" || p.Role.Group != "" {
		return false
	}
	catalog, err := readFaultTimeoutCatalog()
	if err != nil || !reflect.DeepEqual(catalog[p.Case], p.Timeouts) {
		return false
	}
	var recovery int64
	for _, timeout := range p.Timeouts {
		recovery += timeout
	}
	return recovery == p.RecoveryMS && (p.Role.Kind != "broker" || p.Role.Broker == p.Targets[0].Broker)
}

func sourceContainerTargets(targets []faults.Target) bool {
	if len(targets) != 6 {
		return false
	}
	first := targets[0]
	if first.Broker < 1 || first.Broker > 3 || first.Container == "" || first.ProxyContainer == "" {
		return false
	}
	paths := map[string]bool{"internal": false, "controller": false, "admin": false, "worker1": false, "worker2": false, "worker3": false}
	for _, target := range targets {
		seen, valid := paths[target.Listener]
		if !valid || seen || target.Cluster != "source" || target.Broker != first.Broker || target.Container != first.Container ||
			target.ProxyContainer != first.ProxyContainer || target.Service != fmt.Sprintf("source-%d", first.Broker) ||
			target.Proxy != target.Service+"-"+target.Listener {
			return false
		}
		paths[target.Listener] = true
	}
	return true
}

func TestBrokerLabelledSourceEOFRequiresBoundAllConfirmedExit(t *testing.T) {
	t.Run("pinned protocol diagnostic", func(t *testing.T) {
		// Given the pinned plaintext SDK, when API negotiation closes without a
		// response, then the diagnostic retains its native EOF type and cause.
		cluster := kfake.MustCluster(kfake.NumBrokers(1))
		defer cluster.Close()
		cluster.ControlKey(int16(kmsg.ApiVersions), func(kmsg.Request) (kmsg.Response, error, bool) { return nil, io.EOF, true })
		client, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.RequestRetries(0))
		require.NoError(t, err)
		defer client.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		_, err = client.Request(ctx, kmsg.NewPtrMetadataRequest())
		var firstRead *kgo.ErrFirstReadEOF
		require.ErrorAs(t, err, &firstRead)
		require.ErrorIs(t, err, io.EOF)
		require.EqualError(t, err, brokerNegotiationEOF)
	})
	for _, label := range []string{"poll source: ", "save progress: ", "poll source: save progress: "} {
		t.Run(label, func(t *testing.T) {
			// Given live healthy progress from this controlled child, when a
			// registered source restart is followed by a labelled all-confirmed
			// exit, then the same binary/group restart gate can classify it.
			terminal := label + brokerNegotiationEOF
			summary := `{"msg":"copy summary","submitted":1267,"confirmed":1267,"peak_uncommitted_records":270,"peak_uncommitted_bytes":617109}` + "\n"
			process := brokerLogChild(t, summary+terminal)
			binary, err := os.ReadFile(process.command.Path)
			require.NoError(t, err)
			s := &brokerCopy{t: t, caseID: "broker/rolling-source", directory: t.TempDir(), workers: []*cliProcess{process},
				identities: []brokerLifetime{{Number: 1, PID: process.pid(), Started: time.Now().UTC().Add(-time.Second), Binary: brokerDigest(string(binary))}}}
			s.timeouts = faultTimeouts(t, s.caseID)
			s.beforeFault = brokerEvidenceProof("source").Before
			progress, prefix, found, err := s.currentProgress(process)
			require.NoError(t, err)
			require.True(t, found)
			s.beforeFault.At, s.beforeFault.Pending, s.beforeFault.Worker = time.Now().UTC(), progress, prefix
			event := faults.Event{Role: faults.Role{Kind: "broker", Cluster: "source", Broker: 1}, Fault: faults.Fault{Kind: "restart"}, Start: s.beforeFault.At.Add(time.Nanosecond)}
			for _, path := range []string{"internal", "controller", "admin", "worker1", "worker2", "worker3"} {
				event.Targets = append(event.Targets, faults.Target{Cluster: "source", Broker: 1, Listener: path, Service: "source-1", Container: "owned-source", Proxy: "source-1-" + path, ProxyContainer: "owned-proxy"})
			}
			s.active = &faults.Injection{Event: event}
			require.NoError(t, process.stdin.Close())
			require.Error(t, process.wait())
			s.active.Event.Heal = time.Now().UTC()
			proof := s.failureProof(process)
			logs, err := process.currentLogs()
			require.NoError(t, err)
			failure, err := completedBrokerFailure(process, true, proof)
			require.NoError(t, err)
			require.Equal(t, terminal, failure)
			_, err = completedBrokerFailure(process, false, proof)
			require.Error(t, err, "a no-restart caller remains fail closed")
			work, err := brokerWork(logs)
			require.NoError(t, err)
			require.Equal(t, 270, work.Summary.PeakRecords)
			require.Equal(t, int64(617109), work.Summary.PeakBytes)
			sourceExitMutations(t, proof, logs, terminal)
		})
	}
}

func sourceExitMutations(t *testing.T, proof *brokerFailureProof, logs, terminal string) {
	t.Helper()
	changedLogs := []string{strings.ReplaceAll(logs, `"confirmed":1267`, `"confirmed":1266`)}
	for _, replacement := range []string{brokerNegotiationEOF, terminal + " context deadline exceeded", terminal + "\ncontext deadline exceeded", " " + terminal, terminal + " ", "poll source: TLS handshake failed"} {
		changedLogs = append(changedLogs, strings.ReplaceAll(logs, terminal, replacement))
	}
	for _, changed := range changedLogs {
		data, err := json.Marshal(proof)
		require.NoError(t, err)
		var rebound brokerFailureProof
		require.NoError(t, json.Unmarshal(data, &rebound))
		rebound.Exit.Log.SHA256, rebound.Exit.LogBytes, rebound.Exit.Terminal = brokerDigest(changed), len(changed), brokerTerminals(changed)
		_, generic := brokerTerminalFailure(changed)
		_, contextual := brokerContextualFailure(changed, &rebound)
		require.False(t, generic)
		require.False(t, contextual)
	}
	for _, edit := range []func(*brokerFailureProof){
		func(p *brokerFailureProof) { p.Case = "broker/destination-isr-loss"; p.Kind = "cut" },
		func(p *brokerFailureProof) { p.Role.Cluster = "destination" },
		func(p *brokerFailureProof) { p.ObservedExit = p.Start },
		func(p *brokerFailureProof) { p.ObservedExit = p.Heal },
		func(p *brokerFailureProof) { p.Before.Pending.Confirmed = 0 },
		func(p *brokerFailureProof) { p.Before.Worker.Lifetime.Number++ },
		func(p *brokerFailureProof) {
			p.Exit.DriverIntent = []cliSignalIntent{{At: p.Start, Reason: "context cancellation"}}
		},
		func(p *brokerFailureProof) { p.Exit.Signal = "killed" },
		func(p *brokerFailureProof) { p.Targets = p.Targets[:5] },
		func(p *brokerFailureProof) {
			p.Targets[0].Container = "unowned"
			p.Exit.Window.Targets[0] = p.Targets[0]
		},
		func(p *brokerFailureProof) {
			p.Targets[0].Listener = "worker2"
			p.Exit.Window.Targets[0] = p.Targets[0]
		},
		func(p *brokerFailureProof) {
			p.Timeouts["shutdown_timeout"]++
			p.Exit.Window.Timeouts["shutdown_timeout"]++
		},
	} {
		data, err := json.Marshal(proof)
		require.NoError(t, err)
		var changed brokerFailureProof
		require.NoError(t, json.Unmarshal(data, &changed))
		edit(&changed)
		_, known := brokerContextualFailure(logs, &changed)
		require.False(t, known)
	}
	// Given an otherwise consistent sole-Wait witness exactly at physical heal,
	// when every copy of its exit time agrees, then only the strict boundary
	// rejects the exit; lifetime or active-window mismatch cannot mask this gate.
	data, err := json.Marshal(proof)
	require.NoError(t, err)
	var atHeal brokerFailureProof
	require.NoError(t, json.Unmarshal(data, &atHeal))
	atHeal.ObservedExit, atHeal.Exit.Stopped, atHeal.Exit.Recorded = atHeal.Heal, atHeal.Heal, atHeal.Heal
	atHeal.Exit.Window.ObservedExit = atHeal.Heal
	require.True(t, atHeal.forcedCopy(logs), "the bound lifetime and active fault remain valid")
	require.True(t, atHeal.affected("source"), "the existing inclusive fault check admits equality")
	_, known := brokerContextualFailure(logs, &atHeal)
	require.False(t, known, "labelled source EOF must precede physical healing")
}
