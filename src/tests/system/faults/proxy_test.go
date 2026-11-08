package faults

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type proxyStub struct {
	mu     sync.Mutex
	routes map[string]*proxy
	posts  []map[string]any
	calls  []string
	fail   string
}

func newProxyStub(t *testing.T) (*Lab, *proxyStub, *metadataStub) {
	t.Helper()
	p := &proxyStub{routes: make(map[string]*proxy)}
	for _, side := range []string{"source", "destination"} {
		for id := int32(1); id <= 3; id++ {
			for index, listener := range listeners {
				name := fmt.Sprintf("%s-%d-%s", side, id, listener)
				p.routes[name] = &proxy{Name: name, Listen: fmt.Sprintf("[::]:%d", routePort(side, listener, id)),
					Upstream: fmt.Sprintf("%s-%d:%d", side, id, 9092+index), Enabled: true}
			}
		}
	}
	server := httptest.NewServer(http.HandlerFunc(p.serve))
	t.Cleanup(server.Close)
	docker := newDockerStub()
	metadata := &metadataStub{coordinator: 2, part: Partition{Leader: 3, ISR: []int32{3, 2, 1}}}
	return &Lab{Project: docker.project, Docker: docker.run, Metadata: metadata, API: server.URL, HTTP: server.Client()}, p, metadata
}

func (p *proxyStub) serve(w http.ResponseWriter, r *http.Request) {
	p.mu.Lock()
	defer p.mu.Unlock()
	call := r.Method + " " + r.URL.Path
	p.calls = append(p.calls, call)
	if call == p.fail {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	current := p.routes[parts[1]]
	if current == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	var result any = current
	if r.Method == http.MethodPost {
		var value map[string]any
		_ = json.NewDecoder(r.Body).Decode(&value)
		p.posts = append(p.posts, value)
		if len(parts) == 2 {
			current.Enabled, _ = value["enabled"].(bool)
		} else {
			data, _ := json.Marshal(value)
			current.Toxics = append(current.Toxics, data)
		}
	}
	if len(parts) > 2 {
		result = current.Toxics
		if r.Method == http.MethodDelete {
			current.Toxics = withoutToxic(current.Toxics, parts[3])
			w.WriteHeader(http.StatusNoContent)
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

func withoutToxic(toxics []json.RawMessage, name string) []json.RawMessage {
	var retained []json.RawMessage
	for _, value := range toxics {
		var toxic struct{ Name string }
		_ = json.Unmarshal(value, &toxic)
		if toxic.Name != name {
			retained = append(retained, value)
		}
	}
	return retained
}

func TestWorkerCoordinatorCutHealsOriginalPathAfterOwnershipMoves(t *testing.T) {
	// Given worker 2 talking to coordinator broker 2, with a separate admin path.
	lab, proxies, metadata := newProxyStub(t)
	injection, err := lab.Inject(t.Context(), Role{Kind: "worker-coordinator", Cluster: "source", Group: "copy", Worker: 2}, Fault{Kind: "cut"})
	require.NoError(t, err)
	require.False(t, proxies.routes["source-2-worker2"].Enabled)
	require.True(t, proxies.routes["source-2-admin"].Enabled)
	require.True(t, proxies.routes["source-2-worker1"].Enabled)
	metadata.coordinator = 3
	// When ownership changes, then healing restores the exact old route without new resolution.
	require.NoError(t, injection.Heal(t.Context()))
	require.True(t, proxies.routes["source-2-worker2"].Enabled)
	require.NotContains(t, proxies.calls, "GET /proxies/source-3-worker2")
	require.Equal(t, 1, metadata.calls)
	require.Equal(t, "worker2", injection.Event.Targets[0].Listener)
}

func TestWholeClusterIncludesClientReplicationAndControllerPaths(t *testing.T) {
	// Given a destination cluster with six paths per broker.
	lab, proxies, _ := newProxyStub(t)
	injection, err := lab.Inject(t.Context(), Role{Kind: "cluster", Cluster: "destination"}, Fault{Kind: "cut"})
	require.NoError(t, err)
	require.Len(t, injection.Event.Targets, 18)
	// When cutting the cluster, then no destination path remains enabled.
	for name, route := range proxies.routes {
		require.Equal(t, strings.HasPrefix(name, "source-"), route.Enabled, name)
	}
	require.NoError(t, injection.Heal(t.Context()))
	for _, route := range proxies.routes {
		require.True(t, route.Enabled)
	}
}

func TestToxicsUseDocumentedUnitsAndRemoveOnlyTheirOwnNames(t *testing.T) {
	for kind, attribute := range map[string]string{"timeout": "timeout", "reset_peer": "timeout", "latency": "latency", "bandwidth": "rate", "slow_close": "delay"} {
		t.Run(kind, func(t *testing.T) {
			// Given a worker route, inject a directional toxic without affecting admin.
			lab, proxies, _ := newProxyStub(t)
			injection, err := lab.Inject(t.Context(), Role{Kind: "leader", Cluster: "destination", Topic: "output", Worker: 1},
				Fault{Kind: kind, Stream: "upstream", Value: 123})
			require.NoError(t, err)
			post := proxies.posts[0]
			require.Equal(t, kind, post["type"])
			require.Equal(t, "upstream", post["stream"])
			require.Equal(t, map[string]any{attribute: float64(123)}, post["attributes"])
			require.Empty(t, proxies.routes["destination-3-admin"].Toxics)
			name := post["name"].(string)
			// When removal fails, then keep cleanup pending and retry that same toxic.
			proxies.fail = "DELETE /proxies/destination-3-worker1/toxics/" + name
			require.Error(t, injection.Heal(t.Context()))
			require.True(t, injection.Event.Heal.IsZero())
			proxies.fail = ""
			require.NoError(t, injection.Heal(t.Context()))
			require.Empty(t, proxies.routes["destination-3-worker1"].Toxics)
			require.Contains(t, proxies.calls, "DELETE /proxies/destination-3-worker1/toxics/"+name)
		})
	}
}

func TestPartialPathCutUnwindsAndRejectsWrongOrAlreadyFaultedRoutes(t *testing.T) {
	// Given a three-broker worker destination path whose second mutation fails.
	lab, proxies, _ := newProxyStub(t)
	proxies.fail = "POST /proxies/destination-2-worker3"
	injection, err := lab.Inject(t.Context(), Role{Kind: "worker-destination", Cluster: "destination", Worker: 3}, Fault{Kind: "cut"})
	require.Error(t, err)
	require.True(t, proxies.routes["destination-1-worker3"].Enabled)
	require.NotContains(t, proxies.calls, "POST /proxies/destination-3-worker3")
	proxies.fail = ""
	require.NoError(t, injection.Heal(t.Context()))
	// When route ownership or preexisting fault state differs, then do not alter it.
	for _, changed := range []string{"upstream", "disabled", "toxic"} {
		t.Run(changed, func(t *testing.T) {
			lab, proxies, _ := newProxyStub(t)
			route := proxies.routes["source-3-worker1"]
			switch changed {
			case "upstream":
				route.Upstream = "unrelated:9095"
			case "disabled":
				route.Enabled = false
			case "toxic":
				route.Toxics = []json.RawMessage{json.RawMessage(`{"name":"unrelated"}`)}
			}
			_, err := lab.Inject(t.Context(), Role{Kind: "leader", Cluster: "source", Topic: "input", Worker: 1}, Fault{Kind: "cut"})
			require.Error(t, err)
			require.Empty(t, proxies.posts)
		})
	}
}

func TestInvalidPrimitivesCannotReachDockerOrProxy(t *testing.T) {
	lab, proxies, _ := newProxyStub(t)
	for _, fault := range []Fault{{Kind: "unknown"}, {Kind: "bandwidth"}, {Kind: "latency", Value: -1}, {Kind: "timeout", Stream: "both"}, {Kind: "kill"}, {Kind: "cut", Value: 1}} {
		// Given invalid parameters, when injecting, then reject before any route mutation.
		_, err := lab.Inject(t.Context(), Role{Kind: "leader", Cluster: "source", Topic: "input", Worker: 1}, fault)
		require.Error(t, err)
	}
	require.Empty(t, proxies.calls)
}

func TestHealCannotMutateAReplacementProxyContainer(t *testing.T) {
	// Given a cut on a concrete proxy container, retain that container's identity.
	lab, proxies, _ := newProxyStub(t)
	injection, err := lab.Inject(t.Context(), Role{Kind: "leader", Cluster: "source", Topic: "input", Worker: 1}, Fault{Kind: "cut"})
	require.NoError(t, err)
	require.Equal(t, "id-toxiproxy", injection.Event.Targets[0].ProxyContainer)
	original := lab.Docker
	lab.Docker = func(ctx context.Context, args ...string) ([]byte, error) {
		if args[0] == "inspect" && args[1] == "id-toxiproxy" {
			return nil, errors.New("original proxy container was removed")
		}
		return original(ctx, args...)
	}
	// When it disappears, then refuse to enable a replacement reached at the API address.
	require.ErrorContains(t, injection.Heal(t.Context()), "original proxy container")
	require.Len(t, proxies.posts, 1)
	require.True(t, injection.Event.Heal.IsZero())
	lab.Docker = original
	require.NoError(t, injection.Heal(t.Context()))
}

func TestProxyOwnershipAcceptsOnlyEquivalentBoundWildcardListeners(t *testing.T) {
	// Given the pinned API's IPv6 wildcard response, preserve exact route ownership.
	for _, test := range []struct {
		address string
		valid   bool
	}{{"[::]:46394", true}, {"0.0.0.0:46394", true}, {"[::]:46393", false},
		{"127.0.0.1:46394", false}, {"[::1]:46394", false}, {"::46394", false}, {"", false}} {
		t.Run(test.address, func(t *testing.T) {
			lab, proxies, _ := newProxyStub(t)
			proxies.routes["source-3-worker3"].Listen = test.address
			// When checking the route, then concrete, malformed or different-port binds fail.
			injection, err := lab.Inject(t.Context(), Role{Kind: "leader", Cluster: "source", Topic: "input", Worker: 3}, Fault{Kind: "cut"})
			if test.valid {
				require.NoError(t, err)
				require.NoError(t, injection.Heal(t.Context()))
			} else {
				require.ErrorContains(t, err, "does not belong")
				require.Empty(t, proxies.posts)
			}
		})
	}
}

func TestToxicDefaultsAndUncertainCreationPreserveOtherFaults(t *testing.T) {
	// Given a default-direction toxic, apply the documented downstream default.
	lab, proxies, _ := newProxyStub(t)
	lab.HTTP = nil // Exercise the production timeout-bearing client.
	role := Role{Kind: "leader", Cluster: "source", Topic: "input", Worker: 1}
	injection, err := lab.Inject(t.Context(), role, Fault{Kind: "latency", Value: 100})
	require.NoError(t, err)
	require.Equal(t, "downstream", proxies.posts[0]["stream"])
	proxies.routes["source-3-worker1"].Toxics = append(proxies.routes["source-3-worker1"].Toxics, json.RawMessage(`{"name":"another-owner"}`))
	// When healing, then remove only the injected toxic, preserving an unrelated name.
	require.NoError(t, injection.Heal(t.Context()))
	require.Len(t, proxies.routes["source-3-worker1"].Toxics, 1)
	require.JSONEq(t, `{"name":"another-owner"}`, string(proxies.routes["source-3-worker1"].Toxics[0]))
	lab, proxies, _ = newProxyStub(t)
	proxies.fail = "POST /proxies/source-3-worker1/toxics"
	injection, err = lab.Inject(t.Context(), role, Fault{Kind: "timeout"})
	require.Error(t, err)
	// A failed request may not have created the toxic. Cleanup must tolerate its absence.
	require.False(t, injection.Event.Heal.IsZero())
	require.Empty(t, proxies.routes["source-3-worker1"].Toxics)
	for _, call := range proxies.calls {
		require.False(t, strings.HasPrefix(call, "DELETE "))
	}
}

func TestToxicCleanupRetainsUnavailableStateForLaterHealing(t *testing.T) {
	// Given a toxic whose list request fails, keep its cleanup pending.
	lab, proxies, _ := newProxyStub(t)
	role := Role{Kind: "leader", Cluster: "source", Topic: "input", Worker: 1}
	injection, err := lab.Inject(t.Context(), role, Fault{Kind: "slow_close", Value: 100})
	require.NoError(t, err)
	proxies.fail = "GET /proxies/source-3-worker1/toxics"
	require.Error(t, injection.Heal(t.Context()))
	require.True(t, injection.Event.Heal.IsZero())
	proxies.fail = ""
	// When the original proxy container is stopped, then do not issue a removal to its address.
	docker := newDockerStub()
	owned := docker.containers["id-toxiproxy"]
	owned.State.Running = false
	docker.containers[owned.ID] = owned
	lab.Docker = docker.run
	require.ErrorContains(t, injection.Heal(t.Context()), "not running")
	require.True(t, injection.Event.Heal.IsZero())
	owned.State.Running = true
	docker.containers[owned.ID] = owned
	// Then restoring the same container permits the pending cleanup.
	require.NoError(t, injection.Heal(t.Context()))
	require.Empty(t, proxies.routes["source-3-worker1"].Toxics)
}

func TestUnavailableProxyAPICannotMutateRoutes(t *testing.T) {
	for _, mode := range []string{"invalid-address", "closed-server", "invalid-json"} {
		t.Run(mode, func(t *testing.T) {
			// Given an API that cannot establish route ownership.
			lab, proxies, _ := newProxyStub(t)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("invalid JSON")) }))
			t.Cleanup(server.Close)
			lab.API = server.URL
			if mode == "invalid-address" {
				lab.API = "://invalid"
			}
			if mode == "closed-server" {
				server.Close()
			}
			// When injection is attempted, then no fault is sent to the unverified route.
			_, err := lab.Inject(t.Context(), Role{Kind: "leader", Cluster: "source", Topic: "input", Worker: 1}, Fault{Kind: "cut"})
			require.Error(t, err)
			require.Empty(t, proxies.posts)
		})
	}
}
