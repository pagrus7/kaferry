package faults

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
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
