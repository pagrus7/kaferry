package faults

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type metadataStub struct {
	part        Partition
	coordinator int32
	err         error
	calls       int
}

func (m *metadataStub) Partition(context.Context, string, string, int32) (Partition, error) {
	m.calls++
	return m.part, m.err
}

func (m *metadataStub) Coordinator(context.Context, string, string) (int32, error) {
	m.calls++
	return m.coordinator, m.err
}

type dockerStub struct {
	project    string
	calls      [][]string
	containers map[string]containerInfo
	fail       string
	foreign    bool
	foreignNet bool
}

func newDockerStub() *dockerStub {
	d := &dockerStub{project: "kaferry-replicated", containers: make(map[string]containerInfo)}
	for _, side := range []string{"source", "destination"} {
		for id := 1; id <= 3; id++ {
			d.add(fmt.Sprintf("%s-%d", side, id))
		}
	}
	d.add("toxiproxy")
	return d
}

func (d *dockerStub) add(service string) {
	var info containerInfo
	info.ID = "id-" + service
	info.Config.Labels = map[string]string{"com.docker.compose.project": d.project, "com.docker.compose.service": service}
	info.State.Running = true
	data := []byte(`{"NetworkSettings":{"Networks":{"kaferry-replicated_default":{"NetworkID":"owned-network","Aliases":["` + service + `","original-alias"]}}}}`)
	_ = json.Unmarshal(data, &info)
	d.containers[info.ID] = info
}

func (d *dockerStub) run(_ context.Context, args ...string) ([]byte, error) {
	d.calls = append(d.calls, slices.Clone(args))
	if d.fail != "" && strings.Join(args, " ") == d.fail {
		return nil, errors.New("injected Docker failure")
	}
	if args[0] == "ps" {
		service := strings.TrimPrefix(args[len(args)-1], "label=com.docker.compose.service=")
		return []byte("id-" + service), nil
	}
	if args[0] == "network" {
		return d.network(args)
	}
	id := args[len(args)-1]
	info, exists := d.containers[id]
	if !exists {
		return nil, errors.New("missing container")
	}
	if args[0] == "inspect" {
		if d.foreign {
			info.Config.Labels = map[string]string{"com.docker.compose.project": "unrelated"}
		}
		return json.Marshal([]containerInfo{info})
	}
	switch args[0] {
	case "kill":
		info.State.Running = false
	case "start":
		info.State.Running = true
	case "pause":
		info.State.Paused = true
	case "unpause":
		info.State.Paused = false
	}
	d.containers[id] = info
	return nil, nil
}

func (d *dockerStub) network(args []string) ([]byte, error) {
	if args[1] == "inspect" {
		if d.foreignNet {
			return []byte("other-project"), nil
		}
		return []byte(d.project), nil
	}
	id := args[len(args)-1]
	info := d.containers[id]
	if args[1] == "disconnect" {
		clear(info.NetworkSettings.Networks)
	}
	d.containers[id] = info
	return nil, nil
}

func TestRolesResolveCurrentStateAndHealOriginalContainers(t *testing.T) {
	// Given leader 2 and an unsorted ISR containing one leader and two followers.
	metadata := &metadataStub{part: Partition{Leader: 2, ISR: []int32{3, 2, 1}}, coordinator: 3}
	docker := newDockerStub()
	lab := &Lab{Project: docker.project, Metadata: metadata, Docker: docker.run}
	ctx := t.Context()
	targets, err := lab.Resolve(ctx, Role{Kind: "isr-follower", Cluster: "source", Topic: "input", Partition: 7, Listener: "internal"})
	require.NoError(t, err)
	require.Len(t, targets, 1)
	require.Equal(t, int32(1), targets[0].Broker)
	require.Equal(t, "source-1-internal", targets[0].Proxy)
	// When the leader moves before injection, resolve its new role once.
	metadata.part.Leader = 3
	injection, err := lab.Inject(ctx, Role{Kind: "leader", Cluster: "source", Topic: "input", Partition: 7}, Fault{Kind: "kill"})
	require.NoError(t, err)
	require.Len(t, injection.Event.Targets, 6)
	require.Contains(t, docker.calls, []string{"kill", "--signal", "KILL", "id-source-3"})
	metadata.part.Leader = 1
	// Then healing starts the originally killed container, without consulting metadata.
	require.NoError(t, injection.Heal(ctx))
	require.NoError(t, injection.Heal(ctx))
	require.Equal(t, 2, metadata.calls)
	require.False(t, injection.Event.Heal.IsZero())
	require.Contains(t, docker.calls, []string{"start", "id-source-3"})
	require.NotContains(t, docker.calls, []string{"start", "id-source-1"})
	starts := 0
	for _, call := range docker.calls {
		if call[0] == "start" {
			starts++
		}
	}
	require.Equal(t, 1, starts)
}

func TestRolesRejectMissingStateAndForeignOwnership(t *testing.T) {
	// Given invalid roles, never mutate or inspect an unrelated fixture.
	docker := newDockerStub()
	metadata := &metadataStub{part: Partition{Leader: 2, ISR: []int32{2}}, coordinator: 4}
	lab := &Lab{Project: docker.project, Metadata: metadata, Docker: docker.run}
	for _, role := range []Role{
		{Kind: "cluster", Cluster: "other"}, {Kind: "cluster", Cluster: "source", Worker: 4},
		{Kind: "leader", Cluster: "source"}, {Kind: "worker-coordinator", Cluster: "source", Group: "copy"},
		{Kind: "cluster", Cluster: "source", Listener: "direct"}, {Kind: "unknown", Cluster: "source"},
		{Kind: "isr-follower", Cluster: "source", Topic: "input"}, {Kind: "coordinator", Cluster: "source", Group: "copy"},
	} {
		_, err := lab.Resolve(t.Context(), role)
		require.Error(t, err, "%+v", role)
	}
	require.Empty(t, docker.calls)
	// When labels disagree with the project, then every physical primitive is refused.
	docker.foreign = true
	for _, kind := range []string{"kill", "restart", "pause", "disconnect"} {
		_, err := lab.Inject(t.Context(), Role{Kind: "cluster", Cluster: "destination"}, Fault{Kind: kind})
		require.ErrorContains(t, err, "not owned")
	}
	for _, call := range docker.calls {
		require.Contains(t, []string{"ps", "inspect"}, call[0])
	}
}

func TestPartialPhysicalFaultRestoresEarlierTargetsAndRetainsFailedCleanup(t *testing.T) {
	// Given a cluster pause whose second target fails.
	docker := newDockerStub()
	docker.fail = "pause id-source-2"
	lab := &Lab{Project: docker.project, Docker: docker.run}
	injection, err := lab.Inject(t.Context(), Role{Kind: "cluster", Cluster: "source"}, Fault{Kind: "pause"})
	require.Error(t, err)
	require.NotNil(t, injection, "partial failures must return their cleanup handle")
	require.False(t, docker.containers["id-source-1"].State.Paused)
	require.Contains(t, docker.calls, []string{"unpause", "id-source-1"})
	// When healing fails, retain that restore and retry only it on the next cleanup.
	docker.fail = ""
	injection, err = lab.Inject(t.Context(), Role{Kind: "cluster", Cluster: "source"}, Fault{Kind: "pause"})
	require.NoError(t, err)
	docker.fail = "unpause id-source-2"
	require.Error(t, injection.Heal(t.Context()))
	require.True(t, injection.Event.Heal.IsZero())
	require.True(t, docker.containers["id-source-2"].State.Paused)
	require.False(t, docker.containers["id-source-1"].State.Paused)
	count := len(docker.calls)
	docker.fail = ""
	require.NoError(t, injection.Heal(t.Context()))
	require.Equal(t, [][]string{{"inspect", "id-source-2"}, {"unpause", "id-source-2"}}, docker.calls[count:])
}

func TestDisconnectRestoresOwnedNetworkAndOriginalAliases(t *testing.T) {
	// Given an owned broker attached to the fixture network with DNS aliases.
	docker := newDockerStub()
	metadata := &metadataStub{coordinator: 2}
	lab := &Lab{Project: docker.project, Metadata: metadata, Docker: docker.run}
	injection, err := lab.Inject(t.Context(), Role{Kind: "coordinator", Cluster: "source", Group: "copy"}, Fault{Kind: "disconnect"})
	require.NoError(t, err)
	metadata.coordinator = 3
	// When its coordinator moves, then healing reconnects the original container and aliases.
	require.NoError(t, injection.Heal(t.Context()))
	require.Contains(t, docker.calls, []string{"network", "connect", "--alias", "source-2", "--alias", "original-alias", "owned-network", "id-source-2"})
	require.Equal(t, 1, metadata.calls)
	docker = newDockerStub()
	docker.foreignNet = true
	lab.Docker = docker.run
	_, err = lab.Inject(t.Context(), Role{Kind: "coordinator", Cluster: "source", Group: "copy"}, Fault{Kind: "disconnect"})
	require.ErrorContains(t, err, "network is not owned")
	for _, call := range docker.calls {
		require.NotEqual(t, []string{"network", "disconnect"}, call[:min(2, len(call))])
	}
}
