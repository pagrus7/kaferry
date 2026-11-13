//go:build harness

package faults

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

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

func TestRollingBrokerRoleVisitsOnlyTheSelectedOwnedBroker(t *testing.T) {
	// Given explicit logical IDs for a rolling restart in the owned fixture.
	docker := newDockerStub()
	lab := &Lab{Project: docker.project, Docker: docker.run}
	for broker := int32(1); broker <= 3; broker++ {
		// When resolved, then every route belongs to exactly that broker. No
		// metadata role can silently select a previously restarted broker again.
		targets, err := lab.Resolve(t.Context(), Role{Kind: "broker", Cluster: "source", Broker: broker})
		require.NoError(t, err)
		require.Len(t, targets, 6)
		for _, target := range targets {
			require.Equal(t, broker, target.Broker)
			require.Equal(t, fmt.Sprintf("source-%d", broker), target.Service)
		}
	}
	for _, broker := range []int32{0, 4} {
		_, err := lab.Resolve(t.Context(), Role{Kind: "broker", Cluster: "source", Broker: broker})
		require.Error(t, err)
	}
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
		{Kind: "worker-destination", Cluster: "source", Worker: 1}, {Kind: "coordinator", Cluster: "source"},
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

func TestRoleResolutionRequiresCurrentPartitionState(t *testing.T) {
	// Given metadata that cannot establish a current leader, never inspect or mutate a broker.
	metadata := &metadataStub{err: errors.New("metadata unavailable")}
	docker := newDockerStub()
	lab := &Lab{Project: docker.project, Metadata: metadata, Docker: docker.run}
	role := Role{Kind: "leader", Cluster: "source", Topic: "input"}
	_, err := lab.Resolve(t.Context(), role)
	require.ErrorContains(t, err, "metadata unavailable")
	metadata.err = nil
	metadata.part.Leader = -1
	_, err = lab.Resolve(t.Context(), role)
	require.ErrorContains(t, err, "no fixture leader")
	require.Empty(t, docker.calls)
}

func TestContainerInspectionRejectsMissingAmbiguousAndMalformedOwnership(t *testing.T) {
	for _, mode := range []string{"missing-project", "lookup-failure", "ambiguous", "malformed", "missing-broker"} {
		t.Run(mode, func(t *testing.T) {
			// Given an unavailable or ambiguous ownership lookup.
			docker := newDockerStub()
			lab := &Lab{Project: docker.project, Docker: func(ctx context.Context, args ...string) ([]byte, error) {
				return failedInspection(ctx, docker, mode, args)
			}}
			if mode == "missing-project" {
				lab.Project = ""
			}
			// When injection is requested, then fail closed before a container mutation.
			_, err := lab.Inject(t.Context(), Role{Kind: "cluster", Cluster: "source"}, Fault{Kind: "kill"})
			require.Error(t, err)
			for _, call := range docker.calls {
				require.Contains(t, []string{"ps", "inspect"}, call[0])
			}
		})
	}
}

func TestPhysicalFaultChecksStateAgainImmediatelyBeforeMutation(t *testing.T) {
	for _, mode := range []string{"stopped", "paused", "removed"} {
		t.Run(mode, func(t *testing.T) {
			// Given a broker whose state changes after resolving its role.
			docker := newDockerStub()
			lab := &Lab{Project: docker.project, Metadata: &metadataStub{coordinator: 2}}
			lab.Docker = changeOnSecondInspection(docker, mode)
			// When injecting, then refuse the changed target and never send kill.
			_, err := lab.Inject(t.Context(), Role{Kind: "coordinator", Cluster: "source", Group: "copy"}, Fault{Kind: "kill"})
			require.Error(t, err)
			for _, call := range docker.calls {
				require.NotEqual(t, "kill", call[0])
			}
		})
	}
}

func TestRestartUsesOriginalContainerAndHealingDoesNotRestartHealthyBroker(t *testing.T) {
	// Given a healthy coordinator, restart its concrete container once.
	docker := newDockerStub()
	lab := &Lab{Project: docker.project, Metadata: &metadataStub{coordinator: 2}, Docker: docker.run}
	injection, err := lab.Inject(t.Context(), Role{Kind: "coordinator", Cluster: "source", Group: "copy"}, Fault{Kind: "restart"})
	require.NoError(t, err)
	require.Contains(t, docker.calls, []string{"restart", "--time", "10", "id-source-2"})
	// When healing an already running broker, then do not issue a second start.
	require.NoError(t, injection.Heal(t.Context()))
	for _, call := range docker.calls {
		require.NotEqual(t, "start", call[0])
	}
}

func TestKilledBrokerCleanupWaitsForTheOriginalContainer(t *testing.T) {
	// Given an injected kill, do not start a replacement if the original ID is unavailable.
	docker := newDockerStub()
	lab := &Lab{Project: docker.project, Metadata: &metadataStub{coordinator: 2}, Docker: docker.run}
	injection, err := lab.Inject(t.Context(), Role{Kind: "coordinator", Cluster: "source", Group: "copy"}, Fault{Kind: "kill"})
	require.NoError(t, err)
	docker.fail = "inspect id-source-2"
	require.Error(t, injection.Heal(t.Context()))
	require.True(t, injection.Event.Heal.IsZero())
	require.NotContains(t, docker.calls, []string{"start", "id-source-2"})
	// When the original target becomes available, then its pending restore succeeds.
	docker.fail = ""
	require.NoError(t, injection.Heal(t.Context()))
	require.Contains(t, docker.calls, []string{"start", "id-source-2"})
}

func TestNetworkCleanupHandlesNoDetachMissingNetworkAndInspectionFailure(t *testing.T) {
	for _, mode := range []string{"no-detach", "missing-network", "inspect-failure"} {
		t.Run(mode, func(t *testing.T) {
			// Given an original coordinator on its owned network.
			docker := newDockerStub()
			lab := &Lab{Project: docker.project, Metadata: &metadataStub{coordinator: 2}, Docker: docker.run}
			role := Role{Kind: "coordinator", Cluster: "source", Group: "copy"}
			if mode == "no-detach" {
				docker.fail = "network disconnect owned-network id-source-2"
			}
			if mode == "missing-network" {
				owned := docker.containers["id-source-2"]
				owned.NetworkSettings.Networks = nil
				docker.containers[owned.ID] = owned
			}
			injection, err := lab.Inject(t.Context(), role, Fault{Kind: "disconnect"})
			if mode != "inspect-failure" {
				require.Error(t, err)
				for _, call := range docker.calls {
					require.False(t, len(call) > 1 && call[0] == "network" && call[1] == "connect")
				}
				return
			}
			require.NoError(t, err)
			// When healing cannot inspect the old container, then retain cleanup until it can.
			docker.fail = "inspect id-source-2"
			require.Error(t, injection.Heal(t.Context()))
			require.True(t, injection.Event.Heal.IsZero())
			docker.fail = ""
			require.NoError(t, injection.Heal(t.Context()))
		})
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
