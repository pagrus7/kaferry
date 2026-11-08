package faults

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"slices"
	"strings"
)

type containerInfo struct {
	ID              string
	Config          struct{ Labels map[string]string }
	State           struct{ Running, Paused bool }
	NetworkSettings struct {
		Networks map[string]struct {
			NetworkID string
			Aliases   []string
		}
	}
}

func Docker(ctx context.Context, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, "docker", args...).Output()
}

func (l *Lab) command(ctx context.Context, args ...string) ([]byte, error) {
	run := l.Docker
	if run == nil {
		run = Docker
	}
	return run(ctx, args...)
}

func (l *Lab) container(ctx context.Context, name, service string) (containerInfo, error) {
	var result containerInfo
	if l.Project == "" {
		return result, errors.New("fault requires an owned Compose project")
	}
	if name == service {
		data, err := l.command(ctx, "ps", "-aq", "--filter", "label=com.docker.compose.project="+l.Project,
			"--filter", "label=com.docker.compose.service="+service)
		if err != nil {
			return result, fmt.Errorf("find owned %s container: %w", service, err)
		}
		ids := strings.Fields(string(data))
		if len(ids) != 1 {
			return result, fmt.Errorf("expected exactly one owned %s container", service)
		}
		name = ids[0]
	}
	data, err := l.command(ctx, "inspect", name)
	if err != nil {
		return result, fmt.Errorf("inspect owned %s: %w", service, err)
	}
	var inspected []containerInfo
	if err = json.Unmarshal(data, &inspected); err != nil || len(inspected) != 1 {
		return result, fmt.Errorf("invalid container inspection for %s", service)
	}
	result = inspected[0]
	if result.ID == "" || result.Config.Labels["com.docker.compose.project"] != l.Project ||
		result.Config.Labels["com.docker.compose.service"] != service {
		return containerInfo{}, fmt.Errorf("container is not owned by fixture %s/%s", l.Project, service)
	}
	return result, nil
}

func (f Fault) physical() bool {
	return slices.Contains([]string{"kill", "restart", "pause", "disconnect"}, f.Kind)
}

func (f Fault) validate(role Role) error {
	if f.physical() {
		if role.Worker != 0 || role.Listener != "" || f.Stream != "" || f.Value != 0 {
			return errors.New("container faults require a whole broker role")
		}
		return nil
	}
	if f.Kind == "cut" && (f.Stream != "" || f.Value != 0) {
		return errors.New("proxy cuts do not accept toxic parameters")
	}
	if f.Kind != "cut" && !slices.Contains([]string{"timeout", "reset_peer", "latency", "bandwidth", "slow_close"}, f.Kind) {
		return errors.New("unknown fault primitive")
	}
	if f.Value < 0 || f.Kind == "bandwidth" && f.Value == 0 {
		return errors.New("toxic value must be nonnegative; bandwidth must be positive")
	}
	if f.Stream != "" && f.Stream != "upstream" && f.Stream != "downstream" {
		return errors.New("toxic stream must be upstream or downstream")
	}
	return nil
}

func (l *Lab) apply(ctx context.Context, target Target, fault Fault) (restoration, error) {
	if !fault.physical() {
		return l.proxyFault(ctx, target, fault)
	}
	owned, err := l.container(ctx, target.Container, target.Service)
	if err != nil {
		return nil, err
	}
	if !owned.State.Running || owned.State.Paused {
		return nil, errors.New("fault requires a running, unpaused broker")
	}
	if fault.Kind == "disconnect" {
		return l.disconnect(ctx, target, owned)
	}
	undo := func(heal context.Context) error { return l.restoreContainer(heal, target, fault.Kind) }
	args := []string{fault.Kind}
	if fault.Kind == "kill" {
		args = append(args, "--signal", "KILL")
	}
	if fault.Kind == "restart" {
		args = append(args, "--time", "10")
	}
	_, err = l.command(ctx, append(args, target.Container)...)
	return undo, err
}

func (l *Lab) restoreContainer(ctx context.Context, target Target, kind string) error {
	owned, err := l.container(ctx, target.Container, target.Service)
	if err != nil {
		return err
	}
	command := "start"
	if kind == "pause" {
		if !owned.State.Paused {
			return nil
		}
		command = "unpause"
	} else if owned.State.Running {
		return nil
	}
	_, err = l.command(ctx, command, target.Container)
	return err
}

func (l *Lab) disconnect(ctx context.Context, target Target, owned containerInfo) (restoration, error) {
	network, ok := owned.NetworkSettings.Networks[l.Project+"_default"]
	if !ok || network.NetworkID == "" {
		return nil, errors.New("broker is not on its owned fixture network")
	}
	data, err := l.command(ctx, "network", "inspect", "--format", `{{index .Labels "com.docker.compose.project"}}`, network.NetworkID)
	if err != nil || strings.TrimSpace(string(data)) != l.Project {
		return nil, errors.New("network is not owned by the fixture")
	}
	undo := func(heal context.Context) error {
		current, inspectErr := l.container(heal, target.Container, target.Service)
		if inspectErr != nil {
			return inspectErr
		}
		for _, connected := range current.NetworkSettings.Networks {
			if connected.NetworkID == network.NetworkID {
				return nil
			}
		}
		args := []string{"network", "connect"}
		for _, alias := range network.Aliases {
			args = append(args, "--alias", alias)
		}
		_, connectErr := l.command(heal, append(args, network.NetworkID, target.Container)...)
		return connectErr
	}
	_, err = l.command(ctx, "network", "disconnect", network.NetworkID, target.Container)
	return undo, err
}
