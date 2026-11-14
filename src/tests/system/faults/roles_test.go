package faults

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
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

func failedInspection(ctx context.Context, docker *dockerStub, mode string, args []string) ([]byte, error) {
	if args[0] == "ps" && mode == "lookup-failure" {
		return nil, errors.New("daemon unavailable")
	}
	if args[0] == "ps" && mode == "ambiguous" {
		return []byte("first-id second-id"), nil
	}
	if args[0] == "inspect" && mode == "malformed" {
		return []byte("not JSON"), nil
	}
	if args[0] == "inspect" && mode == "missing-broker" && args[1] == "id-source-1" {
		return nil, errors.New("broker container removed")
	}
	return docker.run(ctx, args...)
}

func changeOnSecondInspection(docker *dockerStub, mode string) Command {
	inspections := 0
	return func(ctx context.Context, args ...string) ([]byte, error) {
		if args[0] != "inspect" || args[1] != "id-source-2" {
			return docker.run(ctx, args...)
		}
		inspections++
		if inspections != 2 {
			return docker.run(ctx, args...)
		}
		if mode == "removed" {
			return nil, errors.New("original container removed")
		}
		owned := docker.containers[args[1]]
		owned.State.Running = mode != "stopped"
		owned.State.Paused = mode == "paused"
		docker.containers[args[1]] = owned
		return docker.run(ctx, args...)
	}
}
