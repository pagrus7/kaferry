// Package faults injects reversible faults into the owned replicated lab.
// The qualification coordinator must hold the host lease for the entire run.
package faults

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"time"
)

// Role identifies live broker roles. An empty Listener selects every listener;
// Worker selects only that worker's client route (1 through 3).
type Role struct {
	Kind      string `json:"kind"`
	Cluster   string `json:"cluster"`
	Topic     string `json:"topic,omitempty"`
	Partition int32  `json:"partition"`
	Group     string `json:"group,omitempty"`
	Worker    int    `json:"worker,omitempty"`
	Listener  string `json:"listener,omitempty"`
	Broker    int32  `json:"broker,omitempty"`
}

type Partition struct {
	Leader int32
	ISR    []int32
}

type Metadata interface {
	Partition(context.Context, string, string, int32) (Partition, error)
	Coordinator(context.Context, string, string) (int32, error)
}

type Command func(context.Context, ...string) ([]byte, error)

type Lab struct {
	Project  string
	API      string
	Metadata Metadata
	Docker   Command
	HTTP     *http.Client
}

// Target is the concrete route resolved at injection time. It is retained even
// if the leader or coordinator moves before healing.
type Target struct {
	Cluster        string `json:"cluster"`
	Broker         int32  `json:"broker"`
	Listener       string `json:"listener"`
	Service        string `json:"service"`
	Container      string `json:"container"`
	Proxy          string `json:"proxy"`
	ProxyContainer string `json:"proxy_container"`
}

var listeners = []string{"internal", "controller", "admin", "worker1", "worker2", "worker3"}

func (l *Lab) Resolve(ctx context.Context, role Role) ([]Target, error) {
	paths, err := role.paths()
	if err != nil {
		return nil, err
	}
	brokers, err := l.brokers(ctx, role)
	if err != nil {
		return nil, err
	}
	for _, broker := range brokers {
		if broker < 1 || broker > 3 {
			return nil, fmt.Errorf("role resolved outside the three-broker fixture: %d", broker)
		}
	}
	proxyContainer, err := l.container(ctx, "toxiproxy", "toxiproxy")
	if err != nil {
		return nil, err
	}
	var targets []Target
	for _, broker := range brokers {
		service := fmt.Sprintf("%s-%d", role.Cluster, broker)
		owned, inspectErr := l.container(ctx, service, service)
		if inspectErr != nil {
			return nil, inspectErr
		}
		for _, path := range paths {
			targets = append(targets, Target{role.Cluster, broker, path, service, owned.ID, service + "-" + path, proxyContainer.ID})
		}
	}
	return targets, nil
}

func (r Role) paths() ([]string, error) {
	if r.Cluster != "source" && r.Cluster != "destination" {
		return nil, errors.New("fault cluster must be source or destination")
	}
	if r.Worker < 0 || r.Worker > 3 || r.Worker != 0 && r.Listener != "" {
		return nil, errors.New("select one listener or worker 1 through 3")
	}
	if (r.Kind == "worker-destination" || r.Kind == "worker-coordinator") && r.Worker == 0 {
		return nil, errors.New("worker path requires worker 1 through 3")
	}
	if r.Worker != 0 {
		return []string{fmt.Sprintf("worker%d", r.Worker)}, nil
	}
	if r.Listener == "" {
		return slices.Clone(listeners), nil
	}
	if !slices.Contains(listeners, r.Listener) {
		return nil, errors.New("unknown fixture listener")
	}
	return []string{r.Listener}, nil
}

func (l *Lab) brokers(ctx context.Context, role Role) ([]int32, error) {
	switch role.Kind {
	case "broker":
		if role.Broker < 1 || role.Broker > 3 {
			return nil, errors.New("broker role requires broker 1 through 3")
		}
		return []int32{role.Broker}, nil
	case "cluster":
		return []int32{1, 2, 3}, nil
	case "worker-destination":
		if role.Cluster != "destination" {
			return nil, errors.New("worker-destination requires destination cluster")
		}
		return []int32{1, 2, 3}, nil
	case "coordinator", "worker-coordinator":
		if role.Group == "" || l.Metadata == nil {
			return nil, errors.New("coordinator requires group and metadata")
		}
		id, err := l.Metadata.Coordinator(ctx, role.Cluster, role.Group)
		return []int32{id}, err
	case "leader", "isr-follower":
		return l.partitionBroker(ctx, role)
	default:
		return nil, errors.New("unknown fault role")
	}
}

func (l *Lab) partitionBroker(ctx context.Context, role Role) ([]int32, error) {
	if role.Topic == "" || role.Partition < 0 || l.Metadata == nil {
		return nil, errors.New("partition role requires topic, partition and metadata")
	}
	part, err := l.Metadata.Partition(ctx, role.Cluster, role.Topic, role.Partition)
	if err != nil {
		return nil, err
	}
	if part.Leader < 1 || part.Leader > 3 {
		return nil, errors.New("partition has no fixture leader")
	}
	if role.Kind == "leader" {
		return []int32{part.Leader}, nil
	}
	followers := slices.Clone(part.ISR)
	slices.Sort(followers)
	for _, id := range followers {
		if id != part.Leader {
			return []int32{id}, nil
		}
	}
	return nil, errors.New("partition has no in-sync follower")
}

type Fault struct {
	Kind   string `json:"kind"`
	Stream string `json:"stream,omitempty"`
	Value  int64  `json:"value,omitempty"`
}

type Event struct {
	Role      Role             `json:"role"`
	Fault     Fault            `json:"fault"`
	Targets   []Target         `json:"targets"`
	Start     time.Time        `json:"start"`
	Heal      time.Time        `json:"heal,omitempty"`
	Mutations []*ProxyMutation `json:"mutations,omitempty"`
}

type ProxyMutation struct {
	Target   Target    `json:"target"`
	Started  time.Time `json:"started"`
	Accepted time.Time `json:"accepted"`
}

type restoration func(context.Context) error

type Injection struct {
	Event Event
	undo  []restoration
}

// Heal restores the original targets in reverse order. Failed restores remain
// pending so callers can retry cleanup without touching already healed targets.
func (i *Injection) Heal(ctx context.Context) error {
	var failures []error
	for index := len(i.undo) - 1; index >= 0; index-- {
		if i.undo[index] == nil {
			continue
		}
		if err := i.undo[index](ctx); err != nil {
			failures = append(failures, err)
		} else {
			i.undo[index] = nil
		}
	}
	if len(failures) == 0 && i.Event.Heal.IsZero() {
		i.Event.Heal = time.Now().UTC()
	}
	return errors.Join(failures...)
}

func (l *Lab) Inject(ctx context.Context, role Role, fault Fault) (*Injection, error) {
	if err := fault.validate(role); err != nil {
		return nil, err
	}
	targets, err := l.Resolve(ctx, role)
	if err != nil {
		return nil, err
	}
	injection := &Injection{Event: Event{Role: role, Fault: fault, Targets: targets, Start: time.Now().UTC()}}
	seen := make(map[string]bool)
	for _, target := range targets {
		if fault.physical() && seen[target.Container] {
			continue
		}
		seen[target.Container] = true
		undo, injectErr := l.apply(ctx, target, fault, &injection.Event)
		if undo != nil {
			injection.undo = append(injection.undo, undo)
		}
		if injectErr != nil {
			cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			return injection, errors.Join(injectErr, injection.Heal(cleanup))
		}
	}
	return injection, nil
}
