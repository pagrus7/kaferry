package faults

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaMetadata map[string]*kadm.Client

// NewMetadata uses the independent admin route. Worker-route cuts do not cut
// this observer's connections.
func NewMetadata() (KafkaMetadata, error) {
	clients := make(KafkaMetadata)
	for _, side := range []string{"source", "destination"} {
		client, err := kgo.NewClient(kgo.SeedBrokers(Endpoints(side, 0)...), kgo.RetryTimeout(10*time.Second))
		if err != nil {
			clients.Close()
			return nil, err
		}
		clients[side] = kadm.NewClient(client)
	}
	return clients, nil
}

func (m KafkaMetadata) Close() {
	for _, client := range m {
		client.Close()
	}
}

func (m KafkaMetadata) Partition(ctx context.Context, cluster, topic string, partition int32) (Partition, error) {
	if err := ctx.Err(); err != nil {
		return Partition{}, err
	}
	client := m[cluster]
	if client == nil {
		return Partition{}, errors.New("metadata cluster is not configured")
	}
	metadata, err := client.Metadata(ctx, topic)
	if err != nil {
		return Partition{}, err
	}
	detail := metadata.Topics[topic]
	if detail.Err != nil {
		return Partition{}, fmt.Errorf("topic metadata: %w", detail.Err)
	}
	part, ok := detail.Partitions[partition]
	if !ok {
		return Partition{}, errors.New("partition metadata is unavailable")
	}
	if part.Err != nil {
		return Partition{}, fmt.Errorf("partition metadata: %w", part.Err)
	}
	return Partition{part.Leader, part.ISR}, nil
}

func (m KafkaMetadata) Coordinator(ctx context.Context, cluster, group string) (int32, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	client := m[cluster]
	if client == nil {
		return 0, errors.New("metadata cluster is not configured")
	}
	response, ok := client.FindGroupCoordinators(ctx, group)[group]
	if !ok {
		return 0, errors.New("group coordinator metadata is unavailable")
	}
	return response.NodeID, response.Err
}

func routePort(cluster, listener string, broker int32) int {
	base := 46092
	if cluster == "destination" {
		base = 47092
	}
	shifts := map[string]int{"internal": -20000, "controller": -18000, "admin": 0, "worker1": 100, "worker2": 200, "worker3": 300}
	return base + shifts[listener] + int(broker) - 1
}

// Endpoints returns the three native client addresses; worker zero is the
// independent publisher/oracle/admin route.
func Endpoints(cluster string, worker int) []string {
	listener := "admin"
	if worker != 0 {
		listener = fmt.Sprintf("worker%d", worker)
	}
	var addresses []string
	for id := int32(1); id <= 3; id++ {
		addresses = append(addresses, fmt.Sprintf("127.0.0.1:%d", routePort(cluster, listener, id)))
	}
	return addresses
}
