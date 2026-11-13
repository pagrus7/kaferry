package faults

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func prepareProxyProducer(ctx context.Context, client *kgo.Client, topic string, partition int32, routes map[int32]string, logf func(string, ...any)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	logf("producer setup: discover route brokers")
	if err := client.Ping(ctx); err != nil {
		return fmt.Errorf("discover producer route: %w", err)
	}
	ids := make([]int32, 0, len(routes))
	for id := range routes {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	leader := int32(-1)
	for {
		known, err := proxyProducerLeader(ctx, client, topic, partition, ids, routes, logf)
		if err != nil {
			return err
		}
		if known >= 0 {
			leader = known
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
	logf("producer setup: initialize producer ID")
	if _, _, err := client.ProducerID(ctx); err != nil {
		return fmt.Errorf("initialize diagnostic producer ID: %w", err)
	}
	logf("producer setup: open leader %d produce connection", leader)
	if err := client.EnsureProduceConnectionIsOpen(ctx, leader); err != nil {
		return fmt.Errorf("open diagnostic produce connection: %w", err)
	}
	logf("producer setup: ready for one proof write")
	return nil
}

func proxyProducerLeader(ctx context.Context, client *kgo.Client, topic string, partition int32, ids []int32, routes map[int32]string, logf func(string, ...any)) (int32, error) {
	leader := int32(-1)
	for _, id := range ids {
		request := kmsg.NewPtrMetadataRequest()
		request.Topics = []kmsg.MetadataRequestTopic{{Topic: &topic}}
		response, err := client.Broker(int(id)).Request(ctx, request)
		if err != nil {
			return -1, fmt.Errorf("observe producer metadata on broker %d: %w", id, err)
		}
		known, err := proxyMetadataLeader(response.(*kmsg.MetadataResponse), topic, partition, routes)
		logf("producer setup: broker %d observed leader %d, error %v", id, known, err)
		if errors.Is(err, kerr.UnknownTopicOrPartition) || errors.Is(err, kerr.LeaderNotAvailable) {
			return -1, nil
		}
		if err != nil {
			return -1, err
		}
		if known < 0 || leader >= 0 && known != leader {
			return -1, nil
		}
		leader = known
	}
	return leader, nil
}

func proxyMetadataLeader(response *kmsg.MetadataResponse, topic string, partition int32, routes map[int32]string) (int32, error) {
	if err := kerr.ErrorForCode(response.ErrorCode); err != nil {
		return -1, err
	}
	if err := verifyProducerRoute(response.Brokers, routes); err != nil {
		return -1, err
	}
	for _, detail := range response.Topics {
		if detail.Topic == nil || *detail.Topic != topic {
			continue
		}
		if err := kerr.ErrorForCode(detail.ErrorCode); err != nil {
			return -1, err
		}
		for _, part := range detail.Partitions {
			if part.Partition == partition {
				return proxyPartitionLeader(part, routes)
			}
		}
	}
	return -1, nil
}

func verifyProducerRoute(brokers []kmsg.MetadataResponseBroker, routes map[int32]string) error {
	if len(brokers) != len(routes) {
		return errors.New("producer metadata does not describe the whole selected route")
	}
	seen := make(map[int32]bool)
	for _, broker := range brokers {
		address := net.JoinHostPort(broker.Host, strconv.Itoa(int(broker.Port)))
		if seen[broker.NodeID] || routes[broker.NodeID] != address {
			return fmt.Errorf("metadata broker %d bypasses the selected producer route", broker.NodeID)
		}
		seen[broker.NodeID] = true
	}
	return nil
}

func proxyPartitionLeader(part kmsg.MetadataResponseTopicPartition, routes map[int32]string) (int32, error) {
	if err := kerr.ErrorForCode(part.ErrorCode); err != nil {
		return -1, err
	}
	replicas, isr := slices.Clone(part.Replicas), slices.Clone(part.ISR)
	slices.Sort(replicas)
	slices.Sort(isr)
	if part.Leader < 0 || len(replicas) != 3 || len(slices.Compact(replicas)) != 3 || !slices.Equal(replicas, isr) {
		return -1, nil
	}
	if _, ok := routes[part.Leader]; !ok {
		return -1, errors.New("partition leader is outside the selected producer route")
	}
	return part.Leader, nil
}

type producerConnections struct {
	mu     sync.Mutex
	counts map[int32]int
}

func (c *producerConnections) OnBrokerConnect(meta kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	if err == nil {
		c.mu.Lock()
		c.counts[meta.NodeID]++
		c.mu.Unlock()
	}
}

func readinessCluster(t *testing.T) (*kfake.Cluster, map[int32]string) {
	t.Helper()
	cluster := kfake.MustCluster(kfake.NumBrokers(3), kfake.BrokerConfigs(map[string]string{"default.replication.factor": "3"}), kfake.SeedTopics(3, "records"))
	t.Cleanup(cluster.Close)
	routes := make(map[int32]string)
	for index, address := range cluster.ListenAddrs() {
		routes[int32(index)] = address
	}
	return cluster, routes
}

func unknownProxyTopic(version int16, routes map[int32]string) *kmsg.MetadataResponse {
	response := kmsg.NewPtrMetadataResponse()
	response.Version = version
	for id, address := range routes {
		host, port, _ := net.SplitHostPort(address)
		number, _ := strconv.Atoi(port)
		response.Brokers = append(response.Brokers, kmsg.MetadataResponseBroker{NodeID: id, Host: host, Port: int32(number)})
	}
	response.Topics = []kmsg.MetadataResponseTopic{{Topic: kmsg.StringPtr("records"), ErrorCode: kerr.UnknownTopicOrPartition.Code}}
	return response
}
