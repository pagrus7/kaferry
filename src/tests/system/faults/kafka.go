package faults

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type KafkaMetadata map[string]*MetadataClient

// MetadataClient retains the one underlying client used by the admin wrapper.
type MetadataClient struct {
	*kadm.Client
	raw       *kgo.Client
	cluster   string
	expected  map[int32]string
	mu        sync.RWMutex
	connected map[int32]metadataTransport
}

type metadataTransport struct {
	Broker          int32
	Address, Remote string
	At              time.Time
}

type MetadataProbe struct {
	Target           Target    `json:"target"`
	ClientCluster    string    `json:"client_cluster"`
	Route            string    `json:"route"`
	APIKey           int16     `json:"api_key"`
	AllTopics        bool      `json:"all_topics"`
	ExpectedAddress  string    `json:"expected_address"`
	TransportBroker  int32     `json:"transport_broker"`
	TransportAddress string    `json:"transport_address"`
	RemoteAddress    string    `json:"remote_address"`
	ConnectedAt      time.Time `json:"connected_at"`
	StartedAt        time.Time `json:"started_at"`
	FinishedAt       time.Time `json:"finished_at"`
	ResponseAt       time.Time `json:"response_at"`
	Error            string    `json:"error"`
}

// NewMetadataClient uses the existing three-broker ADMIN route and options.
func NewMetadataClient(cluster string, seeds ...string) (*MetadataClient, error) {
	if (cluster != "source" && cluster != "destination") || len(seeds) != 3 {
		return nil, errors.New("metadata requires a known three-broker cluster")
	}
	client := &MetadataClient{cluster: cluster, expected: make(map[int32]string), connected: make(map[int32]metadataTransport)}
	for index, address := range seeds {
		client.expected[int32(index+1)] = address
	}
	raw, err := kgo.NewClient(kgo.SeedBrokers(seeds...), kgo.RetryTimeout(10*time.Second), kgo.WithHooks(client))
	if err != nil {
		return nil, err
	}
	client.raw, client.Client = raw, kadm.NewClient(raw)
	return client, nil
}

func (c *MetadataClient) OnBrokerConnect(meta kgo.BrokerMetadata, _ time.Duration, connection net.Conn, err error) {
	if err != nil || connection == nil || meta.NodeID < 1 || meta.NodeID > 3 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connected[meta.NodeID] = metadataTransport{Broker: meta.NodeID, Address: net.JoinHostPort(meta.Host, strconv.Itoa(int(meta.Port))), Remote: connection.RemoteAddr().String(), At: time.Now().UTC()}
}

// ProbeMetadata waits for a fresh all-topics response from the explicit broker.
// The caller's context covers discovery, transport and response completion.
func (c *MetadataClient) ProbeMetadata(ctx context.Context, target Target) (response *kmsg.MetadataResponse, proof MetadataProbe, err error) {
	proof = MetadataProbe{Target: target, ClientCluster: c.cluster, Route: "admin", APIKey: 3, AllTopics: true, StartedAt: time.Now().UTC(), ExpectedAddress: c.expected[target.Broker]}
	defer func() { err = c.finishProbe(ctx, target, &proof, err) }()
	if err = ctx.Err(); err != nil {
		return
	}
	if c.raw == nil || target.Cluster != c.cluster || proof.ExpectedAddress == "" || target.Service != fmt.Sprintf("%s-%d", target.Cluster, target.Broker) {
		err = errors.New("metadata target differs from selected cluster or broker")
		return
	}
	request := kmsg.NewPtrMetadataRequest() // nil Topics means all; empty means none.
	var raw kmsg.Response
	raw, err = c.RequestBroker(ctx, target.Broker, request)
	if err != nil {
		return
	}
	var ok bool
	response, ok = raw.(*kmsg.MetadataResponse)
	if !ok || response == nil {
		err = errors.New("pinned metadata response is unavailable")
		return
	}
	proof.ResponseAt = time.Now().UTC()
	return
}

func (c *MetadataClient) finishProbe(ctx context.Context, target Target, proof *MetadataProbe, err error) error {
	proof.FinishedAt = time.Now().UTC()
	if ctx.Err() != nil {
		err = errors.Join(err, ctx.Err())
	}
	c.mu.RLock()
	transport := c.connected[target.Broker]
	c.mu.RUnlock()
	proof.TransportBroker, proof.TransportAddress, proof.RemoteAddress, proof.ConnectedAt = transport.Broker, transport.Address, transport.Remote, transport.At
	if err == nil && (transport.Broker != target.Broker || transport.Address != proof.ExpectedAddress || transport.Remote != proof.ExpectedAddress) {
		err = errors.New("pinned metadata transport differs from selected ADMIN route")
	}
	if err != nil {
		proof.Error = err.Error()
		if len(proof.Error) > 2048 {
			proof.Error = proof.Error[:2048] + " [truncated]"
		}
	}
	return err
}

func (c *MetadataClient) AdminRoutes() map[int32]string {
	routes := make(map[int32]string, len(c.expected))
	for broker, address := range c.expected {
		routes[broker] = address
	}
	return routes
}

// RequestBroker bypasses coordinator caching for independently resolved reads.
func (c *MetadataClient) RequestBroker(ctx context.Context, broker int32, request kmsg.Request) (kmsg.Response, error) {
	if c.raw == nil || c.expected[broker] == "" {
		return nil, errors.New("observation broker is outside the selected ADMIN routes")
	}
	return c.raw.Broker(int(broker)).Request(ctx, request)
}

// NewMetadata uses the independent admin route. Worker-route cuts do not cut
// this observer's connections.
func NewMetadata() (KafkaMetadata, error) {
	clients := make(KafkaMetadata)
	for _, side := range []string{"source", "destination"} {
		client, err := NewMetadataClient(side, Endpoints(side, 0)...)
		if err != nil {
			clients.Close()
			return nil, err
		}
		clients[side] = client
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
