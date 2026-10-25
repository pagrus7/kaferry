package franz

import (
	"context"
	"log/slog"
	"slices"
	"time"

	"github.com/pagrus7/kaferry/internal/commitmeta"
	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Factory struct {
	codec       commitmeta.Codec
	clients     Clients
	positioning kafkaport.PositionerFactory
	clock       core.Clock
	log         *slog.Logger
}

func NewFactory(codec commitmeta.Codec, clients Clients, positioning kafkaport.PositionerFactory, clock core.Clock, log *slog.Logger) *Factory {
	return &Factory{codec: codec, clients: clients, positioning: positioning, clock: clock, log: log}
}

// Validate checks both endpoints before the engine opens Kafka or file handles.
func (f *Factory) Validate(p plan.Plan, limits config.Worker) error {
	for _, endpoint := range []plan.Endpoint{p.Source, p.Destination} {
		if endpoint.Kind != "TOPIC" {
			continue
		}
		s, err := parseSettings(endpoint.Properties)
		if err != nil {
			return err
		}
		// Topic-dependent producer limits are checked when opening its sink.
		if !s.explicitMessageBytes {
			s.messageBytes = 1000000000
		}
		if _, err = s.bounded(limits); err != nil {
			return err
		}
		if _, err = s.commonOptions(); err != nil {
			return err
		}
	}
	return nil
}
func (f *Factory) Open(ctx context.Context, endpoint plan.Endpoint) (kafkaport.Connection, error) {
	connection, err := f.open(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	return connection, nil
}

func (f *Factory) OpenInspection(ctx context.Context, endpoint plan.Endpoint) (kafkaport.InspectionConnection, error) {
	connection, err := f.open(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	return connection, nil
}

func (f *Factory) open(ctx context.Context, endpoint plan.Endpoint) (*connection, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s, err := parseSettings(endpoint.Properties)
	if err != nil {
		return nil, err
	}
	options, err := s.commonOptions()
	if err != nil {
		return nil, err
	}
	clientCtx, cancel := context.WithCancel(context.Background())
	client, err := f.clients.Metadata(append(options, kgo.WithContext(clientCtx))...)
	if err != nil {
		cancel()
		return nil, conflict("invalid Kafka connection configuration")
	}
	return &connection{factory: f, endpoint: endpoint, settings: s, inspector: &Inspector{codec: f.codec, client: client, topic: endpoint.Name, log: f.log}, cancel: cancel}, nil
}

type connection struct {
	factory   *Factory
	endpoint  plan.Endpoint
	settings  settings
	inspector *Inspector
	cancel    context.CancelFunc
}

func (c *connection) ClusterID(ctx context.Context) (string, error) {
	if err := c.inspector.Refresh(ctx); err != nil {
		return "", err
	}
	return c.inspector.clusterID, nil
}
func (c *connection) Partitions(ctx context.Context) ([]int32, error) {
	return c.partitions(ctx, true)
}

func (c *connection) DestinationPartitions(ctx context.Context) (int, error) {
	parts, err := c.partitions(ctx, false)
	return len(parts), err
}

func (c *connection) partitions(ctx context.Context, source bool) ([]int32, error) {
	if err := c.inspector.refresh(ctx, source); err != nil {
		return nil, err
	}
	parts := slices.Clone(c.inspector.parts)
	if len(parts) == 0 {
		return nil, conflict("topic metadata contains no partitions")
	}
	slices.Sort(parts)
	for i, id := range parts {
		if int64(id) != int64(i) {
			return nil, conflict("topic metadata contains invalid partition IDs")
		}
	}
	return parts, nil
}

func (c *connection) ValidatePartitions(ctx context.Context, ids []int32) error {
	count, err := c.DestinationPartitions(ctx)
	if err != nil {
		return err
	}
	if err := plan.CheckPartitions(ids, count); err != nil {
		return &kafkaport.Conflict{Err: err}
	}
	return nil
}

func (c *connection) CheckMembers(ctx context.Context, group, protocol string) error {
	return c.inspector.CheckMembers(ctx, group, protocol)
}
func (c *connection) ReadProgress(ctx context.Context, group, fp string, window plan.WindowSpec, staleness int64) (map[int32]kafkaport.Progress, error) {
	_, progress, err := c.inspector.Saved(ctx, group, fp, window, staleness)
	return progress, err
}
func (c *connection) Close(context.Context) error { c.cancel(); c.inspector.client.Close(); return nil }

func (c *connection) Source(query context.Context, p plan.Plan, fp string, limits config.Worker, routing kafkaport.PartitionValidator) (core.Source, core.ProgressStore, error) {
	if err := query.Err(); err != nil {
		return nil, nil, err
	}
	if c.inspector.clusterID == "" {
		if err := c.inspector.Refresh(query); err != nil {
			return nil, nil, err
		}
	}
	if _, err := p.Partitions.Select(c.inspector.parts); err != nil {
		return nil, nil, &kafkaport.Conflict{Err: err}
	}
	settings, err := c.settings.bounded(limits)
	if err != nil {
		return nil, nil, err
	}
	options, err := settings.commonOptions()
	if err != nil {
		return nil, nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	clientCtx, clientCancel := context.WithCancel(context.Background())
	bridge := newRebalanceBridge(ctx, p.Source.Name)
	options = append(options, settings.consumerOptions()...)
	options = append(options, bridge.options()...)
	clientID := "kaferry-" + p.Group
	if settings.clientID != "" {
		clientID += "-" + settings.clientID
	}
	options = append(options, kgo.WithContext(clientCtx), kgo.ClientID(clientID), kgo.ConsumerGroup(p.Group), kgo.DisableAutoCommit(), kgo.ConsumeResetOffset(kgo.NoResetOffset()), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.FetchMaxWait(500*time.Millisecond), kgo.KeepControlRecords(), kgo.BlockRebalanceOnPoll(), kgo.Balancers(sourceBalancer(p, fp)))
	client, err := c.factory.clients.Consumer(options...)
	if err != nil {
		cancel()
		clientCancel()
		return nil, nil, conflict("invalid Kafka consumer configuration")
	}
	store := &Committer{codec: c.factory.codec, client: client, topic: p.Source.Name, group: p.Group, topicID: c.inspector.topicID, metadata: make(map[int32]kafkaport.Progress)}
	source := &Source{routing: routing, client: client, inspector: c.inspector, positioning: c.factory.positioning, store: store, bridge: bridge, plan: p, fingerprint: fp, topicID: topicID(c.inspector.topicID), clusterID: c.inspector.clusterID, originalTopicID: c.inspector.topicID, query: query, ctx: ctx, cancel: cancel, clientCancel: clientCancel, positions: make(map[int32]int64), paused: make(map[int32]bool), held: make(map[int32]*kgo.Record), retention: make(map[int32]kafkaport.RetentionCheck), log: c.factory.log}
	source.gaps = &fetchGap{client: c.inspector.client, topic: p.Source.Name, topicID: source.topicID}
	return source, store, nil
}
func (c *connection) Sink(query context.Context, limits config.Worker) (core.Sink, error) {
	if err := query.Err(); err != nil {
		return nil, err
	}
	configuration := c.inspector.Config(query)
	if err := query.Err(); err != nil {
		return nil, err
	}
	settings, err := c.settings.withTopicLimit(configuration).bounded(limits)
	if err != nil {
		return nil, err
	}
	options, err := settings.commonOptions()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	options = append(options, settings.producerOptions()...)
	if c.endpoint.PreservesPartitions() {
		options = append(options, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	}
	client, err := c.factory.clients.Producer(append(options, kgo.WithContext(ctx))...)
	if err != nil {
		cancel()
		return nil, conflict("invalid Kafka producer configuration")
	}
	return &Sink{preserve: c.endpoint.PreservesPartitions(), client: client, topic: c.endpoint.Name, settings: settings, configuration: configuration, inspector: c.inspector, clock: c.factory.clock, log: c.factory.log, reports: make(chan core.Delivery, limits.RecordCap+int(settings.queueRecords)+1), ctx: ctx, cancel: cancel, partitionCount: len(c.inspector.parts), lastMetadata: c.factory.clock.Now()}, nil
}
