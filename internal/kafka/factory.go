package kafka

import (
	"context"
	"log/slog"

	"github.com/pagrus7/kaferry/internal/config"
	copy "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/plan"
)

// Connection owns metadata access and creates adapters for one topic.
type Connection = kafkaport.Connection

type Factory struct {
	clients     Clients
	positioning PositionerFactory
	clock       copy.Clock
	logger      *slog.Logger
}

func NewFactory(clients Clients, positioning PositionerFactory, clock copy.Clock, logger *slog.Logger) *Factory {
	return &Factory{clients: clients, positioning: positioning, clock: clock, logger: logger}
}

func (f *Factory) Open(ctx context.Context, endpoint plan.Endpoint) (Connection, error) {
	inspector, err := NewInspector(ctx, f.clients, endpoint.Properties, endpoint.Name, f.logger)
	if err != nil {
		return nil, err
	}
	return &connection{inspector: inspector, endpoint: endpoint, factory: f}, nil
}

type connection struct {
	inspector *Inspector
	endpoint  plan.Endpoint
	factory   *Factory
}

func (c *connection) Source(ctx context.Context, p plan.Plan, fp string, limits config.Worker) (copy.Source, SourceProgress, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	return NewSource(ctx, c.factory.clients, c.view(ctx), c.factory.positioning, p, fp, limits, c.factory.logger)
}

func (c *connection) Sink(ctx context.Context, limits config.Worker) (copy.Sink, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return NewSink(c.factory.clients, c.view(ctx), c.endpoint, limits, c.factory.clock, c.factory.logger)
}

func (c *connection) view(ctx context.Context) Metadata {
	return &scopedMetadata{Inspector: c.inspector, parent: ctx}
}

func (c *connection) ClusterID(ctx context.Context) (string, error) { return c.view(ctx).ClusterID() }
func (c *connection) CheckMembers(ctx context.Context, group string) error {
	return c.view(ctx).CheckMembers(group)
}
func (c *connection) ReadProgress(ctx context.Context, group, fp string, window plan.WindowSpec, cutoff int64) (map[int32]Progress, error) {
	return c.view(ctx).ReadProgress(group, fp, window, cutoff)
}

func (c *connection) Close(context.Context) error {
	// The native binding cannot cancel Close. Always close its handle, including
	// after caller cancellation; early cleanup mitigates coordinator timeout crashes.
	c.inspector.Close()
	return nil
}
