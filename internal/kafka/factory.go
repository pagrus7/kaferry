package kafka

import (
	"log/slog"

	"github.com/pagrus7/kaferry/internal/config"
	copy "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
)

// Connection owns metadata access and creates adapters for one topic.
type Connection interface {
	ClusterID() (string, error)
	CheckMembers(string) error
	ReadProgress(string, string, plan.WindowSpec, int64) (map[int32]Progress, error)
	Source(plan.Plan, string, config.Worker, copy.OutputProgress) (copy.Source, copy.ProgressStore, error)
	Sink(config.Worker) (copy.Sink, error)
	Close()
}

type Factory struct {
	clients     Clients
	positioning PositionerFactory
	clock       copy.Clock
	logger      *slog.Logger
}

func NewFactory(clients Clients, positioning PositionerFactory, clock copy.Clock, logger *slog.Logger) *Factory {
	return &Factory{clients: clients, positioning: positioning, clock: clock, logger: logger}
}

func (f *Factory) Open(endpoint plan.Endpoint) (Connection, error) {
	inspector, err := NewInspector(f.clients, endpoint.Properties, endpoint.Name, f.logger)
	if err != nil {
		return nil, err
	}
	return &connection{Metadata: inspector, endpoint: endpoint, factory: f}, nil
}

type connection struct {
	Metadata
	endpoint plan.Endpoint
	factory  *Factory
}

func (c *connection) Source(p plan.Plan, fp string, limits config.Worker, output copy.OutputProgress) (copy.Source, copy.ProgressStore, error) {
	return NewSource(c.factory.clients, c.Metadata, c.factory.positioning, p, fp, limits, c.factory.logger, output)
}

func (c *connection) Sink(limits config.Worker) (copy.Sink, error) {
	return NewSink(c.factory.clients, c.Metadata, c.endpoint, limits, c.factory.clock, c.factory.logger)
}
