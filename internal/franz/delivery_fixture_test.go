package franz

import (
	"context"
	"errors"
	"github.com/pagrus7/kaferry/internal/commitmeta"
	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/delivery"
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"testing"
)

// memoryAuthority isolates source positioning and group-callback tests from output I/O.
type memoryAuthority struct {
	frames   map[int32]*delivery.Frame
	metadata *sourceMetadataMock
}
type memoryPartition struct {
	owner *memoryAuthority
	id    int32
	frame *delivery.Frame
}

func (m *memoryAuthority) Prepare(context.Context, []int32) error { return nil }
func (m *memoryAuthority) Read(_ context.Context, id int32) (*delivery.Frame, error) {
	return m.frames[id], nil
}
func (m *memoryAuthority) Acquire(_ context.Context, id int32) (delivery.Partition, error) {
	if m.frames == nil {
		m.frames = make(map[int32]*delivery.Frame)
	}
	if m.metadata != nil {
		if pos, ok := m.metadata.saved[id]; ok {
			progress := m.metadata.progress[id]
			text, err := commitmeta.NewCodec().Encode(progress)
			if err != nil {
				return nil, err
			}
			m.frames[id] = &delivery.Frame{Version: 1, Cursor: core.Cursor{Partition: id, Position: pos}, Metadata: text}
		}
	}
	return &memoryPartition{owner: m, id: id, frame: m.frames[id]}, nil
}
func (p *memoryPartition) Saved() *delivery.Frame { return p.frame }
func (p *memoryPartition) Commit(_ context.Context, f delivery.Frame) error {
	p.frame = &f
	p.owner.frames[p.id] = &f
	return nil
}
func (p *memoryPartition) Close() error { return nil }
func configureComponentOutput(ctx context.Context, f *Factory, p plan.Plan, source, destination kafkaport.Connection) error {
	s := f.session
	s.Plan = p
	s.Identity.Group = p.Group
	s.Identity.Fingerprint = "component-fingerprint"
	var err error
	s.Identity.SourceCluster, err = source.ClusterID(ctx)
	if err != nil {
		return err
	}
	s.Identity.DestinationCluster, err = destination.ClusterID(ctx)
	if err != nil {
		return err
	}
	s.Identity.SourceTopic, err = source.(*connection).TopicID(ctx)
	if err != nil {
		return err
	}
	s.Identity.DestinationTopic, err = destination.(*connection).TopicID(ctx)
	if err != nil {
		return err
	}
	opts, err := destination.(*connection).borrowedOptions()
	if err != nil {
		return err
	}
	c, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}
	defer c.Close()
	compact := "compact"
	response, err := kadm.NewClient(c).CreateTopics(ctx, 1, 1, map[string]*string{"cleanup.policy": &compact}, "progress")
	if err != nil {
		return err
	}
	if err = response.Error(); err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		return err
	}
	log, err := destination.(*connection).ProgressLog(ctx, s, "progress")
	if err != nil {
		return err
	}
	return log.Ensure(ctx, nil)
}
func componentOutput(t *testing.T, cluster *kfake.Cluster, p plan.Plan) (kafkaport.Connection, core.Sink, *delivery.Session) {
	t.Helper()
	f := componentFactory()
	src, err := f.Open(t.Context(), p.Source)
	require.NoError(t, err)
	dst, err := f.Open(t.Context(), p.Destination)
	require.NoError(t, err)
	require.NoError(t, configureComponentOutput(t.Context(), f, p, src, dst))
	sink, err := dst.Sink(t.Context(), config.Defaults("").Worker)
	require.NoError(t, err)
	require.NoError(t, f.session.Acquire(t.Context(), []int32{0}))
	t.Cleanup(func() {
		_ = f.session.Close()
		sink.Close()
		_ = src.Close(context.Background())
		_ = dst.Close(context.Background())
	})
	return dst, sink, f.session
}
