package app

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/pagrus7/kaferry/internal/archiveio"
	"github.com/pagrus7/kaferry/internal/checkpoint"
	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/delivery"
	"github.com/pagrus7/kaferry/internal/inspect"
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/plan"
)

type deliveryInspection struct {
	kafka       KafkaFactory
	read        inspect.KafkaFactory
	files       *archiveio.Inspector
	positioning kafkaport.PositionerFactory
	clock       core.Clock
}

func newDeliveryInspection(kafka KafkaFactory, read inspect.KafkaFactory, positioning kafkaport.PositionerFactory, clock core.Clock) *deliveryInspection {
	return &deliveryInspection{kafka: kafka, read: read, files: archiveio.NewInspector(clock), positioning: positioning, clock: clock}
}

type frameReader interface {
	Read(context.Context, int32) (*delivery.Frame, error)
}

func (i *deliveryInspection) service(ctx context.Context, p plan.Plan, w config.Worker) (*inspect.Service, func() error, error) {
	s := delivery.New(p, w)
	factory := i.kafka.ForQuery(s)
	owners := []kafkaport.Connection{}
	cleanup := func() error {
		var err error
		for _, c := range owners {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err = errors.Join(err, c.Close(ctx))
			cancel()
		}
		return err
	}
	fail := func(err error) (*inspect.Service, func() error, error) {
		return nil, cleanup, errors.Join(err, cleanup())
	}
	var source, destination kafkaport.DeliveryConnection
	for _, dest := range []bool{false, true} {
		endpoint := p.Source
		if dest {
			endpoint = p.Destination
		}
		if endpoint.Kind != "TOPIC" {
			continue
		}
		c, err := factory.Open(ctx, endpoint)
		if err != nil {
			return fail(err)
		}
		owners = append(owners, c)
		exec, ok := c.(kafkaport.DeliveryConnection)
		if !ok {
			return fail(errors.New("Kafka endpoint cannot inspect delivery authority"))
		}
		cluster, err := exec.ClusterID(ctx)
		if err != nil {
			return fail(err)
		}
		id, err := exec.TopicID(ctx)
		if err != nil {
			return fail(err)
		}
		if dest {
			s.Identity.DestinationCluster = cluster
			s.Identity.DestinationTopic = id
			destination = exec
		} else {
			s.Identity.SourceCluster = cluster
			s.Identity.SourceTopic = id
			source = exec
		}
	}
	fp, err := p.Fingerprint(s.Identity.SourceCluster, s.Identity.DestinationCluster)
	if err != nil {
		return fail(&kafkaport.Conflict{Err: err})
	}
	s.Identity.Group = p.Group
	s.Identity.Fingerprint = fp
	if p.Source.Kind == "TOPIC" && p.Destination.Kind == "TOPIC" && p.Source.Name == p.Destination.Name && s.Identity.SourceCluster == s.Identity.DestinationCluster {
		return fail(&kafkaport.Conflict{Err: errors.New("source and destination are the same Kafka topic")})
	}
	var log delivery.Log
	authority := destination
	if p.Destination.Kind == "ARCHIVE" {
		authority = source
	}
	if authority != nil {
		log, err = authority.ProgressLog(ctx, s, w.ProgressTopic)
		if err != nil {
			return fail(err)
		}
	}
	var reader frameReader
	if p.Destination.Kind == "TOPIC" {
		reader = delivery.Reader{Session: s, Log: log}
	} else {
		reader = archiveio.NewCapture(s, i.clock, log, checkpoint.DurableWriter{})
	}
	bound := false
	var bindingErr error
	if p.Source.Kind == "ARCHIVE" && !p.OneOff {
		// Resolve configured storage before classifying corrupt saved identity.
		if w.CheckpointDir == nil {
			if _, e := os.UserHomeDir(); e != nil {
				return fail(&core.Exit{Code: 2, Err: errors.New("cannot resolve the default checkpoint directory; set HOME or worker.checkpoint_dir")})
			}
		}
		bound, err = archiveio.InspectBinding(s, w)
		if err != nil {
			bindingErr = err
		}
	}
	files := &deliveryInspectionFiles{Inspector: i.files, ctx: ctx, reader: reader, bound: bound, bindingErr: bindingErr}
	return inspect.NewExplainer(deliveryInspectionFactory{KafkaFactory: i.read, session: s, reader: reader}, files, i.positioning), cleanup, nil
}
func (i *deliveryInspection) Explain(ctx context.Context, p plan.Plan, w config.Worker, stop core.StopObserver) (report inspect.Report, result error) {
	service, close, err := i.service(ctx, p, w)
	if err != nil {
		return report, err
	}
	defer func() { result = errors.Join(result, close()) }()
	return service.Explain(ctx, p, w, stop)
}
func (i *deliveryInspection) Status(ctx context.Context, p plan.Plan, w config.Worker, stop core.StopObserver) (report inspect.StatusReport, result error) {
	service, close, err := i.service(ctx, p, w)
	if err != nil {
		return report, err
	}
	defer func() { result = errors.Join(result, close()) }()
	return service.Status(ctx, p, w, stop)
}

type deliveryInspectionFactory struct {
	inspect.KafkaFactory
	session *delivery.Session
	reader  frameReader
}

func (f deliveryInspectionFactory) OpenInspection(ctx context.Context, e plan.Endpoint) (kafkaport.InspectionConnection, error) {
	c, err := f.KafkaFactory.OpenInspection(ctx, e)
	if err != nil {
		return nil, err
	}
	p := f.session.Plan
	if e.Kind == p.Source.Kind && e.Name == p.Source.Name && e.Alias == p.Source.Alias {
		return &deliveryInspectionConnection{InspectionConnection: c, session: f.session, reader: f.reader}, nil
	}
	return c, nil
}

type deliveryInspectionConnection struct {
	kafkaport.InspectionConnection
	session *delivery.Session
	reader  frameReader
}

func (c *deliveryInspectionConnection) CheckMembers(ctx context.Context, g, p string) error {
	return c.InspectionConnection.CheckMembers(ctx, g, c.session.Protocol(p))
}
func (c *deliveryInspectionConnection) Membership(ctx context.Context, g, p string) (kafkaport.Membership, error) {
	return c.InspectionConnection.Membership(ctx, g, c.session.Protocol(p))
}
func (c *deliveryInspectionConnection) Saved(ctx context.Context, g, fp string, w plan.WindowSpec, stale int64) (map[int32]int64, map[int32]kafkaport.Progress, error) {
	description, err := c.Describe(ctx, false)
	if err != nil {
		return nil, nil, err
	}
	native, _, err := c.InspectionConnection.Saved(ctx, g, fp, w, stale)
	if err != nil {
		return nil, nil, err
	}
	frames := make(map[int32]*delivery.Frame)
	for _, id := range description.Partitions {
		f, err := c.reader.Read(ctx, id)
		if err != nil {
			return nil, nil, &kafkaport.Conflict{Err: err}
		}
		if f == nil {
			if _, present := native[id]; present {
				return nil, nil, &kafkaport.Conflict{Err: errors.New("source offsets exist without an exactly-once checkpoint")}
			}
		}
		frames[id] = f
	}
	return kafkaport.AuthoritativeSaved(frames, c.session.Plan, fp, description.TopicID)
}

type deliveryInspectionFiles struct {
	*archiveio.Inspector
	ctx        context.Context
	reader     frameReader
	bound      bool
	bindingErr error
}

func (f *deliveryInspectionFiles) Source(p plan.Plan, fp string, w config.Worker) (archiveio.SourceInspection, error) {
	if f.bindingErr != nil {
		return archiveio.SourceInspection{}, &archiveio.Conflict{Err: f.bindingErr}
	}
	if !f.bound {
		return f.Inspector.Source(p, fp, w)
	}
	frame, err := f.reader.Read(f.ctx, 0)
	if err != nil {
		return archiveio.SourceInspection{}, &archiveio.Conflict{Err: err}
	}
	if frame == nil || frame.Replay == nil {
		return archiveio.SourceInspection{}, &archiveio.Conflict{Err: errors.New("replay binding has no authoritative checkpoint")}
	}
	return f.Inspector.SavedSource(p, fp, w, frame.Replay)
}
