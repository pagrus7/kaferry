package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	"go.uber.org/fx"
	"kafka-fwd2/internal/config"
	core "kafka-fwd2/internal/copy"
	"kafka-fwd2/internal/file"
	"kafka-fwd2/internal/kafka"
	"kafka-fwd2/internal/plan"
)

type engineFixture struct {
	trace   []string
	fail    string
	failure error
	input   *engineInput
	output  *engineOutput
	source  *engineSource
	store   core.ProgressStore
	groups  []string
	saved   map[int32]kafka.Progress
}

func newEngineFixture() *engineFixture {
	f := &engineFixture{failure: errors.New("injected failure")}
	f.input = &engineInput{fixture: f}
	f.output = &engineOutput{fixture: f}
	f.source = &engineSource{fixture: f}
	return f
}
func (f *engineFixture) stage(name string) error {
	f.trace = append(f.trace, name)
	if f.fail == name {
		return f.failure
	}
	return nil
}
func (f *engineFixture) Open(endpoint plan.Endpoint) (kafka.Connection, error) {
	if err := f.stage("open-" + endpoint.Name); err != nil {
		return nil, err
	}
	return &engineConnection{fixture: f, name: endpoint.Name}, nil
}
func (f *engineFixture) OpenSource(p plan.Plan, _ string, _ config.Worker) (file.Input, error) {
	f.groups = append(f.groups, p.Group)
	return f.input, f.stage("file-input")
}
func (f *engineFixture) OpenOutput(p plan.Plan, _ string, progress file.DestinationProgress, _ config.Worker) (file.OutputSink, error) {
	f.groups = append(f.groups, p.Group)
	f.input.progress = progress
	return f.output, f.stage("file-output")
}
func (f *engineFixture) New(source core.Source, sink core.Sink, store core.ProgressStore, p plan.Plan, _ config.Worker) Execution {
	f.groups = append(f.groups, p.Group)
	if source == nil || sink == nil || store == nil {
		panic("missing copy dependency")
	}
	_ = f.stage("runner")
	return f
}
func (f *engineFixture) Run(context.Context) error { return f.stage("run") }

type engineConnection struct {
	fixture *engineFixture
	name    string
}

func (c *engineConnection) ClusterID() (string, error) {
	return c.name + "-cluster", c.fixture.stage("cluster-" + c.name)
}
func (c *engineConnection) CheckMembers(string) error { return c.fixture.stage("members") }
func (c *engineConnection) ReadProgress(string, string, plan.WindowSpec, int64) (map[int32]kafka.Progress, error) {
	return c.fixture.saved, c.fixture.stage("progress")
}
func (c *engineConnection) Source(_ plan.Plan, _ string, _ config.Worker, output core.OutputProgress) (core.Source, core.ProgressStore, error) {
	c.fixture.source.output = output
	return c.fixture.source, c.fixture.store, c.fixture.stage("topic-input")
}
func (c *engineConnection) Sink(config.Worker) (core.Sink, error) {
	return c.fixture.output, c.fixture.stage("topic-output")
}
func (c *engineConnection) Close() { _ = c.fixture.stage("close-" + c.name) }

type engineSource struct {
	core.Source
	fixture *engineFixture
	output  core.OutputProgress
}

func (s *engineSource) Close() error { return s.fixture.stage("close-topic-input") }

type engineInput struct {
	core.Source
	core.ProgressStore
	fixture  *engineFixture
	progress file.DestinationProgress
	output   core.OutputProgress
}

func (i *engineInput) Close() error { return i.fixture.stage("close-file-input") }
func (i *engineInput) BindOutput(output core.OutputProgress) {
	i.output = output
	_ = i.fixture.stage("bind-output")
}
func (i *engineInput) Progress() file.DestinationProgress { return i.progress }
func (i *engineInput) DeleteCheckpoint() error            { return i.fixture.stage("delete-checkpoint") }

type engineOutput struct {
	core.Sink
	fixture *engineFixture
}

func (o *engineOutput) Close()                       { _ = o.fixture.stage("close-output") }
func (*engineOutput) DurableOutput() (string, int64) { return "file-id", 100 }

func enginePlan(source, destination string) plan.Plan {
	return plan.Plan{Source: plan.Endpoint{Kind: source, Name: "source"}, Destination: plan.Endpoint{Kind: destination, Name: "destination"}, Group: "group", Start: "EARLIEST"}
}
func engineLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestRuntimeCompositionAcceptsReplacementFactories(t *testing.T) {
	for _, kinds := range [][2]string{{"TOPIC", "TOPIC"}, {"TOPIC", "FILE"}, {"FILE", "TOPIC"}, {"FILE", "FILE"}} {
		t.Run(strings.Join(kinds[:], "-"), func(t *testing.T) {
			f := newEngineFixture()
			// A real interface value, without calling its embedded methods in this test.
			f.store = f.input
			var engine Engine
			graph := fx.New(fx.NopLogger, RuntimeModules, fx.Decorate(
				func(KafkaFactory) KafkaFactory { return f },
				func(FileFactory) FileFactory { return f },
				func(RunnerFactory) RunnerFactory { return f },
				func(*slog.Logger) *slog.Logger { return engineLogger() },
			), fx.Populate(&engine))
			if err := graph.Err(); err != nil {
				t.Fatal(err)
			}
			if err := engine.Run(context.Background(), enginePlan(kinds[0], kinds[1]), config.Worker{}); err != nil {
				t.Fatal(err)
			}
			if kinds[0] == "TOPIC" && kinds[1] == "FILE" && f.source.output != f.output {
				t.Fatal("Kafka input did not receive durable output")
			}
			if kinds[0] == "FILE" {
				var wanted core.OutputProgress
				if kinds[1] == "FILE" {
					wanted = f.output
				}
				if f.input.output != wanted {
					t.Fatal("file checkpoints bound to wrong output")
				}
			}
			output := "topic-output"
			if kinds[1] == "FILE" {
				output = "file-output"
			}
			joined := strings.Join(f.trace, ",")
			if kinds[0] == "TOPIC" && strings.Index(joined, output) > strings.Index(joined, "topic-input") {
				t.Fatal("Kafka intake constructed before destination", f.trace)
			}
			if strings.Count(joined, "close-output") != 1 {
				t.Fatal("destination was not closed exactly once", f.trace)
			}
		})
	}
}

func TestEngineClosesOnlyAcquiredResourcesOnFailure(t *testing.T) {
	cases := []struct {
		source, destination, fail string
		closed                    []string
	}{
		{"TOPIC", "TOPIC", "open-source", nil},
		{"TOPIC", "TOPIC", "cluster-source", []string{"close-source"}},
		{"TOPIC", "TOPIC", "open-destination", []string{"close-source"}},
		{"TOPIC", "TOPIC", "cluster-destination", []string{"close-destination", "close-source"}},
		{"TOPIC", "TOPIC", "members", []string{"close-destination", "close-source"}},
		{"TOPIC", "TOPIC", "progress", []string{"close-destination", "close-source"}},
		{"TOPIC", "TOPIC", "topic-output", []string{"close-destination", "close-source"}},
		{"TOPIC", "TOPIC", "topic-input", []string{"close-output", "close-destination", "close-source"}},
		{"TOPIC", "TOPIC", "run", []string{"close-topic-input", "close-output", "close-destination", "close-source"}},
		{"FILE", "TOPIC", "file-input", []string{"close-destination"}},
		{"FILE", "FILE", "file-output", []string{"close-file-input"}},
		{"FILE", "FILE", "run", []string{"close-output", "close-file-input"}},
	}
	for _, test := range cases {
		t.Run(test.source+"-"+test.destination+"-"+test.fail, func(t *testing.T) {
			f := newEngineFixture()
			f.fail, f.store = test.fail, f.input
			engine := newEngine(f, f, f, engineLogger())
			if err := engine.Run(context.Background(), enginePlan(test.source, test.destination), config.Worker{}); !errors.Is(err, f.failure) {
				t.Fatal("original failure was lost", err)
			}
			var closed []string
			for _, stage := range f.trace {
				if strings.HasPrefix(stage, "close-") {
					closed = append(closed, stage)
				}
			}
			if !reflect.DeepEqual(closed, test.closed) {
				t.Fatalf("closed %v, want %v", closed, test.closed)
			}
		})
	}
}

func TestOneOffUsesOneGroupAndDeletesOnlyOnSuccess(t *testing.T) {
	for _, fail := range []string{"", "run", "delete-checkpoint"} {
		t.Run("failure-"+fail, func(t *testing.T) {
			f := newEngineFixture()
			f.fail = fail
			p := enginePlan("FILE", "FILE")
			p.OneOff = true
			err := newEngine(f, f, f, engineLogger()).Run(context.Background(), p, config.Worker{})
			if (err != nil) != (fail != "") {
				t.Fatal("incorrect completion result", err)
			}
			for _, group := range f.groups {
				if group != f.groups[0] || !strings.HasPrefix(group, "kafka-fwd2-") || group == p.Group {
					t.Fatal("inconsistent generated group", f.groups)
				}
			}
			deleted := strings.Contains(strings.Join(f.trace, ","), "delete-checkpoint")
			if deleted != (fail != "run") {
				t.Fatal("checkpoint cleanup happened at wrong stage", f.trace)
			}
		})
	}
}

func TestDestinationProgressRequiresConsistentFileIdentity(t *testing.T) {
	saved := map[int32]kafka.Progress{0: {FileID: "archive", Extent: 100}, 1: {FileID: "archive", Extent: 200}}
	progress, err := destinationProgress(saved)
	if err != nil || progress != (file.DestinationProgress{ID: "archive", Extent: 200, Exists: true}) {
		t.Fatal("durable extent was lost", progress, err)
	}
	saved[1] = kafka.Progress{FileID: "other", Extent: 200}
	if _, err := destinationProgress(saved); err == nil {
		t.Fatal("inconsistent destination identity accepted")
	}
}
