package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/file"
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/plan"
	"go.uber.org/fx"
)

type engineFixture struct {
	trace   []string
	fail    string
	failure error
	input   *engineInput
	output  *engineOutput
	source  *engineSource
	store   kafkaport.SourceProgress
	groups  []string
	saved   map[int32]kafkaport.Progress
	onStage func(string)
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
	if f.onStage != nil {
		f.onStage(name)
	}
	if f.fail == name {
		return f.failure
	}
	return nil
}
func (f *engineFixture) Open(_ context.Context, endpoint plan.Endpoint) (kafkaport.Connection, error) {
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
func (f *engineFixture) New(source core.Source, sink core.Sink, store core.ProgressStore, p plan.Plan, _ config.Worker, _ core.StopObserver, _ core.Observer) Execution {
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

func (c *engineConnection) ClusterID(context.Context) (string, error) {
	return c.name + "-cluster", c.fixture.stage("cluster-" + c.name)
}
func (c *engineConnection) CheckMembers(context.Context, string) error {
	return c.fixture.stage("members")
}
func (c *engineConnection) ReadProgress(context.Context, string, string, plan.WindowSpec, int64) (map[int32]kafkaport.Progress, error) {
	return c.fixture.saved, c.fixture.stage("progress")
}
func (c *engineConnection) Source(_ context.Context, _ plan.Plan, _ string, _ config.Worker) (core.Source, kafkaport.SourceProgress, error) {
	return c.fixture.source, c.fixture.store, c.fixture.stage("topic-input")
}
func (c *engineConnection) Sink(context.Context, config.Worker) (core.Sink, error) {
	return c.fixture.output, c.fixture.stage("topic-output")
}
func (c *engineConnection) Close(context.Context) error { return c.fixture.stage("close-" + c.name) }

type engineSource struct {
	core.Source
	fixture     *engineFixture
	beforeClose func()
}

func (s *engineSource) Close() error {
	if s.beforeClose != nil {
		s.beforeClose()
	}
	return s.fixture.stage("close-topic-input")
}

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
	closed  bool
}

func (o *engineOutput) Close() {
	o.closed = true
	_ = o.fixture.stage("close-output")
}
func (*engineOutput) DurableOutput() (string, int64) { return "file-id", 100 }

func enginePlan(source, destination string) plan.Plan {
	return plan.Plan{Source: plan.Endpoint{Kind: source, Name: "source"}, Destination: plan.Endpoint{Kind: destination, Name: "destination"}, Group: "group", Start: "EARLIEST"}
}
func engineLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestRuntimeCompositionAcceptsReplacementFactories(t *testing.T) {
	// Given
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

			// When
			runErr := engine.Run(context.Background(), enginePlan(kinds[0], kinds[1]), config.Config{}, noopShutdown{})

			// Then
			if runErr != nil {
				t.Fatal(runErr)
			}
			if kinds[0] == "TOPIC" && kinds[1] == "FILE" && f.input.output != f.output {
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
			if kinds[0] == "TOPIC" && strings.Index(joined, "topic-input") > strings.Index(joined, output) {
				t.Fatal("Kafka consumer constructed after destination", f.trace)
			}
			if strings.Count(joined, "close-output") != 1 {
				t.Fatal("destination was not closed exactly once", f.trace)
			}
		})
	}
}

func TestEngineClosesOnlyAcquiredResourcesOnFailure(t *testing.T) {
	// Given
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
		{"TOPIC", "FILE", "members", []string{"close-source"}},
		{"TOPIC", "FILE", "progress", []string{"close-source"}},
		{"TOPIC", "FILE", "topic-input", []string{"close-source"}},
		{"TOPIC", "FILE", "file-output", []string{"close-topic-input", "close-source"}},
		{"TOPIC", "TOPIC", "topic-output", []string{"close-topic-input", "close-destination", "close-source"}},
		{"TOPIC", "TOPIC", "topic-input", []string{"close-destination", "close-source"}},
		{"TOPIC", "TOPIC", "run", []string{"close-output", "close-topic-input", "close-destination", "close-source"}},
		{"FILE", "TOPIC", "file-input", []string{"close-destination"}},
		{"FILE", "FILE", "file-output", []string{"close-file-input"}},
		{"FILE", "FILE", "run", []string{"close-output", "close-file-input"}},
	}
	for _, test := range cases {
		t.Run(test.source+"-"+test.destination+"-"+test.fail, func(t *testing.T) {
			f := newEngineFixture()
			f.fail, f.store = test.fail, f.input
			engine := newEngine(f, f, f, engineLogger(), nil)

			// When
			runErr := engine.Run(context.Background(), enginePlan(test.source, test.destination), config.Config{}, noopShutdown{})

			// Then
			if !errors.Is(runErr, f.failure) {
				t.Fatal("original failure was lost", runErr)
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

func TestEngineReportsCleanupFailureAfterSuccessfulCopy(t *testing.T) {
	for _, stage := range []string{"close-topic-input", "close-source", "close-destination"} {
		t.Run(stage, func(t *testing.T) {
			// Given a completed copy whose source or endpoint cleanup fails.
			fixture := newEngineFixture()
			fixture.fail, fixture.store = stage, fixture.input
			engine := newEngine(fixture, fixture, fixture, engineLogger(), nil)
			// When all acquired resources are closed after the copy.
			err := engine.Run(context.Background(), enginePlan("TOPIC", "TOPIC"), config.Config{}, noopShutdown{})
			// Then the failure is reported and each remaining resource is still closed once.
			if !errors.Is(err, fixture.failure) {
				t.Fatal("cleanup failure was hidden", err)
			}
			for _, closed := range []string{"close-topic-input", "close-output", "close-source", "close-destination"} {
				count := 0
				for _, action := range fixture.trace {
					if action == closed {
						count++
					}
				}
				if count != 1 {
					t.Fatal("cleanup stopped or closed a resource twice", fixture.trace)
				}
			}
		})
	}
}

func TestEngineSkipsAcquisitionWhenStartupIsCancelled(t *testing.T) {
	// Given a query cancelled before execution starts.
	f := newEngineFixture()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	// When preparing Kafka input and file output.
	err := newEngine(f, f, f, engineLogger(), nil).Run(ctx, enginePlan("TOPIC", "FILE"), config.Config{}, noopShutdown{})
	// Then no connection, file or saved progress is created.
	if !errors.Is(err, context.Canceled) || len(f.trace) != 0 {
		t.Fatal("cancelled startup acquired resources", err, f.trace)
	}
}

func TestStartupCancellationClosesInputWithoutCreatingOutput(t *testing.T) {
	for _, kind := range []string{"TOPIC", "FILE"} {
		t.Run(kind, func(t *testing.T) {
			// Given cancellation while the source adapter is being created.
			f := newEngineFixture()
			f.store = f.input
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			stage, closed := "topic-input", []string{"close-topic-input", "close-source"}
			if kind == "FILE" {
				stage, closed = "file-input", []string{"close-file-input"}
			}
			f.onStage = func(name string) {
				if name == stage {
					cancel()
				}
			}

			// When starting a copy into a file.
			err := newEngine(f, f, f, engineLogger(), nil).Run(ctx, enginePlan(kind, "FILE"), config.Config{}, noopShutdown{})

			// Then cancellation preserves its cause, closes acquired input, and creates no output.
			var afterCancellation []string
			for index, name := range f.trace {
				if name == stage {
					afterCancellation = f.trace[index+1:]
					break
				}
			}
			if !errors.Is(err, context.Canceled) || !reflect.DeepEqual(afterCancellation, closed) {
				t.Fatal("incorrect cancellation cleanup", err, f.trace)
			}
		})
	}
}

func TestOneOffUsesOneGroupAndDeletesOnlyOnSuccess(t *testing.T) {
	// Given
	for _, fail := range []string{"", "run", "delete-checkpoint"} {
		t.Run("failure-"+fail, func(t *testing.T) {
			f := newEngineFixture()
			f.fail = fail
			p := enginePlan("FILE", "FILE")
			p.OneOff = true

			// When
			err := newEngine(f, f, f, engineLogger(), nil).Run(context.Background(), p, config.Config{}, noopShutdown{})

			// Then
			if (err != nil) != (fail != "") {
				t.Fatal("incorrect completion result", err)
			}
			for _, group := range f.groups {
				if group != f.groups[0] || !strings.HasPrefix(group, "kaferry-") || group == p.Group {
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
	// Given
	saved := map[int32]kafkaport.Progress{0: {FileID: "archive", Extent: 100}, 1: {FileID: "archive", Extent: 200}}

	// When
	progress, err := destinationProgress(saved)

	// Then
	if err != nil || progress != (file.DestinationProgress{ID: "archive", Extent: 200, Exists: true}) {
		t.Fatal("durable extent was lost", progress, err)
	}
	saved[1] = kafkaport.Progress{FileID: "other", Extent: 200}
	if _, err := destinationProgress(saved); err == nil {
		t.Fatal("inconsistent destination identity accepted")
	}
}

func TestCleanupFailurePreservesTheFirstExitCode(t *testing.T) {
	for _, tc := range []struct {
		name    string
		primary error
		want    int
	}{
		{"copy succeeded", nil, 1},
		{"runtime failure", errors.New("broker unavailable"), 1},
		{"startup conflict", &kafkaport.Conflict{Err: errors.New("foreign progress")}, 2},
		{"runtime conflict", &core.Exit{Code: 1, Err: errors.New("changed topic")}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given a close operation that fails with a classified error after the primary outcome.
			cleanup := &kafkaport.Conflict{Err: errors.New("close failed")}
			// When combining cleanup with the copy result.
			err := core.CleanupResult(tc.primary, "close Kafka input", cleanup)
			// Then both causes remain available and cleanup cannot replace the primary exit code.
			var coded interface{ ExitCode() int }
			if !errors.As(err, &coded) || coded.ExitCode() != tc.want || !errors.Is(err, cleanup) || tc.primary != nil && !errors.Is(err, tc.primary) || !strings.Contains(err.Error(), "close Kafka input") {
				t.Fatal("cleanup changed outcome or hid its cause", err)
			}
		})
	}
}
