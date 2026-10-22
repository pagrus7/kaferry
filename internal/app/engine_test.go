package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"testing"

	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/file"
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
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
}

func (o *engineOutput) Close() {
	_ = o.fixture.stage("close-output")
}
func (*engineOutput) DurableOutput() (string, int64) { return "file-id", 100 }

type archiveEngineFiles struct{ *engineFixture }
type finalizedOutput struct{ *engineOutput }

func (f archiveEngineFiles) OpenOutput(plan.Plan, string, file.DestinationProgress, config.Worker) (file.OutputSink, error) {
	return finalizedOutput{f.output}, nil
}
func (o finalizedOutput) Finish() error { return o.fixture.stage("publish") }

func TestEngineReportsArchivePublicationFailureBeforeCleanup(t *testing.T) {
	// Given a runner that saved progress and an archive that fails final publication.
	f := newEngineFixture()
	f.fail = "publish"
	engine := newEngine(f, archiveEngineFiles{f}, f, engineLogger(), nil)
	// When finishing, then publication cannot be hidden behind the void Close method.
	err := engine.Run(t.Context(), enginePlan("FILE", "FILE"), config.Defaults(""), noopShutdown{})
	require.ErrorIs(t, err, f.failure)
	var exit *core.Exit
	require.ErrorAs(t, err, &exit)
	require.Equal(t, 1, exit.Code)
	require.Less(t, slices.Index(f.trace, "run"), slices.Index(f.trace, "publish"))
	require.Less(t, slices.Index(f.trace, "publish"), slices.Index(f.trace, "close-output"))
}

func TestEnginePublishesAfterRunnerCancellationAndPreservesBothErrors(t *testing.T) {
	for _, publicationFails := range []bool{false, true} {
		t.Run(fmt.Sprint(publicationFails), func(t *testing.T) {
			// Given a cancelled runner and, optionally, a separate publication failure.
			f := newEngineFixture()
			f.fail = "run"
			f.failure = context.Canceled
			publication := errors.New("publication failed")
			if publicationFails {
				f.onStage = func(stage string) {
					if stage == "publish" {
						f.fail = "publish"
						f.failure = publication
					}
				}
			}
			engine := newEngine(f, archiveEngineFiles{f}, f, engineLogger(), nil)
			// When the runner returns cancellation, then Finish still runs before releasing the output.
			err := engine.Run(t.Context(), enginePlan("FILE", "FILE"), config.Defaults(""), noopShutdown{})
			require.ErrorIs(t, err, context.Canceled)
			if publicationFails {
				require.ErrorIs(t, err, publication)
			}
			require.Less(t, slices.Index(f.trace, "run"), slices.Index(f.trace, "publish"))
			require.Less(t, slices.Index(f.trace, "publish"), slices.Index(f.trace, "close-output"))
		})
	}
}

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
			require.NoError(t, graph.Err())

			// When
			runErr := engine.Run(context.Background(), enginePlan(kinds[0], kinds[1]), config.Config{}, noopShutdown{})

			// Then
			require.NoError(t, runErr)
			if kinds[0] == "TOPIC" && kinds[1] == "FILE" {
				require.Same(t, f.output, f.input.output)
			}
			if kinds[0] == "FILE" {
				var wanted core.OutputProgress
				if kinds[1] == "FILE" {
					wanted = f.output
				}
				if wanted == nil {
					require.Nil(t, f.input.output)
				} else {
					require.Same(t, wanted, f.input.output)
				}
			}
			output := "topic-output"
			if kinds[1] == "FILE" {
				output = "file-output"
			}
			if kinds[0] == "TOPIC" {
				inputIndex, outputIndex := slices.Index(f.trace, "topic-input"), slices.Index(f.trace, output)
				require.NotEqual(t, -1, inputIndex)
				require.NotEqual(t, -1, outputIndex)
				require.Less(t, inputIndex, outputIndex)
			}
			closed := 0
			for _, stage := range f.trace {
				if stage == "close-output" {
					closed++
				}
			}
			require.Equal(t, 1, closed)
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
			require.ErrorIs(t, runErr, f.failure)
			var closed []string
			for _, stage := range f.trace {
				if strings.HasPrefix(stage, "close-") {
					closed = append(closed, stage)
				}
			}
			require.Equal(t, test.closed, closed)
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
			require.ErrorIs(t, err, fixture.failure)
			for _, closed := range []string{"close-topic-input", "close-output", "close-source", "close-destination"} {
				count := 0
				for _, action := range fixture.trace {
					if action == closed {
						count++
					}
				}
				require.EqualValues(t, 1, count)
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
	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, f.trace, 0)
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
			index := slices.Index(f.trace, stage)
			require.NotEqual(t, -1, index, "source acquisition stage missing")
			afterCancellation := f.trace[index+1:]
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, closed, afterCancellation)
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
			require.Equal(t, (fail != ""), (err != nil))
			require.NotEmpty(t, f.groups)
			for _, group := range f.groups {
				require.Equal(t, f.groups[0], group)
				require.Regexp(t, `^kaferry-[0-9a-f]{32}$`, group)
				require.NotEqual(t, p.Group, group)
			}
			deleted := slices.Contains(f.trace, "delete-checkpoint")
			require.Equal(t, (fail != "run"), deleted)
		})
	}
}

func TestDestinationProgressRequiresConsistentFileIdentity(t *testing.T) {
	// Given
	saved := map[int32]kafkaport.Progress{0: {FileID: "archive", Extent: 100}, 1: {FileID: "archive", Extent: 200}}

	// When
	progress, err := destinationProgress(saved)

	// Then
	require.NoError(t, err)
	require.Equal(t, (file.DestinationProgress{ID: "archive", Extent: 200, Exists: true}), progress)
	saved[1] = kafkaport.Progress{FileID: "other", Extent: 200}
	_, err = destinationProgress(saved)
	require.Error(t, err)
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
			require.ErrorAs(t, err, &coded)
			require.Equal(t, tc.want, coded.ExitCode())
			require.ErrorIs(t, err, cleanup)
			if tc.primary != nil {
				require.ErrorIs(t, err, tc.primary)
			}
			require.Contains(t, err.Error(), "close Kafka input")
		})
	}
}
