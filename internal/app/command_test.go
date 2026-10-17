package app

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/inspect"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/pagrus7/kaferry/internal/sql"
)

type commandMocks struct {
	t       *testing.T
	calls   []string
	fail    string
	failure error
	ctx     context.Context
	cfg     config.Config
	ast     sql.Statement
	plan    plan.Plan
}

func (m *commandMocks) stage(name string) error {
	m.calls = append(m.calls, name)
	if m.fail == name {
		return m.failure
	}
	return nil
}
func (m *commandMocks) Parse(input string) (sql.Statement, error) {
	if input != "statement" {
		m.t.Fatal("wrong statement", input)
	}
	return m.ast, m.stage("parse")
}
func (m *commandMocks) Load(path string) (config.Config, error) {
	if path != "config.toml" {
		m.t.Fatal("wrong config path", path)
	}
	return m.cfg, m.stage("load")
}
func (m *commandMocks) Build(ast sql.Statement, profiles plan.Profiles) (plan.Plan, error) {
	if !reflect.DeepEqual(ast, m.ast) || !reflect.DeepEqual(profiles, m.cfg.Connections) {
		m.t.Fatal("planning input changed")
	}
	return m.plan, m.stage("build")
}
func (m *commandMocks) Run(ctx context.Context, p plan.Plan, worker config.Worker, _ core.StopObserver) error {
	if ctx != m.ctx || !reflect.DeepEqual(p, m.plan) || worker != m.cfg.Worker {
		m.t.Fatal("execution input changed")
	}
	return m.stage("execute")
}
func (m *commandMocks) Validate(p plan.Plan, worker config.Worker) error {
	if !reflect.DeepEqual(p, m.plan) || worker != m.cfg.Worker {
		m.t.Fatal("validation input changed")
	}
	return m.stage("validate")
}

func (m *commandMocks) Explain(ctx context.Context, p plan.Plan, worker config.Worker, _ core.StopObserver) (inspect.Report, error) {
	if ctx != m.ctx || !reflect.DeepEqual(p, m.plan) || worker != m.cfg.Worker {
		m.t.Fatal("inspection input changed")
	}
	return inspect.Report{Version: 1, Group: p.Group}, m.stage("inspect")
}

func TestExplainPlansAndInspectsWithoutExecuting(t *testing.T) {
	for _, failure := range []string{"", "parse", "inspect"} {
		t.Run(failure, func(t *testing.T) {
			// Given independent execution and inspection boundaries.
			m := &commandMocks{t: t, ctx: t.Context(), cfg: config.Defaults("home"), plan: enginePlan("FILE", "FILE"), fail: failure, failure: &core.Exit{Code: 1, Err: errors.New("failed")}}
			// When explaining through the same preparation stages as run.
			report, err := newCommand(m, m, m, m, m, m, nil).Explain(m.ctx, "statement", "config.toml", noopShutdown{})
			// Then only the inspector executes, and failures retain their cause.
			want := []string{"parse", "load", "build", "validate", "inspect"}
			if failure == "parse" {
				want = want[:1]
			}
			if !reflect.DeepEqual(m.calls, want) || failure == "" && (err != nil || report.Group != m.plan.Group) || failure != "" && !errors.Is(err, m.failure) {
				t.Fatal(report, err, m.calls)
			}
		})
	}
}
func TestCommandPlansBeforeExecutingAndStopsOnFailure(t *testing.T) {
	// Given
	stages := []string{"parse", "load", "build", "validate", "execute"}
	for index, fail := range append(append([]string{}, stages...), "") {
		t.Run("failure-"+fail, func(t *testing.T) {
			m := &commandMocks{t: t, fail: fail, failure: errors.New("failure"), ctx: context.Background(), cfg: config.Defaults("home"), ast: sql.Statement{Source: sql.Endpoint{Name: "input"}}, plan: enginePlan("FILE", "FILE")}

			// When
			err := newCommand(m, m, m, m, m, nil, nil).Run(m.ctx, "statement", "config.toml", noopShutdown{})
			wanted := stages

			// Then
			if fail != "" {
				wanted = stages[:index+1]
				if !errors.Is(err, m.failure) {
					t.Fatal("failure lost", err)
				}
				var coded interface{ ExitCode() int }
				if index < 4 && (!errors.As(err, &coded) || coded.ExitCode() != 2) {
					t.Fatal("planning failure has wrong exit code", err)
				}
				if index == 4 && err != m.failure {
					t.Fatal("execution failure was reclassified", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(m.calls, wanted) {
				t.Fatal("unexpected component calls", m.calls, wanted)
			}
		})
	}
}
func TestCommandPreservesExecutionExitCode(t *testing.T) {
	// Given
	for _, code := range []int{1, 2, 3} {
		failure := &core.Exit{Code: code, Err: errors.New("execution failure")}
		m := &commandMocks{t: t, fail: "execute", failure: failure, ctx: context.Background()}

		// When
		runErr := newCommand(m, m, m, m, m, nil, nil).Run(m.ctx, "statement", "config.toml", noopShutdown{})

		// Then
		if runErr != failure {
			t.Fatal("execution classification lost", runErr)
		}
	}
}

type mockProgress struct {
	core.ProgressTracker
	assignments int
}

func (p *mockProgress) Assign(core.Cursor) { p.assignments++ }

type mockProgressFactory struct{ made []*mockProgress }

func (f *mockProgressFactory) New() core.ProgressTracker {
	p := &mockProgress{}
	f.made = append(f.made, p)
	return p
}

type assignmentSource struct {
	core.Source
	failure error
}

func (s assignmentSource) Start(lifecycle core.Lifecycle) error {
	if err := lifecycle.Assign([]core.Assignment{{Cursor: core.Cursor{Partition: 0}}}); err != nil {
		return err
	}
	return s.failure
}
func TestRunnerFactoryInjectsFreshProgressPerExecution(t *testing.T) {
	// Given
	progress := &mockProgressFactory{}
	factory := newRunnerFactory(core.RealClock{}, engineLogger(), progress)
	stop := errors.New("stop after assignment")
	source := assignmentSource{failure: stop}
	first := factory.New(source, nil, nil, enginePlan("FILE", "FILE"), config.Worker{}, core.NoopStopObserver{})
	second := factory.New(source, nil, nil, enginePlan("FILE", "FILE"), config.Worker{}, core.NoopStopObserver{})
	if len(progress.made) != 2 || progress.made[0] == progress.made[1] {
		t.Fatal("progress state shared between executions")
	}

	// When
	runErr := first.Run(context.Background())

	// Then
	if !errors.Is(runErr, stop) {
		t.Fatal(runErr)
	}
	if progress.made[0].assignments != 1 || progress.made[1].assignments != 0 {
		t.Fatal("first execution used wrong tracker")
	}
	if err := second.Run(context.Background()); !errors.Is(err, stop) {
		t.Fatal(err)
	}
	if progress.made[0].assignments != 1 || progress.made[1].assignments != 1 {
		t.Fatal("second execution used wrong tracker")
	}
}

type noopShutdown struct{ core.NoopStopObserver }

func (noopShutdown) Configure(time.Duration) {}
