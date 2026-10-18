package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/inspect"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/pagrus7/kaferry/internal/sql"
	"github.com/stretchr/testify/require"
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
	require.Equal(m.t, "statement", input)
	return m.ast, m.stage("parse")
}
func (m *commandMocks) Load(path string) (config.Config, error) {
	require.Equal(m.t, "config.toml", path)
	return m.cfg, m.stage("load")
}
func (m *commandMocks) Build(ast sql.Statement, profiles plan.Profiles) (plan.Plan, error) {
	require.Equal(m.t, m.ast, ast)
	require.Equal(m.t, m.cfg.Connections, profiles)
	return m.plan, m.stage("build")
}
func (m *commandMocks) Run(ctx context.Context, p plan.Plan, cfg config.Config, _ Shutdown) error {
	require.True(m.t, ctx == m.ctx, "context identity changed")
	require.Equal(m.t, m.plan, p)
	require.Equal(m.t, m.cfg, cfg)
	return m.stage("execute")
}
func (m *commandMocks) Validate(p plan.Plan, worker config.Worker) error {
	require.Equal(m.t, m.plan, p)
	require.Equal(m.t, m.cfg.Worker, worker)
	return m.stage("validate")
}

func (m *commandMocks) Explain(ctx context.Context, p plan.Plan, worker config.Worker, _ core.StopObserver) (inspect.Report, error) {
	require.True(m.t, ctx == m.ctx, "context identity changed")
	require.Equal(m.t, m.plan, p)
	require.Equal(m.t, m.cfg.Worker, worker)
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
			require.Equal(t, want, m.calls)
			if failure == "" {
				require.NoError(t, err)
				require.Equal(t, m.plan.Group, report.Group)
			} else {
				require.ErrorIs(t, err, m.failure)
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
				require.ErrorIs(t, err, m.failure)
				var coded interface{ ExitCode() int }
				if index < 4 {
					require.ErrorAs(t, err, &coded)
					require.Equal(t, 2, coded.ExitCode())
				} else {
					require.True(t, err == m.failure, "execution error identity changed")
				}
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, wanted, m.calls)
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
		require.Same(t, failure, runErr)
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

func (s assignmentSource) Start(lifecycle core.Lifecycle, _ core.Observer) error {
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
	first := factory.New(source, nil, nil, enginePlan("FILE", "FILE"), config.Worker{}, core.NoopStopObserver{}, nil)
	second := factory.New(source, nil, nil, enginePlan("FILE", "FILE"), config.Worker{}, core.NoopStopObserver{}, nil)
	require.Len(t, progress.made, 2)
	require.NotSame(t, progress.made[0], progress.made[1])

	// When
	runErr := first.Run(context.Background())

	// Then
	require.ErrorIs(t, runErr, stop)
	require.EqualValues(t, 1, progress.made[0].assignments)
	require.Zero(t, progress.made[1].assignments)
	require.ErrorIs(t, second.Run(context.Background()), stop)
	require.EqualValues(t, 1, progress.made[0].assignments)
	require.EqualValues(t, 1, progress.made[1].assignments)
}

type noopShutdown struct{ core.NoopStopObserver }

func (noopShutdown) Configure(time.Duration) {}

func (noopShutdown) Deadline() time.Time { return time.Now().Add(time.Hour) }
