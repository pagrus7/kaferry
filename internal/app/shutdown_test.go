package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
)

func TestShutdownBudgetUsesConfiguredTimeoutAndCurrentStage(t *testing.T) {
	// Given a signal watcher with a long default and a short explicit configuration.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	exited := make(chan string, 1)
	budget := newShutdownBudget(ctx, time.Hour, func(stage string) { exited <- stage })
	defer budget.Done()
	budget.Configure(20 * time.Millisecond)
	budget.Stage("saving progress")
	// When a signal starts shutdown, then the configured budget reports the blocked operation.
	cancel()
	select {
	case stage := <-exited:
		if stage != "saving progress" {
			t.Fatal("wrong stage", stage)
		}
	case <-time.After(time.Second):
		t.Fatal("configured shutdown budget ignored")
	}
}
func TestCompletedShutdownBudgetCannotBeRearmed(t *testing.T) {
	// Given a completed command whose cancellation can arrive later.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	exited := make(chan string, 1)
	budget := newShutdownBudget(ctx, time.Millisecond, func(stage string) { exited <- stage })
	budget.Done()
	// When cleanup and cancellation race after completion, then no exit is scheduled.
	budget.Begin()
	budget.Done()
	cancel()
	select {
	case stage := <-exited:
		t.Fatal("completed query armed an exit", stage)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestShutdownDeadlineStopsBlockedCleanup(t *testing.T) {
	if os.Getenv("KAFERRY_SHUTDOWN_HELPER") == "cleanup" {
		f := newEngineFixture()
		f.store = f.input
		f.source.beforeClose = func() { select {} }
		budget := newShutdownBudget(context.Background(), 40*time.Millisecond, forceShutdown)
		_ = newEngine(f, f, f, engineLogger()).Run(context.Background(), enginePlan("TOPIC", "FILE"), config.Worker{}, budget)
		t.Fatal("blocked cleanup returned")
	}
	// Given a close operation that never returns, when normal completion starts cleanup,
	// then the process exits 1 without relying on a cancellable Kafka/filesystem call.
	runShutdownHelper(t, "cleanup", "closing Kafka input")
}

type blockingInput struct{ cancel context.CancelFunc }

func (b blockingInput) Read([]byte) (int, error) { b.cancel(); select {} }

type blockingErrorOutput struct{}

func (blockingErrorOutput) Write([]byte) (int, error) { select {} }

func TestShutdownDeadlineCoversFinalErrorPrinting(t *testing.T) {
	if os.Getenv("KAFERRY_SHUTDOWN_HELPER") == "error" {
		ctx := context.Background()
		budget := newShutdownBudget(ctx, 40*time.Millisecond, forceShutdown)
		_ = executeWithShutdown(ctx, []string{"run", "--sql", "x"}, nil, io.Discard, blockingErrorOutput{}, budget)
		t.Fatal("blocked error writer returned")
	}
	// Given an error returned from Command.Run, when stderr is blocked without a signal,
	// then the same process guard stays alive until printing and execution have both ended.
	runShutdownHelper(t, "error", "printing error")
}

type failedStartupSource struct{ core.Source }

func (failedStartupSource) Start(core.Lifecycle) error { return errors.New("checkpoint write failed") }

func TestShutdownDeadlineCoversSummaryAfterStartupFailure(t *testing.T) {
	if os.Getenv("KAFERRY_SHUTDOWN_HELPER") == "summary" {
		budget := newShutdownBudget(context.Background(), 40*time.Millisecond, forceShutdown)
		logger := slog.New(slog.NewTextHandler(blockingErrorOutput{}, nil))
		runner := core.NewRunner(failedStartupSource{}, nil, nil, core.RealClock{}, logger, plan.Plan{}, config.Worker{}, nil, budget)
		_ = runner.Run(context.Background())
		t.Fatal("blocked summary writer returned")
	}
	// Given startup fails before reading, when the summary writer blocks without a signal,
	// then the shutdown deadline still ends the process with a clear error.
	runShutdownHelper(t, "summary", "printing copy summary")
}

func TestShutdownDeadlineNamesBlockedCheckpointDeletion(t *testing.T) {
	if os.Getenv("KAFERRY_SHUTDOWN_HELPER") == "checkpoint" {
		fixture := newEngineFixture()
		fixture.onStage = func(stage string) {
			if stage == "delete-checkpoint" {
				select {}
			}
		}
		p := enginePlan("FILE", "FILE")
		p.OneOff = true
		budget := newShutdownBudget(context.Background(), 40*time.Millisecond, forceShutdown)
		_ = newEngine(fixture, fixture, fixture, engineLogger()).Run(context.Background(), p, config.Worker{}, budget)
		t.Fatal("blocked checkpoint deletion returned")
	}
	// Given a completed one-off copy, when checkpoint deletion blocks,
	// then the timeout names that operation and bounds cleanup without a signal.
	runShutdownHelper(t, "checkpoint", "deleting one-off checkpoint")
}
func TestShutdownDeadlineAlsoCoversBlockedSQLInput(t *testing.T) {
	if os.Getenv("KAFERRY_SHUTDOWN_HELPER") == "input" {
		ctx, cancel := context.WithCancel(context.Background())
		budget := newShutdownBudget(ctx, 40*time.Millisecond, forceShutdown)
		_ = executeWithShutdown(ctx, []string{"run", "--file", "-"}, blockingInput{cancel: cancel}, io.Discard, os.Stderr, budget)
		t.Fatal("blocked input returned")
	}
	// Given stdin that never returns, when a signal arrives before parsing SQL,
	// then the default process budget still bounds the command.
	runShutdownHelper(t, "input", "reading SQL input")
}
func runShutdownHelper(t *testing.T, mode, stage string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+t.Name()+"$")
	command.Env = append(os.Environ(), "KAFERRY_SHUTDOWN_HELPER="+mode)
	output, err := command.CombinedOutput()
	var exit *exec.ExitError
	if ctx.Err() != nil || !errors.As(err, &exit) || exit.ExitCode() != 1 || !strings.Contains(string(output), "shutdown timed out while "+stage) {
		t.Fatalf("deadline did not force a clear exit: err=%v output=%s", err, output)
	}
}
