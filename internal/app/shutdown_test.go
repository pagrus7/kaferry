package app

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
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

type blockingInput struct{}

func (blockingInput) Read([]byte) (int, error) { select {} }

type blockingErrorOutput struct{}

func (blockingErrorOutput) Write([]byte) (int, error) { select {} }

func TestShutdownDeadlineCoversFinalErrorPrinting(t *testing.T) {
	if os.Getenv("KAFERRY_SHUTDOWN_HELPER") == "error" {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		budget := newShutdownBudget(ctx, 40*time.Millisecond, forceShutdown)
		_ = executeWithShutdown(ctx, []string{"run", "--sql", "x"}, nil, io.Discard, blockingErrorOutput{}, budget)
		t.Fatal("blocked error writer returned")
	}
	// Given an error returned from Command.Run, when stderr is blocked after a signal,
	// then the same process guard stays alive until printing and execution have both ended.
	runShutdownHelper(t, "error", "printing error")
}
func TestShutdownDeadlineAlsoCoversBlockedSQLInput(t *testing.T) {
	if os.Getenv("KAFERRY_SHUTDOWN_HELPER") == "input" {
		ctx, cancel := context.WithCancel(context.Background())
		budget := newShutdownBudget(ctx, 40*time.Millisecond, forceShutdown)
		cancel()
		_ = executeWithShutdown(ctx, []string{"run", "--file", "-"}, blockingInput{}, io.Discard, os.Stderr, budget)
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
	testName := map[string]string{"cleanup": "TestShutdownDeadlineStopsBlockedCleanup", "input": "TestShutdownDeadlineAlsoCoversBlockedSQLInput", "error": "TestShutdownDeadlineCoversFinalErrorPrinting"}[mode]
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+testName+"$")
	command.Env = append(os.Environ(), "KAFERRY_SHUTDOWN_HELPER="+mode)
	output, err := command.CombinedOutput()
	var exit *exec.ExitError
	if ctx.Err() != nil || !errors.As(err, &exit) || exit.ExitCode() != 1 || !strings.Contains(string(output), "shutdown timed out while "+stage) {
		t.Fatalf("deadline did not force a clear exit: err=%v output=%s", err, output)
	}
}
