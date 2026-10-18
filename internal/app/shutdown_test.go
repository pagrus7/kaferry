package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
)

func TestShutdownBudgetUsesConfiguredTimeoutAndCurrentStage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Given a signal watcher with a long default and a short explicit configuration.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		exited := make(chan string, 1)
		budget := newShutdownBudget(ctx, time.Hour, func(stage string) { exited <- stage })
		defer budget.Done()
		budget.Configure(20 * time.Millisecond)
		budget.Stage("saving progress")
		// When cancellation starts the configured shutdown budget.
		cancel()
		synctest.Wait()
		time.Sleep(20 * time.Millisecond)
		synctest.Wait()
		// Then the timeout reports the current blocked operation.
		require.Len(t, exited, 1)
		require.Equal(t, "saving progress", <-exited)
	})
}

func TestShutdownReconfigurationKeepsTheOriginalStart(t *testing.T) {
	for _, timeout := range []time.Duration{5 * time.Second, 20 * time.Second} {
		t.Run(timeout.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				// Given shutdown already started with a ten-second allowance.
				exited := make(chan string, 1)
				budget := newShutdownBudget(context.Background(), 10*time.Second, func(stage string) { exited <- stage })
				defer budget.Done()
				started := time.Now()
				budget.Begin()
				time.Sleep(3 * time.Second)
				// When configuration shortens or extends the allowance after shutdown begins.
				budget.Configure(timeout)
				budget.Stage("closing input")
				// Then the deadline remains start plus timeout, including the elapsed three seconds.
				require.Equal(t, started.Add(timeout), budget.Deadline())
				time.Sleep(timeout - 3*time.Second - time.Nanosecond)
				synctest.Wait()
				require.Empty(t, exited)
				time.Sleep(time.Nanosecond)
				synctest.Wait()
				require.Len(t, exited, 1)
				require.Equal(t, "closing input", <-exited)
			})
		})
	}
}

func TestCompletedShutdownBudgetCannotBeRearmed(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run(fmt.Sprint(active), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				// Given a command with an idle or already active shutdown timer.
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				exited := make(chan string, 1)
				budget := newShutdownBudget(ctx, time.Second, func(stage string) { exited <- stage })
				if active {
					budget.Begin()
					time.Sleep(500 * time.Millisecond)
				}
				// When cleanup finishes before later cancellation and repeated shutdown calls.
				budget.Done()
				budget.Begin()
				budget.Configure(time.Millisecond)
				budget.Done()
				cancel()
				time.Sleep(2 * time.Second)
				synctest.Wait()
				// Then completion suppresses an active timer and any later attempt to begin.
				require.Empty(t, exited)
			})
		})
	}
}

func TestShutdownDeadlineStopsBlockedCleanup(t *testing.T) {
	if os.Getenv("KAFERRY_SHUTDOWN_HELPER") == "cleanup" {
		f := newEngineFixture()
		f.store = f.input
		f.source.beforeClose = func() { select {} }
		budget := newShutdownBudget(context.Background(), 40*time.Millisecond, forceShutdown)
		_ = newEngine(f, f, f, engineLogger(), nil).Run(context.Background(), enginePlan("TOPIC", "FILE"), config.Config{}, budget)
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

func (failedStartupSource) Start(core.Lifecycle, core.Observer) error {
	return errors.New("checkpoint write failed")
}

func TestShutdownDeadlineCoversSummaryAfterStartupFailure(t *testing.T) {
	if os.Getenv("KAFERRY_SHUTDOWN_HELPER") == "summary" {
		budget := newShutdownBudget(context.Background(), 40*time.Millisecond, forceShutdown)
		logger := slog.New(slog.NewTextHandler(blockingErrorOutput{}, nil))
		runner := core.NewRunner(failedStartupSource{}, nil, nil, core.RealClock{}, logger, plan.Plan{}, config.Worker{}, nil, budget, nil)
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
		_ = newEngine(fixture, fixture, fixture, engineLogger(), nil).Run(context.Background(), p, config.Config{}, budget)
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
	require.Nil(t, ctx.Err())
	require.ErrorAs(t, err, &exit)
	require.EqualValues(t, 1, exit.ExitCode())
	require.Contains(t, string(output), "shutdown timed out while "+stage)
}

func TestForcedShutdownExitsWithAFullUndrainedStderrPipe(t *testing.T) {
	if os.Getenv("KAFERRY_SHUTDOWN_HELPER") == "full-stderr" {
		forceShutdown("writing blocked stderr")
		t.Fatal("forced shutdown returned")
	}
	// Given the subprocess's actual stderr pipe filled to capacity and left undrained.
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	defer reader.Close()
	defer writer.Close()
	fd := int(writer.Fd())
	require.NoError(t, syscall.SetNonblock(fd, true))
	for {
		_, err = syscall.Write(fd, make([]byte, 64<<10))
		if errors.Is(err, syscall.EAGAIN) {
			break
		}
		require.NoError(t, err)
	}
	require.NoError(t, syscall.SetNonblock(fd, false))
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+t.Name()+"$")
	command.Env = append(os.Environ(), "KAFERRY_SHUTDOWN_HELPER=full-stderr")
	command.Stderr = writer
	// When forced shutdown tries to print its best-effort diagnostic to that full pipe.
	err = command.Run()
	// Then blocked diagnostic output cannot prevent exit 1.
	require.NoError(t, ctx.Err())
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit)
	require.Equal(t, 1, exit.ExitCode())
}
