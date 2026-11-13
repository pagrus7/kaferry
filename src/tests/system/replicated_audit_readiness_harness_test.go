//go:build integration && fault && harness

package compat

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReplicatedAuditReadinessReadsEveryBrokerConcurrently(t *testing.T) {
	// Given three blocked log reads and the same two-second polling context.
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	targets, rows := readinessTargets(), readinessRows()
	started, finished := make(chan string, 3), make(chan string, 3)
	release := map[string]chan struct{}{"one": make(chan struct{}), "two": make(chan struct{}), "three": make(chan struct{})}
	payloads := map[string][]byte{"one": rows[0], "two": rows[1], "three": rows[2]}
	result := make(chan readinessReadResult, 1)
	go func() {
		data, err := readAuditRequests(ctx, targets, func(query context.Context, args ...string) ([]byte, error) {
			if query != ctx || len(args) != 5 || !slices.Equal(args, []string{"exec", args[1], "sh", "-ec", requestLogFiles}) {
				return nil, errors.New("read changed its complete-log command or polling context")
			}
			started <- args[1]
			select {
			case <-release[args[1]]:
				finished <- args[1]
				return payloads[args[1]], nil
			case <-query.Done():
				return nil, query.Err()
			}
		})
		result <- readinessReadResult{data, err}
	}()

	// When every broker has started, finish them in reverse target order.
	awaitReadinessStarts(ctx, t, started)
	for _, target := range slices.Backward(targets) {
		close(release[target.Container])
		select {
		case container := <-finished:
			require.Equal(t, target.Container, container)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	got := awaitReadinessResult(ctx, t, result)
	// Then every raw result survives in deterministic target order, and the
	// combined actual generation/progress observations can establish readiness.
	require.NoError(t, got.err)
	require.Equal(t, bytes.Join(rows, nil), got.data)
	generation, ready, err := currentGenerationSaved(got.data, "worker", "group", "member", []int32{0, 1}, nil, time.Time{})
	require.NoError(t, err)
	require.EqualValues(t, 2, generation)
	require.True(t, ready)
}

func TestReplicatedAuditReadinessRejectsIncompleteBrokerReads(t *testing.T) {
	// Given complete positive observations on one broker, another broker's
	// missing, failed, oversized or malformed raw data must still prevent proof.
	for _, mode := range []string{"failed", "empty", "oversized", "malformed"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			data, readErr := readAuditRequests(ctx, readinessTargets(), func(_ context.Context, args ...string) ([]byte, error) {
				if args[1] != "three" {
					return bytes.Join(readinessRows(), nil), nil
				}
				switch mode {
				case "failed":
					return bytes.Join(readinessRows(), nil), errors.New("log command failed")
				case "empty":
					return nil, nil
				case "oversized":
					return bytes.Repeat([]byte("x"), auditCaptureLimit+1), nil
				default:
					return []byte("Completed request:{\n"), nil
				}
			})
			// When complete logs are required, then partial broker success cannot
			// mask failed collection, and malformed records still reach parsing.
			if mode != "malformed" {
				require.ErrorContains(t, readErr, "broker=3")
				require.Contains(t, readErr.Error(), "elapsed=")
				require.Contains(t, readErr.Error(), "bytes=")
				require.Contains(t, readErr.Error(), "context=<nil>")
				require.Nil(t, data, "failed collection cannot expose a partial passing audit")
				return
			}
			require.NoError(t, readErr)
			_, ready, parseErr := currentGenerationSaved(data, "worker", "group", "member", []int32{0, 1}, nil, time.Time{})
			require.Error(t, parseErr)
			require.False(t, ready)
		})
	}
}

func TestReplicatedAuditReadinessSharesCancellation(t *testing.T) {
	// Given all three commands waiting on the caller's polling context.
	deadline, timeout := context.WithTimeout(t.Context(), 2*time.Second)
	defer timeout()
	ctx, cancel := context.WithCancelCause(deadline)
	defer cancel(nil)
	started, finished := make(chan string, 3), make(chan string, 3)
	result := make(chan readinessReadResult, 1)
	go func() {
		data, err := readAuditRequests(ctx, readinessTargets(), func(query context.Context, args ...string) ([]byte, error) {
			started <- args[1]
			<-query.Done()
			finished <- args[1]
			return []byte("available prefix\n"), errors.New("signal: killed")
		})
		result <- readinessReadResult{data, err}
	}()
	awaitReadinessStarts(ctx, t, started)
	// When that shared context is canceled, then every command joins before
	// returning an attributed error; no replacement context or retry is used.
	cause := errors.New("poll deadline exhausted")
	cancel(cause)
	got := awaitReadinessResult(deadline, t, result)
	require.ErrorIs(t, got.err, cause)
	require.Nil(t, got.data)
	require.Len(t, finished, 3)
	for _, target := range readinessTargets() {
		require.Contains(t, got.err.Error(), fmt.Sprintf("broker=%d", target.Broker))
	}
	require.Contains(t, got.err.Error(), "context=poll deadline exhausted")
	require.Contains(t, got.err.Error(), "signal: killed")
}
