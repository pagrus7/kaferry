package franz

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
)

func TestProducerFencingPreservesPermanentFailureAndCancellation(t *testing.T) {
	for _, mode := range []string{"authorization", "canceled during initialization", "already canceled"} {
		t.Run(mode, func(t *testing.T) {
			// Given a permanent broker refusal or a canceled ownership attempt.
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			client := &fencingProbe{failure: kerr.TransactionalIDAuthorizationFailed}
			want, calls := error(kerr.TransactionalIDAuthorizationFailed), 1
			if mode == "canceled during initialization" {
				client.failure, client.cancel = net.ErrClosed, cancel
				want = context.Canceled
			}
			if mode == "already canceled" {
				cancel()
				want, calls = context.Canceled, 0
			}
			// When fencing fails, then it preserves the error and never starts an output transaction or keeps retrying cancellation.
			require.ErrorIs(t, fenceProducer(ctx, client), want)
			require.Equal(t, calls, client.calls)
		})
	}
}

type fencingProbe struct {
	TransactionClient
	failure     error
	cancel      context.CancelFunc
	calls       int
	deadline    time.Time
	hasDeadline bool
}

func (p *fencingProbe) ProducerID(ctx context.Context) (int64, int16, error) {
	p.calls++
	p.deadline, p.hasDeadline = ctx.Deadline()
	if p.cancel != nil {
		p.cancel()
	}
	return 0, 0, p.failure
}

func TestProducerFencingHasFiniteDefaultAndPreservesShorterDeadline(t *testing.T) {
	t.Run("default budget", func(t *testing.T) {
		// Given an archive caller with no execution deadline.
		probe := &fencingProbe{}
		start := time.Now()
		// When initialization reaches its dependency, then it supplies a finite ten-second budget.
		require.NoError(t, fenceProducer(context.Background(), probe))
		require.True(t, probe.hasDeadline)
		require.False(t, probe.deadline.Before(start.Add(10*time.Second)))
		require.False(t, probe.deadline.After(time.Now().Add(10*time.Second)))
	})
	t.Run("shorter caller budget", func(t *testing.T) {
		// Given repeated transient failures and a caller deadline shorter than the default.
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		deadline, _ := ctx.Deadline()
		probe := &fencingProbe{failure: net.ErrClosed}
		// When initialization retries, then it stops at the caller deadline without extending it.
		require.ErrorIs(t, fenceProducer(ctx, probe), context.DeadlineExceeded)
		require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
		require.Positive(t, probe.calls)
		require.Equal(t, deadline, probe.deadline)
	})
}
