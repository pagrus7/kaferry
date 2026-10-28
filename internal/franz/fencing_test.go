package franz

import (
	"context"
	"net"
	"testing"

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
	failure error
	cancel  context.CancelFunc
	calls   int
}

func (p *fencingProbe) ProducerID(context.Context) (int64, int16, error) {
	p.calls++
	if p.cancel != nil {
		p.cancel()
	}
	return 0, 0, p.failure
}
