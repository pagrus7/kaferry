package franz

import (
	"context"
	"errors"

	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/twmb/franz-go/pkg/kgo"
)

type fetchResult struct {
	records []*kgo.Record
	err     error
	done    chan struct{}
}
type recordPoller interface {
	PollRecords(context.Context, int) kgo.Fetches
	AllowRebalance()
}
type fetchPump struct {
	client  recordPoller
	ctx     context.Context
	fetched chan fetchResult
	done    chan struct{}
}

func newFetchPump(ctx context.Context, client recordPoller) *fetchPump {
	return &fetchPump{client: client, ctx: ctx, fetched: make(chan fetchResult), done: make(chan struct{})}
}
func (p *fetchPump) run() {
	defer close(p.done)
	defer p.client.AllowRebalance()
	for {
		p.client.AllowRebalance()
		fetch := p.client.PollRecords(p.ctx, 1)
		if p.ctx.Err() != nil {
			return
		}
		result := fetchResult{records: fetch.Records(), err: fetchFailure(fetch), done: make(chan struct{})}
		select {
		case p.fetched <- result:
		case <-p.ctx.Done():
			return
		}
		select {
		case <-result.done:
		case <-p.ctx.Done():
			return
		}
	}
}

func fetchFailure(fetches kgo.Fetches) error {
	var retry error
	for _, failure := range fetches.Errors() {
		var recoverable *core.Recoverable
		if !errors.As(transientFailure(failure.Err), &recoverable) {
			return failure.Err
		}
		if retry == nil {
			retry = failure.Err
		}
	}
	return retry
}
