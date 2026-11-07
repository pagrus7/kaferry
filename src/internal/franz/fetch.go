package franz

import (
	"context"
	"errors"

	core "github.com/pagrus7/kaferry/src/internal/copy"
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
	fetched chan fetchResult
	done    chan struct{}
}

func newFetchPump(client recordPoller) *fetchPump {
	return &fetchPump{client: client, fetched: make(chan fetchResult), done: make(chan struct{})}
}
func (p *fetchPump) run(ctx context.Context) {
	defer close(p.done)
	defer p.client.AllowRebalance()
	for {
		p.client.AllowRebalance()
		fetch := p.client.PollRecords(ctx, 1)
		if ctx.Err() != nil {
			return
		}
		result := fetchResult{records: fetch.Records(), err: fetchFailure(fetch), done: make(chan struct{})}
		select {
		case p.fetched <- result:
		case <-ctx.Done():
			return
		}
		select {
		case <-result.done:
		case <-ctx.Done():
			return
		}
	}
}

func fetchFailure(fetches kgo.Fetches) error {
	var retry error
	for _, failure := range fetches.Errors() {
		if !errors.Is(transientFailure(failure.Err), core.ErrRecoverable) {
			return failure.Err
		}
		if retry == nil {
			retry = failure.Err
		}
	}
	return retry
}
