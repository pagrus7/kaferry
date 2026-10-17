package franz

import (
	"context"
	"fmt"
	"sync"

	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/twmb/franz-go/pkg/kgo"
)

type lifecycleEvent struct {
	ids          []int32
	revoke, lost bool
	answer       chan lifecycleAnswer
}
type lifecycleAnswer struct {
	assignments []core.Assignment
	err         error
}

// rebalanceBridge transfers callbacks to Poll. Only callbacks access desired.
type rebalanceBridge struct {
	ctx     context.Context
	topic   string
	events  chan lifecycleEvent
	desired map[int32]int64
	mu      sync.Mutex
}

func newRebalanceBridge(ctx context.Context, topic string) *rebalanceBridge {
	return &rebalanceBridge{ctx: ctx, topic: topic, events: make(chan lifecycleEvent), desired: make(map[int32]int64)}
}
func (b *rebalanceBridge) notify(event lifecycleEvent) lifecycleAnswer {
	event.answer = make(chan lifecycleAnswer, 1)
	select {
	case b.events <- event:
	case <-b.ctx.Done():
		return lifecycleAnswer{err: b.ctx.Err()}
	}
	select {
	case result := <-event.answer:
		return result
	case <-b.ctx.Done():
		return lifecycleAnswer{err: b.ctx.Err()}
	}
}
func (b *rebalanceBridge) options() []kgo.Opt {
	return []kgo.Opt{
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, parts map[string][]int32) {
			result := b.notify(lifecycleEvent{ids: parts[b.topic]})
			if result.err != nil {
				return
			}
			b.mu.Lock()
			defer b.mu.Unlock()
			for _, a := range result.assignments {
				b.desired[a.Cursor.Partition] = a.Cursor.Position
			}
		}),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, parts map[string][]int32) { b.revoked(parts[b.topic], false) }),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, parts map[string][]int32) { b.revoked(parts[b.topic], true) }),
		kgo.AdjustFetchOffsetsFn(func(_ context.Context, offsets map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
			b.mu.Lock()
			defer b.mu.Unlock()
			for topic, parts := range offsets {
				if topic != b.topic {
					return nil, fmt.Errorf("assignment names an unexpected topic")
				}
				for id := range parts {
					position, ok := b.desired[id]
					if !ok {
						return nil, fmt.Errorf("assignment has no resolved start for partition %d", id)
					}
					parts[id] = kgo.NewOffset().At(position)
				}
			}
			return offsets, nil
		}),
	}
}
func (b *rebalanceBridge) revoked(ids []int32, lost bool) {
	_ = b.notify(lifecycleEvent{ids: ids, revoke: true, lost: lost})
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, id := range ids {
		delete(b.desired, id)
	}
}
