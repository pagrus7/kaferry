package franz

import (
	"context"
	"fmt"
	"sync"

	core "github.com/pagrus7/kaferry/src/internal/copy"
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
	topic   string
	events  chan lifecycleEvent
	desired map[int32]int64
	mu      sync.Mutex
}

func newRebalanceBridge(topic string) *rebalanceBridge {
	return &rebalanceBridge{topic: topic, events: make(chan lifecycleEvent), desired: make(map[int32]int64)}
}
func (b *rebalanceBridge) notify(ctx context.Context, event lifecycleEvent) lifecycleAnswer {
	event.answer = make(chan lifecycleAnswer, 1)
	select {
	case b.events <- event:
	case <-ctx.Done():
		return lifecycleAnswer{err: ctx.Err()}
	}
	select {
	case result := <-event.answer:
		return result
	case <-ctx.Done():
		return lifecycleAnswer{err: ctx.Err()}
	}
}
func (b *rebalanceBridge) options(ctx context.Context) []kgo.Opt {
	return []kgo.Opt{
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, parts map[string][]int32) {
			result := b.notify(ctx, lifecycleEvent{ids: parts[b.topic]})
			if result.err != nil {
				return
			}
			b.mu.Lock()
			defer b.mu.Unlock()
			for _, a := range result.assignments {
				b.desired[a.Cursor.Partition] = a.Cursor.Position
			}
		}),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, parts map[string][]int32) {
			b.revoked(ctx, parts[b.topic], false)
		}),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, parts map[string][]int32) { b.revoked(ctx, parts[b.topic], true) }),
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
func (b *rebalanceBridge) revoked(ctx context.Context, ids []int32, lost bool) {
	_ = b.notify(ctx, lifecycleEvent{ids: ids, revoke: true, lost: lost})
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, id := range ids {
		delete(b.desired, id)
	}
}
