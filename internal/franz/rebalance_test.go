package franz

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/twmb/franz-go/pkg/kgo"
)

func bridgeClient(t *testing.T, b *rebalanceBridge) *kgo.Client {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	c, err := kgo.NewClient(append(b.options(), kgo.WithContext(ctx), kgo.ConsumerGroup("group"))...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.CloseAllowingRebalance)
	return c
}
func TestBridgeTransfersAssignmentAndLostCallbacksToTheCore(t *testing.T) {
	// Given a group callback bridge and no resolved starts.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	b := newRebalanceBridge(ctx, "input")
	c := bridgeClient(t, b)
	adjust := c.OptValue(kgo.AdjustFetchOffsetsFn).(func(context.Context, map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error))
	if _, err := adjust(ctx, map[string]map[int32]kgo.Offset{"input": {0: kgo.NewOffset().AtStart()}}); err == nil {
		t.Fatal("unresolved assignment fell back to a reset rule")
	}
	assigned := c.OptValue(kgo.OnPartitionsAssigned).(func(context.Context, *kgo.Client, map[string][]int32))
	done := make(chan struct{})
	go func() { assigned(ctx, c, map[string][]int32{"input": {0}}); close(done) }()
	// When the core resolves the callback, then the adjustment uses exactly its chosen start.
	event := <-b.events
	if event.revoke || !reflect.DeepEqual(event.ids, []int32{0}) {
		t.Fatal("assignment event changed", event)
	}
	event.answer <- lifecycleAnswer{assignments: []core.Assignment{{Cursor: core.Cursor{Partition: 0, Position: 7}}}}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("assignment callback remained blocked")
	}
	got, err := adjust(ctx, map[string]map[int32]kgo.Offset{"input": {0: kgo.NewOffset().AtStart()}})
	if err != nil || !reflect.DeepEqual(got["input"][0], kgo.NewOffset().At(7)) {
		t.Fatal("resolved start lost", got, err)
	}
	// When ownership is lost, then the core receives lost=true before desired state is removed.
	lost := c.OptValue(kgo.OnPartitionsLost).(func(context.Context, *kgo.Client, map[string][]int32))
	done = make(chan struct{})
	go func() { lost(ctx, c, map[string][]int32{"input": {0}}); close(done) }()
	event = <-b.events
	if !event.revoke || !event.lost {
		t.Fatal("lost ownership became an ordinary revoke")
	}
	event.answer <- lifecycleAnswer{}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("lost callback remained blocked")
	}
	if _, err := adjust(ctx, map[string]map[int32]kgo.Offset{"input": {0: kgo.NewOffset().AtStart()}}); err == nil {
		t.Fatal("lost start remained available")
	}
}
func TestBridgeCancellationReleasesCallbacksWithoutACorePoll(t *testing.T) {
	// Given a callback waiting while the core is draining instead of polling.
	ctx, cancel := context.WithCancel(t.Context())
	b := newRebalanceBridge(ctx, "input")
	done := make(chan lifecycleAnswer, 1)
	go func() { done <- b.notify(lifecycleEvent{}) }()
	// When Source.Close cancels the bridge, then it does not need a callback from another worker.
	cancel()
	select {
	case result := <-done:
		if !errors.Is(result.err, context.Canceled) {
			t.Fatal(result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("callback did not cancel")
	}
}
