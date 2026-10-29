package app

import (
	"context"
	"github.com/pagrus7/kaferry/internal/config"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"
	"sync/atomic"
	"testing"
	"time"
)

func TestAmbiguousProduceHonorsDeliveryDeadlineAndCannotSkip(t *testing.T) {
	// Given output accepted by the broker whose produce acknowledgements are lost.
	source := kfake.MustCluster(kfake.NumBrokers(1))
	defer source.Close()
	gate := &commitReplyGate{apiKey: uint16(kmsg.Produce)}
	destination := kfake.MustCluster(kfake.NumBrokers(1), kfake.ListenFn(gate.listen))
	defer destination.Close()
	topic(t, source.ListenAddrs(), "input", 1, false)
	topic(t, destination.ListenAddrs(), "output", 1, false)
	topic(t, destination.ListenAddrs(), "progress", 1, true)
	seed(t, source.ListenAddrs(), 1, 2)
	metadata, err := kadm.NewClient(client(t, destination.ListenAddrs())).Metadata(testContext(t), "output")
	require.NoError(t, err)
	id := metadata.Topics["output"].ID
	var fault atomic.Bool
	fault.Store(true)
	destination.ControlKey(int16(kmsg.Produce), func(request kmsg.Request) (kmsg.Response, error, bool) {
		destination.KeepControl()
		for _, topic := range request.(*kmsg.ProduceRequest).Topics {
			if fault.Load() && (topic.Topic == "output" || topic.TopicID == id) {
				gate.after.Store(1)
			}
		}
		return nil, nil, false
	})
	p := query(t, source.ListenAddrs(), destination.ListenAddrs(), "WHERE RECORD_OFFSET < LATEST", "OPTIONS(ON_ERROR=SKIP)")
	cfg := config.Defaults("")
	cfg.Worker.RecordCap = 1
	cfg.Worker.PartitionRecordCap = 1
	cfg.Worker.DeliveryTimeout = "100ms"
	// When delivery times out, then the ambiguous transaction fails, rather than skipping its input.
	started := time.Now()
	err = Run(testContext(t), p, cfg, Options{ProgressTopic: "progress"})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Greater(t, gate.dropped.Load(), int64(0))
	require.Less(t, time.Since(started), 3*time.Second)
	require.Empty(t, output(t, destination.ListenAddrs(), 1))
	// A replacement fences the abandoned transaction and copies both records once.
	fault.Store(false)
	gate.after.Store(0)
	cfg.Worker.DeliveryTimeout = "5s"
	require.NoError(t, Run(testContext(t), p, cfg, Options{ProgressTopic: "progress"}))
	require.Equal(t, map[string]int{"0:0": 1, "0:1": 1}, output(t, destination.ListenAddrs(), 1))
}
