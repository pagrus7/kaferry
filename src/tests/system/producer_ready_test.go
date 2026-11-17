//go:build integration && fault && load

package compat

import (
	"context"
	"time"

	"github.com/pagrus7/kaferry/src/tests/system/faults"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func initializeProducerID(ctx context.Context, client *faults.MetadataClient, broker int32) error {
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		response, err := client.RequestBroker(ctx, broker, kmsg.NewPtrInitProducerIDRequest())
		if err == nil {
			err = kerr.ErrorForCode(response.(*kmsg.InitProducerIDResponse).ErrorCode)
			if !kerr.IsRetriable(err) {
				return err
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}
