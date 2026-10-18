//go:build integration

package compat

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestCancelledFixturePublishDoesNotWaitForReportReader(t *testing.T) {
	// Given cancelled publication with no receiver for its delivery report.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	results := make(chan kgo.ProduceResult)
	returned := make(chan struct{})
	// When publication reports its cancellation.
	go func() {
		deliveryReporter(ctx, results)(&kgo.Record{Topic: "unused"}, context.Canceled)
		close(returned)
	}()
	// Then cleanup can finish without a delivery-report reader.
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("cancelled publication waited for a delivery-report receiver")
	}
}
