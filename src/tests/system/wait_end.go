//go:build integration

package compat

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

func waitEnd(t *testing.T, a *kadm.Client, topic string, end int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		result, err := a.ListEndOffsets(ctx, topic)
		if err == nil {
			if v, ok := result.Lookup(topic, 0); ok && v.Err == nil && int64(v.Offset) >= end {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("destination did not reach expected end", err)
		case <-tick.C:
		}
	}
}
