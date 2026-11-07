//go:build integration

package compat

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

func committedPartitions(t *testing.T, a *kadm.Client, group, topic string, count int) []kadm.Offset {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		result, err := a.FetchOffsets(ctx, group)
		if err == nil && result.Error() == nil && len(result[topic]) == count {
			valid := true
			for _, p := range result[topic] {
				valid = valid && p.At >= 0 && p.Metadata != ""
			}
			if valid {
				return kadm.OffsetResponses{topic: result[topic]}.Offsets().Sorted()
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("initial progress not committed: %v %+v", err, result)
		case <-tick.C:
		}
	}
}
