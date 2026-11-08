//go:build integration

package compat

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

type workerResult struct {
	code   int
	detail string
}

func waitMembers(t *testing.T, a *kadm.Client, group string, members int, partitions int, results <-chan workerResult) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		result, err := a.DescribeGroups(ctx, group)
		if err == nil && len(result) == 1 && result[group].Err == nil {
			g := result[group]
			assigned := 0
			for _, m := range g.Members {
				if assignment, ok := m.Assigned.AsConsumer(); ok {
					for _, topic := range assignment.Topics {
						assigned += len(topic.Partitions)
					}
				}
			}
			if len(g.Members) == members && assigned == partitions {
				return
			}
		}
		select {
		case r := <-results:
			t.Fatalf("worker exited during assignment: code=%d detail=%s", r.code, r.detail)
		case <-ctx.Done():
			t.Fatal("workers did not acquire expected assignments", err)
		case <-ticker.C:
		}
	}
}
