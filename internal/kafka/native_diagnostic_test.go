//go:build native_diagnostic

package kafka

import (
	"context"
	"os"
	"testing"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Diagnostic only: this canary deliberately expects the bundled upstream defect to remain present.
// Run manually with go test -tags=native_diagnostic -run TestStockClientCoordinatorTimeout ./internal/kafka.
func TestStockClientCoordinatorTimeout(t *testing.T) {
	for _, operation := range []string{"groups", "offsets"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			// Given a native timeout that expires before the broker recovers.
			s := nativeScenario{Operation: operation, GoTimeout: 3 * time.Second, RecoverAfter: 800 * time.Millisecond, CloseAfter: 1500 * time.Millisecond}

			// When the stock client's request runs in an isolated subprocess.
			outcome := runNativeSubprocess(t, s, "TestStockNativeHelperProcess")

			// Then only the known assertion abort confirms this reproduction.
			if outcome.Kind != "abort" || !outcome.reached("eonce->refcnt > 0") || !outcome.reached("brokers restored") || outcome.reached("close start") {
				t.Fatalf("expected coordinator assertion before close; got %s after %.2fs:\n%s", outcome.Kind, outcome.Elapsed.Seconds(), outcome.Output)
			}
		})
	}
}

func TestStockNativeHelperProcess(t *testing.T) {
	if s, ok := nativeScenarioFromEnvironment(t); ok {
		runNativeScenario(s, stockNativeRequest)
		os.Exit(0)
	}
}

func stockNativeRequest(s nativeScenario, inspector *Inspector) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.GoTimeout)
	defer cancel()
	timeout := k.SetAdminRequestTimeout(300 * time.Millisecond)
	if s.Operation == "groups" {
		result, err := inspector.admin.DescribeConsumerGroups(ctx, []string{nativeGroup}, timeout)
		if err != nil {
			return err
		}
		for _, group := range result.ConsumerGroupDescriptions {
			if group.Error.Code() != k.ErrNoError {
				return group.Error
			}
		}
		return nil
	}
	topic := nativeTopic
	request := []k.ConsumerGroupTopicPartitions{{Group: nativeGroup, Partitions: []k.TopicPartition{{Topic: &topic, Partition: 0}}}}
	result, err := inspector.admin.ListConsumerGroupOffsets(ctx, request, timeout)
	if err != nil {
		return err
	}
	for _, group := range result.ConsumerGroupsTopicPartitions {
		for _, part := range group.Partitions {
			if part.Error != nil {
				return part.Error
			}
		}
	}
	return nil
}
