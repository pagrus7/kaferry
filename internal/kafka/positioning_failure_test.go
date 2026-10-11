package kafka

import (
	"errors"
	"strings"
	"testing"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pagrus7/kaferry/internal/plan"
)

type failedTimes struct {
	result []k.TopicPartition
	err    error
}

func (t failedTimes) OffsetsForTimes([]k.TopicPartition, int) ([]k.TopicPartition, error) {
	return t.result, t.err
}

type failedStableOffsets struct {
	countingOffsets
	err error
}

func (o *failedStableOffsets) Offsets(ids []int32, spec k.OffsetSpec, committed bool) (map[int32]int64, error) {
	if committed {
		return nil, o.err
	}
	return o.countingOffsets.Offsets(ids, spec, committed)
}

func TestPositioningDoesNotFallbackAfterLookupFailure(t *testing.T) {
	for _, scenario := range []string{"timestamp RPC", "timestamp partition", "foreign topic result", "stable end RPC"} {
		t.Run(scenario, func(t *testing.T) {
			// Given valid retained offsets but an unsuccessful start-position lookup.
			topic, foreign := "orders", "other"
			failure := errors.New("offset lookup failed")
			offsets := &failedStableOffsets{countingOffsets: countingOffsets{low: 100, high: 1000}, err: failure}
			times := failedTimes{}
			p := plan.Plan{Source: plan.Endpoint{Name: topic}, Window: plan.WindowSpec{Lower: &plan.Expression{LiteralMS: 1000}}}
			switch scenario {
			case "timestamp RPC":
				times.err = failure
			case "timestamp partition":
				times.result = []k.TopicPartition{{Topic: &topic, Partition: 1, Error: failure}}
			case "foreign topic result":
				times.result = []k.TopicPartition{{Topic: &foreign, Partition: 1, Offset: 500}}
			case "stable end RPC":
				p.Window = plan.WindowSpec{}
				p.Start = "LATEST"
			}
			positioner := NewPositionerFactory(&fixedClock{now: time.Unix(100, 0)}, quietLogger()).New(offsets, times, p, "fp", "topic-id")

			// When resolving the assigned partition's starting offset.
			got, err := positioner.Locate([]k.TopicPartition{{Topic: &topic, Partition: 1}}, nil, nil)

			// Then no assignment or initial progress escapes via an earliest/latest fallback.
			if err == nil || got.Assignments != nil || got.Progress != nil {
				t.Fatal("failed lookup returned a usable position", got, err)
			}
			if scenario == "foreign topic result" {
				if !strings.Contains(err.Error(), "missing timestamp lookup result") {
					t.Fatal("foreign result accepted", err)
				}
			} else if !errors.Is(err, failure) {
				t.Fatal("lookup cause lost", err)
			}
		})
	}
}
