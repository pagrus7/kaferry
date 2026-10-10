package kafka

import (
	"errors"
	core "github.com/pagrus7/kaferry/internal/copy"
	"math"
	"testing"
	"time"
)

func TestTimestampLimitsResolveDirectionsIndependently(t *testing.T) {
	for _, scenario := range []struct {
		name, before, after, legacy string
		delta                       int64
		rejected                    bool
	}{
		{"legacy past", "9223372036854775807", "9223372036854775807", "1000", -1001, true},
		{"legacy future", "9223372036854775807", "9223372036854775807", "1000", 1001, true},
		{"inclusive legacy edge", "9223372036854775807", "9223372036854775807", "1000", -1000, false},
		{"direction overrides legacy", "5000", "", "1000", -2000, false},
		{"other direction unavailable", "1000", "", "", -1001, true},
		{"past unavailable future known", "", "1000", "", 1001, true},
		{"unavailable direction skipped", "", "1000", "", -2000, false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			// Given independently readable directional and legacy broker limits.
			clock := &fixedClock{now: time.UnixMilli(100000)}
			sink := &Sink{clock: clock, configuration: map[string]string{"message.timestamp.type": "CreateTime", "message.timestamp.before.max.ms": scenario.before, "message.timestamp.after.max.ms": scenario.after, "message.timestamp.difference.max.ms": scenario.legacy}}
			stamp := clock.Now().UnixMilli() + scenario.delta

			// When preflighting a record timestamp before native submission.
			err := sink.validate(core.Record{TimestampMS: &stamp})

			// Then only a known violated bound rejects the identified record.
			var recordError *core.RecordError
			if (err != nil) != scenario.rejected || err != nil && (!errors.As(err, &recordError) || recordError.Class != "rejected_timestamp") {
				t.Fatal("incorrect timestamp decision", err)
			}
		})
	}
}

func TestTimestampAndHeaderPreflightAreRecordAttributable(t *testing.T) {
	// Given
	s := &Sink{clock: core.RealClock{}, configuration: map[string]string{"message.timestamp.type": "CreateTime", "message.timestamp.before.max.ms": "1000", "message.timestamp.after.max.ms": "1000"}}
	now := time.Now().UnixMilli()
	for _, stamp := range []int64{0, -1, math.MaxInt64, now - 10000, now + 10000} {

		// When
		validateErr := s.validate(core.Record{TimestampMS: &stamp})

		// Then
		if validateErr == nil {
			t.Fatal("unpreservable timestamp accepted", stamp)
		}
	}
	if err := s.validate(core.Record{TimestampMS: &now}); err != nil {
		t.Fatal(err)
	}
	if err := s.validate(core.Record{Headers: []core.Header{{Name: "nul\x00suffix"}}}); err == nil {
		t.Fatal("silently truncated header accepted")
	}
	s.configuration = map[string]string{"message.timestamp.type": "LogAppendTime"}
	old := int64(1000)
	if err := s.validate(core.Record{TimestampMS: &old}); err != nil {
		t.Fatal("CreateTime checks applied to LogAppendTime")
	}
	for _, stamp := range []int64{0, -1, math.MaxInt64} {
		if err := s.validate(core.Record{TimestampMS: &stamp}); err != nil {
			t.Fatal("restamped timestamp rejected before LogAppendTime destination", err)
		}
	}
}
