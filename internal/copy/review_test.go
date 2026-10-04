package copy

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"kafka-fwd2/internal/plan"
)

type watermarkSource struct {
	*testSource
	failure error
	calls   int
}

func (s *watermarkSource) HighWatermark(int32) (int64, error) {
	s.calls++
	return s.high, s.failure
}

func TestTipSamplingRetriesWithoutStartingWaitOrBlockingOtherPartitions(t *testing.T) {
	// Given two bounded partitions at their tip and a recoverable query outage.
	r, source, _, _, clock := fixture(t)
	r.plan.Source.Kind = "TOPIC"
	hi := clock.Now().Add(-time.Minute).UnixMilli()
	assignments := []Assignment{{Cursor: Cursor{Partition: 0}, Window: plan.Window{Upper: &hi}}, {Cursor: Cursor{Partition: 1}, Window: plan.Window{Upper: &hi}}}
	if err := r.Assign(assignments); err != nil {
		t.Fatal(err)
	}
	for _, p := range r.partitions {
		p.atTip = true
	}
	query := &watermarkSource{testSource: source, failure: &Recoverable{Err: errors.New("timeout")}}
	r.source = query

	// When sampling during the outage and checking again before the retry delay.
	err := r.tips()
	if err != nil {
		t.Fatal(err)
	}
	if query.calls != 1 {
		t.Fatal("watermark queries were not interleaved with polling", query.calls)
	}
	err = r.tips()

	// Then both partitions are attempted once; neither H nor tip_wait is started.
	if err != nil || query.calls != 2 {
		t.Fatal("query retry was unbounded or failed", query.calls, err)
	}
	for _, p := range r.partitions {
		if p.high != nil || !p.waitStarted.IsZero() || p.stopped {
			t.Fatal("outage was mistaken for a sampled tip", p)
		}
	}

	// When the delay elapses and the source recovers with an empty watermark.
	clock.Sleep(time.Second)
	query.failure = nil
	err = r.tips()
	if err != nil {
		t.Fatal(err)
	}
	err = r.tips()

	// Then each partition completes and its successful sample starts the wait clock.
	if err != nil || query.calls != 4 {
		t.Fatal(query.calls, err)
	}
	for _, p := range r.partitions {
		if !p.stopped || p.high == nil || !p.waitStarted.Equal(clock.Now()) {
			t.Fatal("recovery did not complete", p)
		}
	}
}

func TestTipSamplingPermanentFailuresStopImmediately(t *testing.T) {
	// Given a bounded partition and an unclassified permanent query error.
	r, source, _, _, clock := fixture(t)
	r.plan.Source.Kind = "TOPIC"
	hi := clock.Now().Add(-time.Minute).UnixMilli()
	if err := r.Assign([]Assignment{{Cursor: Cursor{}, Window: plan.Window{Upper: &hi}}}); err != nil {
		t.Fatal(err)
	}
	r.partitions[0].atTip = true
	failure := errors.New("authorization")
	r.source = &watermarkSource{testSource: source, failure: failure}

	// When sampling the watermark.
	err := r.tips()

	// Then the permanent failure is returned without starting a retry.
	if !errors.Is(err, failure) || !r.partitions[0].nextSample.IsZero() {
		t.Fatal("permanent query failure retried", err)
	}
}

func TestReassignedPartitionResumesWithFreshRunnerState(t *testing.T) {
	// Given a partition paused by its uncommitted record cap.
	r, source, sink, _, _ := fixture(t)
	r.limits.RecordCap, r.limits.PartitionRecordCap = 1, 1
	if err := source.Start(r); err != nil {
		t.Fatal(err)
	}
	if err := r.input(incoming(0, 0, 1)); err != nil {
		t.Fatal(err)
	}
	if err := r.pause(); err != nil {
		t.Fatal(err)
	}
	if !source.paused[0] {
		t.Fatal("fixture did not reach the pause cap")
	}

	// When ownership is revoked and the source clears pause before reassigning.
	err := r.Revoke([]int32{0}, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Pause(0, false); err != nil {
		t.Fatal(err)
	}
	err = r.Assign([]Assignment{{Cursor: Cursor{Partition: 0, Position: 0}}})
	if err != nil {
		t.Fatal(err)
	}
	err = r.input(incoming(0, 0, 1))

	// Then the new generation publishes again and late old delivery is fenced.
	if err != nil || source.paused[0] || len(sink.writes) != 2 || sink.writes[0].Generation == sink.writes[1].Generation {
		t.Fatal("reassigned partition stalled or reused ownership", err)
	}
	if r.tracker.Resolve(sink.writes[0]) {
		t.Fatal("revoked delivery resolved new work")
	}
}

func TestTipSamplingBackoffCapsResetsAndLimitsLogs(t *testing.T) {
	// Given an unavailable leader, a fake clock and captured operator diagnostics.
	r, source, _, _, clock := fixture(t)
	r.plan.Source.Kind = "TOPIC"
	hi := clock.Now().Add(-time.Minute).UnixMilli()
	if err := r.Assign([]Assignment{{Cursor: Cursor{}, Window: plan.Window{Upper: &hi}}}); err != nil {
		t.Fatal(err)
	}
	p := r.partitions[0]
	p.atTip = true
	query := &watermarkSource{testSource: source, failure: &Recoverable{Err: errors.New("leader unavailable")}}
	r.source = query
	var logs bytes.Buffer
	r.log = slog.New(slog.NewTextHandler(&logs, nil))

	for attempt, seconds := range []int{1, 2, 4, 8, 16, 30, 30, 30} {
		// When the next scheduled attempt fails.
		delay := time.Duration(seconds) * time.Second
		err := r.tips()

		// Then delay grows to 30s; suppressed polls do not query or start tip_wait.
		if err != nil || query.calls != attempt+1 || p.sampleDelay != delay || !p.nextSample.Equal(clock.Now().Add(delay)) || p.high != nil || !p.waitStarted.IsZero() {
			t.Fatal("invalid retry state", query.calls, p, err)
		}
		clock.Sleep(delay - time.Millisecond)
		if err := r.tips(); err != nil || query.calls != attempt+1 {
			t.Fatal("early retry", query.calls, err)
		}
		clock.Sleep(time.Millisecond)
	}

	// When the leader recovers at the next permitted sample.
	query.failure = nil
	err := r.tips()

	// Then retry state resets, the partition completes, and logs remain bounded.
	if err != nil || !p.stopped || p.sampleDelay != 0 || p.sampleAttempts != 0 || !p.nextSample.IsZero() || !p.sampleLog.IsZero() || !p.waitStarted.Equal(clock.Now()) {
		t.Fatal("retry state not reset", p, err)
	}
	if strings.Count(logs.String(), "tip watermark query retrying") != 2 || strings.Count(logs.String(), "tip watermark query recovered") != 1 {
		t.Fatal("retry diagnostics not rate limited", logs.String())
	}
	if !strings.Contains(logs.String(), `reason="leader unavailable"`) || !strings.Contains(logs.String(), "attempt=1") {
		t.Fatal("retry diagnostics omit cause/attempt", logs.String())
	}
}
