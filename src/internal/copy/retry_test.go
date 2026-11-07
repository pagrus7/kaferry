package copy

import (
	"testing"
	"time"
)

func TestRetryScheduleCapsDelayAndResetsAfterRecovery(t *testing.T) {
	// Given a fresh schedule and a deterministic clock.
	var schedule retrySchedule
	now := time.Unix(100, 0)
	logs := 0
	for attempt, seconds := range []int{1, 2, 4, 8, 16, 30, 30, 30} {
		// When an attempt fails at its permitted time.
		if !schedule.due(now) {
			t.Fatal("scheduled attempt suppressed")
		}
		if schedule.failed(now) {
			logs++
		}
		delay := time.Duration(seconds) * time.Second

		// Then retry waits for the capped delay and never starts early.
		if schedule.attempts != attempt+1 || schedule.delay != delay || schedule.due(now.Add(delay-time.Nanosecond)) || !schedule.due(now.Add(delay)) {
			t.Fatal("invalid retry schedule", schedule)
		}
		now = now.Add(delay)
	}
	// When an attempt succeeds.
	attempts := schedule.recovered()

	// Then the next outage starts fresh and prior logs were rate limited.
	if attempts != 8 || logs != 2 || schedule != (retrySchedule{}) || !schedule.due(now) {
		t.Fatal("recovery did not reset retries", attempts, logs, schedule)
	}

	// When another outage begins.
	schedule.failed(now)

	// Then it starts with the initial delay.
	if schedule.delay != time.Second || schedule.attempts != 1 {
		t.Fatal("outage reused old backoff", schedule)
	}
}
