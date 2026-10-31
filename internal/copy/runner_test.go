package copy

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
)

type testClock struct {
	now  time.Time
	tick func()
}

type stopObserverMock struct{ begun bool }

func (s *stopObserverMock) Begin()     { s.begun = true }
func (*stopObserverMock) Stage(string) {}

func TestShutdownBudgetStartsBeforePotentiallyBlockingDrain(t *testing.T) {
	for _, signal := range []bool{false, true} {
		t.Run(map[bool]string{false: "write failure", true: "signal"}[signal], func(t *testing.T) {
			// Given a following copy that must drain after a signal or a write failure.
			r, _, sink, _, _ := fixture(t)
			r.plan.Source.Kind = "TOPIC"
			observer := &stopObserverMock{}
			r.stop = observer
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if signal {
				cancel()
			} else {
				r.failure = errors.New("write failed")
			}
			sink.flush = func() error {
				require.True(t, observer.begun, "drain began without a shutdown budget")
				return nil
			}
			// When stopping, then a blocked flush is already covered by the process deadline.
			_ = r.Run(ctx)
			require.True(t, observer.begun, "shutdown budget never started")
		})
	}
}

func (c *testClock) Now() time.Time { return c.now }
func (c *testClock) Sleep(d time.Duration) {
	c.now = c.now.Add(d)
	if c.tick != nil {
		c.tick()
	}
}

type testSource struct {
	clock       *testClock
	assignments []Assignment
	events      []Event
	paused      map[int32]bool
	high        int64
}

func (s *testSource) Start(l Lifecycle, _ Observer) error { return l.Assign(s.assignments) }
func (s *testSource) Poll(d time.Duration) (Event, error) {
	s.clock.Sleep(d)
	if len(s.events) == 0 {
		return Event{}, nil
	}
	e := s.events[0]
	if e.Input != nil && s.paused[e.Input.Cursor.Partition] {
		return Event{}, nil
	}
	s.events = s.events[1:]
	return e, nil
}
func (s *testSource) Pause(id int32, paused bool) error  { s.paused[id] = paused; return nil }
func (s *testSource) HighWatermark(int32) (int64, error) { return s.high, nil }
func (*testSource) Close() error                         { return nil }

type testSink struct {
	reports chan Delivery
	writes  []Token
	submit  func(Record, Token) error
	flush   func() error
}

func (s *testSink) Submit(record Record, token Token) error {
	if s.submit != nil {
		if err := s.submit(record, token); err != nil {
			return err
		}
	}
	s.writes = append(s.writes, token)
	return nil
}
func (s *testSink) Deliveries() <-chan Delivery { return s.reports }
func (s *testSink) Flush() error {
	if s.flush != nil {
		return s.flush()
	}
	return nil
}
func (*testSink) Close() {}

type testStore struct {
	saves  [][]Cursor
	fail   map[int32]error
	omit   map[int32]bool
	onSave func()
}

func (s *testStore) Save(_ context.Context, cursors []Cursor) (map[int32]error, error) {
	s.saves = append(s.saves, append([]Cursor(nil), cursors...))
	if s.onSave != nil {
		s.onSave()
	}
	out := make(map[int32]error)
	for _, c := range cursors {
		if !s.omit[c.Partition] {
			out[c.Partition] = s.fail[c.Partition]
		}
	}
	return out, nil
}
func fixture(t *testing.T) (*Runner, *testSource, *testSink, *testStore, *testClock) {
	t.Helper()
	c := &testClock{now: time.Unix(100, 0)}
	s := &testSource{clock: c, paused: make(map[int32]bool), assignments: []Assignment{{Cursor: Cursor{Partition: 0, Position: 0}}}}
	d := &testSink{reports: make(chan Delivery, 100)}
	p := &testStore{}
	r := NewRunner(s, d, p, c, slog.New(slog.NewTextHandler(io.Discard, nil)), plan.Plan{Source: plan.Endpoint{Kind: "ARCHIVE"}}, config.Defaults(t.TempDir()).Worker, NewTracker(), NoopStopObserver{}, nil)
	return r, s, d, p, c
}
func incoming(id int32, position int64, size int) Input {
	return Input{Cursor: Cursor{Partition: id, Position: position}, Next: position + 1, Record: Record{Value: make([]byte, size)}}
}

func TestRunnerDoesNotCommitPastFailedWrite(t *testing.T) {
	// Given
	r, s, d, p, _ := fixture(t)
	for i := range 3 {
		in := incoming(0, int64(i), 1)
		s.events = append(s.events, Event{Input: &in})
	}
	s.events = append(s.events, Event{EOF: &Cursor{Partition: 0, Position: 3}})
	d.flush = func() error {
		if len(d.writes) == 3 {
			d.reports <- Delivery{Token: d.writes[2]}
			d.reports <- Delivery{Token: d.writes[0]}
			d.reports <- Delivery{Token: d.writes[1], Err: errors.New("rejected batch")}
			d.flush = nil
		}
		return nil
	}

	// When
	runErr := r.Run(context.Background())

	// Then
	require.Error(t, runErr, "failed write was accepted")
	if len(p.saves) != 1 || p.saves[0][0].Position != 1 {
		t.Fatalf("unsafe checkpoint: %+v", p.saves)
	}
}

func TestRunnerCapsIncludeConfirmedWorkAndOversizedAlone(t *testing.T) {
	// Given
	r, _, d, p, _ := fixture(t)
	r.limits.ByteCap = 150
	r.limits.PartitionByteCap = 150
	require.NoError(t, r.Assign([]Assignment{{Cursor: Cursor{Partition: 0, Position: 0}}}))

	// When
	inputErr := r.input(incoming(0, 0, 30))

	// Then
	require.NoError(t, inputErr)

	// When the first write succeeds but is still uncommitted.
	d.reports <- Delivery{Token: d.writes[0]}
	require.NoError(t, r.deliveries())
	require.NoError(t, r.input(incoming(0, 1, 200)))

	// Then the oversized record waits for the outstanding work.
	if len(d.writes) != 1 || r.flow.deferred == nil {
		t.Fatal("oversized record submitted alongside uncommitted work")
	}

	// When confirmed progress is committed and intake resumes.
	require.NoError(t, r.pause())
	require.NoError(t, r.commit(r.ids()))
	require.NoError(t, r.admitDeferred())

	// Then the oversized record is admitted alone.
	if len(d.writes) != 2 || p.saves[0][0].Position != 1 {
		t.Fatal("oversized record did not resume after progress became durable")
	}
	n, b := r.tracker.Total()
	if n != 1 || b != 264 {
		t.Fatalf("work=%d/%d", n, b)
	}
}

func TestRunnerQueueFullPreservesSubmissionOrder(t *testing.T) {
	// Given
	r, s, d, _, _ := fixture(t)
	in := incoming(0, 0, 1)
	s.events = []Event{{Input: &in}, {EOF: &Cursor{Partition: 0, Position: 1}}}
	tries := 0
	d.submit = func(_ Record, tok Token) error {
		tries++
		if tries < 3 {
			return &Busy{}
		}
		d.reports <- Delivery{Token: tok}
		return nil
	}

	// When
	runErr := r.Run(context.Background())

	// Then
	require.NoError(t, runErr)
	if tries != 3 || len(d.writes) != 1 {
		t.Fatalf("tries=%d submitted=%d", tries, len(d.writes))
	}
}

func TestRevokeFencesLateOutcomesAndLostOwnershipNeverCommits(t *testing.T) {
	// Given
	for _, lost := range []bool{false, true} {
		t.Run(map[bool]string{false: "revoked", true: "lost"}[lost], func(t *testing.T) {
			r, _, d, p, _ := fixture(t)
			require.NoError(t, r.Assign([]Assignment{{Cursor: Cursor{Partition: 0, Position: 0}}}))
			_ = r.input(incoming(0, 0, 1))
			old := d.writes[0]

			// When
			revokeErr := r.Revoke([]int32{0}, lost)

			// Then
			require.NoError(t, revokeErr)
			require.NoError(t, r.Assign([]Assignment{{Cursor: Cursor{Partition: 0, Position: 0}}}))
			_ = r.input(incoming(0, 0, 1))
			d.reports <- Delivery{Token: old, Err: errors.New("late rejected write")}
			require.NoError(t, r.deliveries(), "late outcome affected new owner")
			if r.tracker.Resolved(0) || len(p.saves) != 0 {
				t.Fatal("late delivery advanced durable progress")
			}
		})
	}
	r, _, d, p, _ := fixture(t)
	require.NoError(t, r.Assign([]Assignment{{Cursor: Cursor{Partition: 0, Position: 0}}}))
	_ = r.input(incoming(0, 0, 1))

	// When the first write succeeds but is still uncommitted.
	d.reports <- Delivery{Token: d.writes[0]}
	require.NoError(t, r.deliveries())
	_ = r.Revoke([]int32{0}, true)
	require.Empty(t, p.saves, "committed after ownership loss")
}

func TestPartialCommitReleasesSuccessfulPartitionsOnly(t *testing.T) {
	for _, scenario := range []string{"failed result", "missing result"} {
		t.Run(scenario, func(t *testing.T) {
			// Given confirmed writes on two partitions and one unconfirmed commit result.
			r, _, d, p, _ := fixture(t)
			require.NoError(t, r.Assign([]Assignment{{Cursor: Cursor{Partition: 0, Position: 0}}, {Cursor: Cursor{Partition: 1, Position: 0}}}))
			for _, id := range []int32{0, 1} {
				require.NoError(t, r.input(incoming(id, 0, 1)))
			}
			for _, tok := range d.writes {
				d.reports <- Delivery{Token: tok}
			}
			require.NoError(t, r.deliveries())
			if scenario == "missing result" {
				p.omit = map[int32]bool{1: true}
			} else {
				p.fail = map[int32]error{1: errors.New("commit failed")}
			}

			// When saving both partitions' confirmed progress.
			err := r.commit(r.ids())

			// Then only the successful partition releases its work and advances saved progress.
			n0, _ := r.tracker.Work(0)
			n1, _ := r.tracker.Work(1)
			if err == nil || n0 != 0 || n1 != 1 || r.tracker.Saved(0) != 1 || r.tracker.Saved(1) != 0 {
				t.Fatalf("partial commit released work=%d/%d: %v", n0, n1, err)
			}
			if scenario == "missing result" && !strings.Contains(err.Error(), "missing commit result for partition 1") {
				t.Fatal("missing result accepted", err)
			}
		})
	}
}

func TestShutdownDrainDeadlineCommitsConfirmedPrefix(t *testing.T) {
	// Given
	r, _, d, p, c := fixture(t)
	require.NoError(t, r.Assign([]Assignment{{Cursor: Cursor{Partition: 0, Position: 0}}}))
	_ = r.input(incoming(0, 0, 1))
	_ = r.input(incoming(0, 1, 1))
	d.reports <- Delivery{Token: d.writes[0]}
	start := c.Now()

	// When
	drainErr := r.drain()

	// Then
	require.Error(t, drainErr, "unacknowledged write treated as drained")
	if c.Now().Sub(start) < 10*time.Second || c.Now().Sub(start) > 10*time.Second+10*time.Millisecond {
		t.Fatal("wrong drain deadline")
	}
	if len(p.saves) != 1 || p.saves[0][0].Position != 1 {
		t.Fatalf("wrong drain commit: %+v", p.saves)
	}
}

func TestTimestampTriggerReplaysTriggerOffset(t *testing.T) {
	// Given
	r, s, d, p, _ := fixture(t)
	hi := int64(1000)
	stamp := int64(2001)
	r.plan.Source.Kind = "TOPIC"
	r.plan.Window.Upper = &plan.Expression{LiteralMS: hi}
	r.plan.MaxStalenessMS = 1000
	s.high = 1
	s.assignments[0].Window.Upper = &hi
	in := incoming(0, 0, 1)
	in.Record.TimestampMS = &stamp
	s.events = []Event{{Input: &in}}

	// When
	runErr := r.Run(context.Background())

	// Then
	require.NoError(t, runErr)
	if len(d.writes) != 0 || len(p.saves) != 2 || p.saves[0][0].Position != 0 || p.saves[0][0].Stop != nil || p.saves[1][0].Position != 0 || p.saves[1][0].Stop == nil || p.saves[1][0].Stop.Reason != "timestamp" {
		t.Fatal("trigger record was written or checkpointed past")
	}
}

func TestTipWaitUsesSampledHighWatermarkAndTimesOut(t *testing.T) {
	// Given
	r, s, _, _, c := fixture(t)
	hi := int64(1000)
	r.plan.Source.Kind = "TOPIC"
	r.plan.Window.Upper = &plan.Expression{LiteralMS: hi}
	r.limits.TipWait = "1s"
	_ = r.Assign([]Assignment{{Cursor: Cursor{Partition: 0, Position: 0}, Window: plan.Window{Upper: &hi}}})
	s.high = 2
	_ = r.eof(Cursor{Partition: 0, Position: 0})

	// When
	tipsErr := r.tips()

	// Then
	require.NoError(t, tipsErr)

	// When the topic grows after the sample and tip_wait expires.
	s.high = 999
	c.Sleep(2 * time.Second)
	tipsErr = r.tips()

	// Then the original boundary stays fixed and the diagnostic names the cursors.
	require.Equal(t, int64(2), *r.partitions[0].high, "tip boundary moved")
	if tipsErr == nil || !strings.Contains(tipsErr.Error(), "H=2 stable=0 consumed=0") {
		t.Fatal("stalled transaction must name its boundary and cursors", tipsErr)
	}
}

func TestIdleExitRequiresProcessedEmptyAssignment(t *testing.T) {
	// Given a bounded worker that has never received an assignment.
	r, _, _, _, c := fixture(t)
	r.limits.IdleExit = "1s"
	c.Sleep(time.Hour)

	// When its idle deadline elapses before assignment.
	finished := r.readyToExit()

	// Then it remains available for a first assignment.
	require.False(t, finished, "exited before assignment")
	_ = r.Assign(nil)
	c.Sleep(2 * time.Second)
	require.True(t, r.readyToExit(), "bounded idle worker did not exit")
	require.NoError(t, r.Assign([]Assignment{{Cursor: Cursor{Partition: 0, Position: 0}}}))
	_ = r.Revoke([]int32{0}, false)
	c.Sleep(time.Hour)
	require.False(t, r.readyToExit(), "empty rebalance treated as completion")
}

func TestSignalDuringPollDoesNotSubmitReturnedRecord(t *testing.T) {
	// Given
	r, s, d, _, c := fixture(t)
	r.plan.Source.Kind = "TOPIC"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.tick = cancel
	in := incoming(0, 0, 1)
	s.events = []Event{{Input: &in}}
	pausedBeforeFlush := false
	d.flush = func() error { pausedBeforeFlush = s.paused[0]; return nil }

	// When
	runErr := r.Run(ctx)

	// Then
	require.NoError(t, runErr)
	if len(d.writes) != 0 || !pausedBeforeFlush {
		t.Fatal("signal submitted new input or flushed before pausing intake")
	}
}

func TestSignalCannotHideKnownFailure(t *testing.T) {
	// Given
	r, _, _, _, _ := fixture(t)
	r.plan.Source.Kind = "TOPIC"
	failure := errors.New("destination failed")
	r.failure = failure
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// When
	runErr := r.Run(ctx)

	// Then
	require.ErrorIs(t, runErr, failure, "signal hid a known failure")
}

type cancellingPoll struct {
	*testSource
	cancel context.CancelFunc
	err    error
}

type cancellingWatermark struct {
	*testSource
	cancel context.CancelFunc
	err    error
}

func (s *cancellingWatermark) HighWatermark(int32) (int64, error) { s.cancel(); return 0, s.err }
func TestSignalDuringWatermarkQueryReportsInterruptionAndPreservesOtherErrors(t *testing.T) {
	for _, unrelated := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancelled query", true: "broker failure"}[unrelated], func(t *testing.T) {
			// Given a bounded copy that reaches the topic tip and samples its ending offset.
			r, source, _, _, _ := fixture(t)
			r.plan.Source.Kind = "TOPIC"
			hi := int64(1)
			r.plan.Window.Upper = &plan.Expression{LiteralMS: hi}
			source.assignments[0].Window.Upper = &hi
			source.events = []Event{{EOF: &Cursor{Partition: 0, Position: 0}}}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			failure := error(context.Canceled)
			if unrelated {
				failure = errors.New("topic authorization failed")
			}
			r.source = &cancellingWatermark{testSource: source, cancel: cancel, err: failure}
			// When a signal arrives during the query, then cancellation follows the ordinary interrupted-copy path.
			err := r.Run(ctx)
			if err == nil || unrelated && !errors.Is(err, failure) || !unrelated && !strings.Contains(err.Error(), "bounded copy interrupted before completion") {
				t.Fatal("unclear interruption or hidden broker failure", err)
			}
		})
	}
}

func (s *cancellingPoll) Poll(time.Duration) (Event, error) {
	s.cancel()
	input := incoming(0, 0, 1)
	return Event{Input: &input}, s.err
}

func TestSignalDuringPollDoesNotTurnCancellationIntoCopyFailure(t *testing.T) {
	for _, unrelated := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancellation", true: "unrelated failure"}[unrelated], func(t *testing.T) {
			// Given a following Kafka copy cancelled during a poll or assignment request.
			r, source, sink, store, _ := fixture(t)
			r.plan.Source.Kind = "TOPIC"
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			failure := error(context.Canceled)
			if unrelated {
				failure = errors.New("topic authorization failed")
			}
			r.source = &cancellingPoll{testSource: source, cancel: cancel, err: failure}
			// When the signal and poll result arrive together.
			err := r.Run(ctx)
			// Then no input is published or saved; only an unrelated failure exits with an error.
			if (err != nil) != unrelated || unrelated && !errors.Is(err, failure) || len(sink.writes) != 0 || len(store.saves) != 0 {
				t.Fatal("signal admitted input, hid a failure, or failed a clean stop", err, sink.writes, store.saves)
			}
		})
	}
}

func TestCapturedTipFinishesWithoutAnotherEOF(t *testing.T) {
	// Given
	for _, gap := range []bool{false, true} {
		t.Run(map[bool]string{false: "records", true: "control gap"}[gap], func(t *testing.T) {
			r, s, d, p, _ := fixture(t)
			hi := int64(1000)
			stamp := int64(500)
			r.plan.Source.Kind = "TOPIC"
			r.plan.Window.Upper = &plan.Expression{LiteralMS: hi}
			r.limits.TipWait = "1s"
			s.assignments[0].Window.Upper = &hi
			first := incoming(0, 0, 1)
			first.Record.TimestampMS = &stamp
			second := incoming(0, 1, 1)
			if gap {
				second = incoming(0, 4, 1)
			}
			second.Record.TimestampMS = &stamp
			s.high = 2
			if gap {
				s.high = 3
			}
			s.events = []Event{{EOF: &Cursor{Partition: 0, Position: 0}}, {Input: &first}, {Input: &second}}
			d.flush = func() error {
				if len(d.writes) > 0 {
					for _, tok := range d.writes {
						d.reports <- Delivery{Token: tok}
					}
					d.flush = nil
				}
				return nil
			}

			// When
			runErr := r.Run(context.Background())

			// Then
			require.NoError(t, runErr, "sampled H incorrectly required another EOF")
			want := 2
			if gap {
				want = 1
			}
			if len(d.writes) != want || p.saves[len(p.saves)-1][0].Position != s.high {
				t.Fatal("copied beyond H or failed to commit its consumed boundary")
			}
		})
	}
}

func TestRevocationCommitFailures(t *testing.T) {
	// Given
	for _, tc := range []struct {
		name    string
		err     error
		request bool
		fails   bool
	}{
		{"recoverable partition error", &Recoverable{Err: errors.New("rebalance in progress")}, false, false},
		{"recoverable request error", &Recoverable{Err: errors.New("coordinator unavailable")}, true, false},
		{"terminal partition error", errors.New("group authorization failed"), false, true},
		{"terminal request error", errors.New("group authorization failed"), true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, _, _, _, _ := fixture(t)
			store := &requestStore{err: tc.err, request: tc.request}
			r.store = store
			require.NoError(t, r.Assign([]Assignment{{Cursor: Cursor{Partition: 0, Position: 0}}}))
			in := incoming(0, 0, 1)
			token, _ := r.tracker.Observe(in.Cursor, in.Next, 1)
			r.tracker.Settle(token, true)

			// When
			err := r.Revoke([]int32{0}, false)

			// Then
			require.Equal(t, tc.fails, err != nil, "revoke error = %v, want failure=%v", err, tc.fails)
			if len(r.partitions) != 0 || store.calls != 1 {
				t.Fatalf("partition not released or commit retried: partitions=%d calls=%d", len(r.partitions), store.calls)
			}
		})
	}
	// Outside revocation a recoverable error still fails the commit.
	r, _, _, _, _ := fixture(t)
	r.store = &requestStore{err: &Recoverable{Err: errors.New("rebalance in progress")}}
	require.NoError(t, r.Assign([]Assignment{{Cursor: Cursor{Partition: 0, Position: 0}}}))
	token, _ := r.tracker.Observe(Cursor{Partition: 0, Position: 0}, 1, 1)
	r.tracker.Settle(token, true)
	require.Error(t, r.commit([]int32{0}), "periodic commit must fail on a recoverable error")
}

type requestStore struct {
	err     error
	request bool
	calls   int
}

func (s *requestStore) Save(_ context.Context, cursors []Cursor) (map[int32]error, error) {
	s.calls++
	if s.request {
		return nil, s.err
	}
	out := make(map[int32]error)
	for _, c := range cursors {
		out[c.Partition] = s.err
	}
	return out, nil
}

type operatorConflict struct{}

func (operatorConflict) Error() string { return "fingerprint mismatch" }
func (operatorConflict) ExitCode() int { return 2 }

func exitCode(err error) int {
	var coded interface{ ExitCode() int }
	if errors.As(err, &coded) {
		return coded.ExitCode()
	}
	return 1
}

func TestExitCodeFollowsExecutionStage(t *testing.T) {
	for _, stage := range []string{"before intake", "after intake", "runtime then cleanup", "cleanup then runtime", "runtime then idle outcome"} {
		t.Run(stage, func(t *testing.T) {
			// Given a conflict at a particular stage of execution.
			r, source, _, _, _ := fixture(t)
			conflict, runtime := operatorConflict{}, errors.New("broker unavailable")
			want := 1
			action := func() error { return r.Run(context.Background()) }
			switch stage {
			case "before intake":
				r.source = &failingStart{testSource: source, err: conflict}
				want = 2
			case "after intake":
				input := incoming(0, 0, 1)
				source.events = []Event{{Input: &input}}
				r.source = &failingPoll{testSource: source, err: conflict}
			case "runtime then cleanup":
				r.failure = runtime
				action = func() error { return r.exit(errors.Join(runtime, conflict)) }
			case "cleanup then runtime":
				r.failure = runtime
				action = func() error { return r.exit(errors.Join(conflict, runtime)) }
			case "runtime then idle outcome":
				r.failure = runtime
				action = func() error { return r.exit(&Exit{Code: 3, Err: errors.New("worker held no partitions for 1s")}) }
			}

			// When that stage fails.
			err := action()

			// Then only a conflict before intake is an operator error.
			if code := exitCode(err); code != want {
				t.Fatalf("exit=%d want=%d", code, want)
			}
		})
	}
}

type failingStart struct {
	*testSource
	err error
}

func (s *failingStart) Start(Lifecycle, Observer) error { return s.err }

type failingPoll struct {
	*testSource
	err error
}

func (s *failingPoll) Poll(d time.Duration) (Event, error) {
	if len(s.events) == 0 {
		return Event{}, s.err
	}
	return s.testSource.Poll(d)
}
