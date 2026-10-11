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
)

type testClock struct {
	now  time.Time
	tick func()
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

func (s *testSource) Start(l Lifecycle) error { return l.Assign(s.assignments) }
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
	s := &testSource{clock: c, paused: make(map[int32]bool), assignments: []Assignment{{Cursor: Cursor{0, 0}}}}
	d := &testSink{reports: make(chan Delivery, 100)}
	p := &testStore{}
	r := NewRunner(s, d, p, c, slog.New(slog.NewTextHandler(io.Discard, nil)), plan.Plan{Source: plan.Endpoint{Kind: "FILE"}}, config.Defaults(t.TempDir()).Worker, NewTracker())
	return r, s, d, p, c
}
func incoming(id int32, position int64, size int) Input {
	return Input{Cursor: Cursor{id, position}, Next: position + 1, Record: Record{Value: make([]byte, size)}}
}

func TestRunnerDoesNotCommitPastFailedWrite(t *testing.T) {
	// Given
	r, s, d, p, _ := fixture(t)
	for i := range 3 {
		in := incoming(0, int64(i), 1)
		s.events = append(s.events, Event{Input: &in})
	}
	s.events = append(s.events, Event{EOF: &Cursor{0, 3}})
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
	if runErr == nil {
		t.Fatal("failed write was accepted")
	}
	if len(p.saves) != 1 || p.saves[0][0].Position != 1 {
		t.Fatalf("unsafe checkpoint: %+v", p.saves)
	}
}

func TestRunnerCapsIncludeConfirmedWorkAndOversizedAlone(t *testing.T) {
	// Given
	r, _, d, p, _ := fixture(t)
	r.limits.ByteCap = 150
	r.limits.PartitionByteCap = 150
	if err := r.Assign([]Assignment{{Cursor: Cursor{0, 0}}}); err != nil {
		t.Fatal(err)
	}

	// When
	inputErr := r.input(incoming(0, 0, 30))

	// Then
	if inputErr != nil {
		t.Fatal(inputErr)
	}

	// When the first write succeeds but is still uncommitted.
	d.reports <- Delivery{Token: d.writes[0]}
	if err := r.deliveries(); err != nil {
		t.Fatal(err)
	}
	if err := r.input(incoming(0, 1, 200)); err != nil {
		t.Fatal(err)
	}

	// Then the oversized record waits for the outstanding work.
	if len(d.writes) != 1 || r.deferred == nil {
		t.Fatal("oversized record submitted alongside uncommitted work")
	}

	// When confirmed progress is committed and intake resumes.
	if err := r.pause(); err != nil {
		t.Fatal(err)
	}
	if err := r.commit(r.ids()); err != nil {
		t.Fatal(err)
	}
	if err := r.admitDeferred(); err != nil {
		t.Fatal(err)
	}

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
	s.events = []Event{{Input: &in}, {EOF: &Cursor{0, 1}}}
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
	if runErr != nil {
		t.Fatal(runErr)
	}
	if tries != 3 || len(d.writes) != 1 {
		t.Fatalf("tries=%d submitted=%d", tries, len(d.writes))
	}
}

func TestRevokeFencesLateOutcomesAndLostOwnershipNeverCommits(t *testing.T) {
	// Given
	for _, lost := range []bool{false, true} {
		t.Run(map[bool]string{false: "revoked", true: "lost"}[lost], func(t *testing.T) {
			r, _, d, p, _ := fixture(t)
			if err := r.Assign([]Assignment{{Cursor: Cursor{0, 0}}}); err != nil {
				t.Fatal(err)
			}
			_ = r.input(incoming(0, 0, 1))
			old := d.writes[0]

			// When
			revokeErr := r.Revoke([]int32{0}, lost)

			// Then
			if revokeErr != nil {
				t.Fatal(revokeErr)
			}
			if err := r.Assign([]Assignment{{Cursor: Cursor{0, 0}}}); err != nil {
				t.Fatal(err)
			}
			_ = r.input(incoming(0, 0, 1))
			d.reports <- Delivery{Token: old, Err: errors.New("late rejected write")}
			if err := r.deliveries(); err != nil {
				t.Fatal("late outcome affected new owner", err)
			}
			if r.tracker.Resolved(0) || len(p.saves) != 0 {
				t.Fatal("late delivery advanced durable progress")
			}
		})
	}
	r, _, d, p, _ := fixture(t)
	if err := r.Assign([]Assignment{{Cursor: Cursor{0, 0}}}); err != nil {
		t.Fatal(err)
	}
	_ = r.input(incoming(0, 0, 1))

	// When the first write succeeds but is still uncommitted.
	d.reports <- Delivery{Token: d.writes[0]}
	if err := r.deliveries(); err != nil {
		t.Fatal(err)
	}
	_ = r.Revoke([]int32{0}, true)
	if len(p.saves) != 0 {
		t.Fatal("committed after ownership loss")
	}
}

func TestPartialCommitReleasesSuccessfulPartitionsOnly(t *testing.T) {
	for _, scenario := range []string{"failed result", "missing result"} {
		t.Run(scenario, func(t *testing.T) {
			// Given confirmed writes on two partitions and one unconfirmed commit result.
			r, _, d, p, _ := fixture(t)
			if err := r.Assign([]Assignment{{Cursor: Cursor{0, 0}}, {Cursor: Cursor{1, 0}}}); err != nil {
				t.Fatal(err)
			}
			for _, id := range []int32{0, 1} {
				if err := r.input(incoming(id, 0, 1)); err != nil {
					t.Fatal(err)
				}
			}
			for _, tok := range d.writes {
				d.reports <- Delivery{Token: tok}
			}
			if err := r.deliveries(); err != nil {
				t.Fatal(err)
			}
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
	if err := r.Assign([]Assignment{{Cursor: Cursor{0, 0}}}); err != nil {
		t.Fatal(err)
	}
	_ = r.input(incoming(0, 0, 1))
	_ = r.input(incoming(0, 1, 1))
	d.reports <- Delivery{Token: d.writes[0]}
	start := c.Now()

	// When
	drainErr := r.drain()

	// Then
	if drainErr == nil {
		t.Fatal("unacknowledged write treated as drained")
	}
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
	r.plan.CutoffMS = 1000
	s.assignments[0].Window.Upper = &hi
	in := incoming(0, 0, 1)
	in.Record.TimestampMS = &stamp
	s.events = []Event{{Input: &in}}

	// When
	runErr := r.Run(context.Background())

	// Then
	if runErr != nil {
		t.Fatal(runErr)
	}
	if len(d.writes) != 0 || len(p.saves) != 0 {
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
	_ = r.Assign([]Assignment{{Cursor: Cursor{0, 0}, Window: plan.Window{Upper: &hi}}})
	s.high = 2
	_ = r.eof(Cursor{0, 0})

	// When
	tipsErr := r.tips()

	// Then
	if tipsErr != nil {
		t.Fatal(tipsErr)
	}

	// When the topic grows after the sample and tip_wait expires.
	s.high = 999
	c.Sleep(2 * time.Second)
	tipsErr = r.tips()

	// Then the original boundary stays fixed and the diagnostic names the cursors.
	if *r.partitions[0].high != 2 {
		t.Fatal("tip boundary moved")
	}
	if tipsErr == nil || !strings.Contains(tipsErr.Error(), "H=2 stable=0 consumed=0") {
		t.Fatal("stalled transaction must name its boundary and cursors", tipsErr)
	}
}

func TestEmptyAssignmentIdleExitRequiresCompletedAssignment(t *testing.T) {
	// Given a bounded worker that has never received an assignment.
	r, _, _, _, c := fixture(t)
	r.limits.IdleExit = "1s"
	c.Sleep(time.Hour)

	// When its idle deadline elapses before assignment.
	finished := r.finished()

	// Then it remains available for a first assignment.
	if finished {
		t.Fatal("exited before assignment")
	}
	_ = r.Assign(nil)
	c.Sleep(2 * time.Second)
	if !r.finished() {
		t.Fatal("bounded idle worker did not exit")
	}
	if err := r.Assign([]Assignment{{Cursor: Cursor{0, 0}}}); err != nil {
		t.Fatal(err)
	}
	_ = r.Revoke([]int32{0}, false)
	c.Sleep(time.Hour)
	if r.finished() {
		t.Fatal("empty rebalance treated as completion")
	}
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
	if runErr != nil {
		t.Fatal(runErr)
	}
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
	if !errors.Is(runErr, failure) {
		t.Fatal("signal hid a known failure", runErr)
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
			s.events = []Event{{EOF: &Cursor{0, 0}}, {Input: &first}, {Input: &second}}
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
			if runErr != nil {
				t.Fatal("sampled H incorrectly required another EOF", runErr)
			}
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
			if err := r.Assign([]Assignment{{Cursor: Cursor{0, 0}}}); err != nil {
				t.Fatal(err)
			}
			in := incoming(0, 0, 1)
			token, _ := r.tracker.Observe(in.Cursor, in.Next, 1)
			r.tracker.Settle(token, true)

			// When
			err := r.Revoke([]int32{0}, false)

			// Then
			if (err != nil) != tc.fails {
				t.Fatalf("revoke error = %v, want failure=%v", err, tc.fails)
			}
			if len(r.partitions) != 0 || store.calls != 1 {
				t.Fatalf("partition not released or commit retried: partitions=%d calls=%d", len(r.partitions), store.calls)
			}
		})
	}
	// Outside revocation a recoverable error still fails the commit.
	r, _, _, _, _ := fixture(t)
	r.store = &requestStore{err: &Recoverable{Err: errors.New("rebalance in progress")}}
	if err := r.Assign([]Assignment{{Cursor: Cursor{0, 0}}}); err != nil {
		t.Fatal(err)
	}
	token, _ := r.tracker.Observe(Cursor{0, 0}, 1, 1)
	r.tracker.Settle(token, true)
	if err := r.commit([]int32{0}); err == nil {
		t.Fatal("periodic commit must fail on a recoverable error")
	}
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
	for _, stage := range []string{"before intake", "after intake", "runtime then cleanup", "cleanup then runtime"} {
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

func (s *failingStart) Start(Lifecycle) error { return s.err }

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
