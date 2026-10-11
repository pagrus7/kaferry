package copy

import "testing"

func TestConfirmedPrefixIncludesNumericGaps(t *testing.T) {
	// Given
	tk := NewTracker()
	tk.Assign(Cursor{0, 100})

	// When
	a, _ := tk.Observe(Cursor{0, 100}, 101, 10)
	b, _ := tk.Observe(Cursor{0, 105}, 106, 20)
	tk.Settle(b, true)

	// Then
	if tk.Confirmed(0).Position != 100 {
		t.Fatal("committed past unresolved record")
	}

	// When the missing earlier delivery succeeds.
	tk.Settle(a, true)

	// Then the consumed prefix includes numeric offset gaps.
	if tk.Confirmed(0).Position != 106 {
		t.Fatal("numeric offset gaps blocked consumed prefix")
	}
	if records, bytes := tk.Total(); records != 2 || bytes != 30 {
		t.Fatal("confirmed work was released before commit")
	}

	// When only the first confirmed position is committed.
	tk.Committed(Cursor{0, 101})

	// Then later confirmed work still counts against the caps.
	if records, bytes := tk.Total(); records != 1 || bytes != 20 {
		t.Fatal("partial commit released the wrong work")
	}

	// When the remaining confirmed position is committed.
	tk.Committed(Cursor{0, 106})

	// Then its work is released.
	if records, bytes := tk.Total(); records != 0 || bytes != 0 {
		t.Fatal("committed work retained")
	}
}

func TestOldGenerationDeliveryCannotAdvanceNewOwner(t *testing.T) {
	// Given a new assignment after the previous generation was revoked.
	tk := NewTracker()
	tk.Assign(Cursor{0, 0})
	old, _ := tk.Observe(Cursor{0, 0}, 1, 1)
	tk.Revoke(0)
	tk.Assign(Cursor{0, 0})
	current, _ := tk.Observe(Cursor{0, 0}, 1, 1)

	// When an old delivery arrives late.
	resolved := tk.Settle(old, true)

	// Then it cannot advance the new owner's confirmed prefix.
	if resolved || tk.Confirmed(0).Position != 0 {
		t.Fatal("late delivery advanced new assignment")
	}

	// When the current generation's delivery arrives.
	resolved = tk.Settle(current, true)

	// Then its progress is confirmed.
	if !resolved || tk.Confirmed(0).Position != 1 {
		t.Fatal("current delivery not confirmed")
	}
}

func TestControlRecordCursorAndTimestampStop(t *testing.T) {
	// Given
	tk := NewTracker()
	tk.Assign(Cursor{0, 0})

	// When
	err := tk.Advance(Cursor{0, 2})

	// Then
	if err != nil {
		t.Fatal(err)
	}
	if tk.Confirmed(0).Position != 2 {
		t.Fatal("control cursor did not advance")
	}

	// When an unchanged position marker follows the control cursor.
	err = tk.Advance(Cursor{0, 2})

	// Then it adds no unresolved work.
	if err != nil {
		t.Fatal(err)
	}
	tk.Committed(Cursor{0, 2})
	if records, _ := tk.Total(); records != 0 {
		t.Fatal("trigger marker retained")
	}
}

func TestBackwardsSourceCursorCannotChangeProgressOrWork(t *testing.T) {
	// Given saved progress followed by one outstanding write.
	tracker := NewTracker()
	tracker.Assign(Cursor{0, 10})
	first, err := tracker.Observe(Cursor{0, 10}, 11, 7)
	if err != nil || !tracker.Settle(first, true) {
		t.Fatal("initial progress failed", err)
	}
	tracker.Committed(Cursor{0, 11})
	pending, err := tracker.Observe(Cursor{0, 11}, 12, 9)
	if err != nil {
		t.Fatal(err)
	}

	// When a source supplies a backwards record, next position, or EOF cursor.
	for _, operation := range []func() error{
		func() error { _, err := tracker.Observe(Cursor{0, 9}, 10, 100); return err },
		func() error { _, err := tracker.Observe(Cursor{0, 12}, 11, 100); return err },
		func() error { return tracker.Advance(Cursor{0, 5}) },
	} {
		err := operation()

		// Then rejection leaves both saved progress and the outstanding work unchanged.
		if records, bytes := tracker.Total(); err == nil || records != 1 || bytes != 9 || tracker.Saved(0) != 11 || tracker.Confirmed(0).Position != 11 {
			t.Fatal("invalid cursor changed progress or work", err, records, bytes)
		}
	}

	// When the legitimate outstanding write succeeds, then it still advances normally.
	if !tracker.Settle(pending, true) || tracker.Confirmed(0).Position != 12 {
		t.Fatal("cursor rejection damaged pending delivery")
	}
	tracker.Committed(Cursor{0, 12})
	if records, bytes := tracker.Total(); records != 0 || bytes != 0 || tracker.Saved(0) != 12 {
		t.Fatal("legitimate work could not be committed")
	}
}
