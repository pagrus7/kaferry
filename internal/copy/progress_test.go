package copy

import "testing"

func TestConfirmedPrefixIncludesNumericGaps(t *testing.T) {
	// Given
	tk := NewTracker()
	tk.Assign(Cursor{0, 100})

	// When
	a, _ := tk.Observe(Cursor{0, 100}, 101, 10, false)
	b, _ := tk.Observe(Cursor{0, 105}, 106, 20, false)
	tk.Resolve(b)

	// Then
	if tk.Confirmed(0).Position != 100 {
		t.Fatal("committed past unresolved record")
	}
	tk.Resolve(a)
	if tk.Confirmed(0).Position != 106 {
		t.Fatal("numeric offset gaps blocked consumed prefix")
	}
	if records, bytes := tk.Total(); records != 2 || bytes != 30 {
		t.Fatal("confirmed work was released before commit")
	}
	tk.Committed(Cursor{0, 101})
	if records, bytes := tk.Total(); records != 1 || bytes != 20 {
		t.Fatal("partial commit released the wrong work")
	}
	tk.Committed(Cursor{0, 106})
	if records, bytes := tk.Total(); records != 0 || bytes != 0 {
		t.Fatal("committed work retained")
	}
}

func TestOldGenerationDeliveryCannotAdvanceNewOwner(t *testing.T) {
	// Given a new assignment after the previous generation was revoked.
	tk := NewTracker()
	tk.Assign(Cursor{0, 0})
	old, _ := tk.Observe(Cursor{0, 0}, 1, 1, false)
	tk.Revoke(0)
	tk.Assign(Cursor{0, 0})
	current, _ := tk.Observe(Cursor{0, 0}, 1, 1, false)

	// When an old delivery arrives late.
	resolved := tk.Resolve(old)

	// Then it cannot advance the new owner's confirmed prefix.
	if resolved || tk.Confirmed(0).Position != 0 {
		t.Fatal("late delivery advanced new assignment")
	}

	// When the current generation's delivery arrives.
	resolved = tk.Resolve(current)

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
	_, err := tk.Observe(Cursor{0, 0}, 2, 0, true)

	// Then
	if err != nil {
		t.Fatal(err)
	}
	if tk.Confirmed(0).Position != 2 {
		t.Fatal("control cursor did not advance")
	}
	_, err = tk.Observe(Cursor{0, 2}, 2, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	tk.Committed(Cursor{0, 2})
	if records, _ := tk.Total(); records != 0 {
		t.Fatal("trigger marker retained")
	}
}
