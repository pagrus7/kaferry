package copy

import (
	"errors"
	"testing"

	"github.com/pagrus7/kaferry/internal/plan"
)

func TestStopSavedOnlyAfterEarlierWritesResolve(t *testing.T) {
	for _, release := range []string{"periodic", "revoke", "lost"} {
		t.Run(release, func(t *testing.T) {
			// Given one unacknowledged write followed by a timestamp stopping record.
			r, _, sink, store, _ := fixture(t)
			r.plan.Source.Kind = "TOPIC"
			upper := int64(100)
			if err := r.Assign([]Assignment{{Cursor: Cursor{Partition: 0, Position: 0}, Window: plan.Window{Upper: &upper}}}); err != nil {
				t.Fatal(err)
			}
			first := incoming(0, 0, 1)
			stamp := int64(1)
			first.Record.TimestampMS = &stamp
			if err := r.input(first); err != nil {
				t.Fatal(err)
			}
			trigger := incoming(0, 1, 1)
			future := int64(101)
			trigger.Record.TimestampMS = &future
			if err := r.input(trigger); err != nil {
				t.Fatal(err)
			}
			if err := r.commit(r.ids()); err != nil || len(store.saves) != 0 {
				t.Fatal("unconfirmed stop saved", err, store.saves)
			}
			// When the earlier write succeeds, then periodic/revoke commits can carry the stop; ownership loss cannot.
			sink.reports <- Delivery{Token: sink.writes[0]}
			if err := r.deliveries(); err != nil {
				t.Fatal(err)
			}
			var err error
			switch release {
			case "periodic":
				err = r.commit(r.ids())
			case "revoke":
				err = r.Revoke([]int32{0}, false)
			case "lost":
				err = r.Revoke([]int32{0}, true)
			}
			if err != nil {
				t.Fatal(err)
			}
			if release == "lost" {
				if len(store.saves) != 0 {
					t.Fatal(store.saves)
				}
				return
			}
			if len(store.saves) != 1 || store.saves[0][0].Position != 1 || store.saves[0][0].Stop == nil || store.saves[0][0].Stop.At != 1 {
				t.Fatal(store.saves)
			}
			if release == "periodic" {
				if err := r.commit(r.ids()); err != nil || len(store.saves) != 1 {
					t.Fatal("stop saved repeatedly", err, store.saves)
				}
			}
		})
	}
}
func TestFailedStopCommitCanBeRetriedAtSamePosition(t *testing.T) {
	// Given an empty file partition whose stop does not advance its cursor.
	r, _, _, store, _ := fixture(t)
	if err := r.Assign([]Assignment{{Cursor: Cursor{Position: 10}}}); err != nil {
		t.Fatal(err)
	}
	if err := r.eof(Cursor{Position: 10}); err != nil {
		t.Fatal(err)
	}
	failed := errors.New("disk failed")
	store.fail = map[int32]error{0: failed}
	// When the first metadata-only save fails and the store recovers.
	if err := r.commit(r.ids()); !errors.Is(err, failed) {
		t.Fatal(err)
	}
	if r.partitions[0].stopSaved {
		t.Fatal("failed stop treated as durable")
	}
	delete(store.fail, 0)
	if err := r.commit(r.ids()); err != nil {
		t.Fatal(err)
	}
	// Then the same position is retried and carries an EOF stop.
	if len(store.saves) != 2 || store.saves[1][0].Position != 10 || store.saves[1][0].Stop.Reason != "eof" {
		t.Fatal(store.saves)
	}
}
