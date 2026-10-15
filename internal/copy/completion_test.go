package copy

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/plan"
)

func TestRunnerThresholdEqualityDoesNotStopUntilWallTimePasses(t *testing.T) {
	// Given a record and EOF exactly at the timestamp/wall-time threshold.
	r, source, sink, store, clock := fixture(t)
	upper, stamp := int64(1000), int64(2000)
	r.plan.Source.Kind = "TOPIC"
	r.plan.CutoffMS = 1000
	clock.now = time.UnixMilli(stamp)
	source.high = 1
	query := &watermarkSource{testSource: source}
	r.source = query
	if err := r.Assign([]Assignment{{Cursor: Cursor{0, 0}, Window: plan.Window{Upper: &upper}}}); err != nil {
		t.Fatal(err)
	}
	input := incoming(0, 0, 1)
	input.Record.TimestampMS = &stamp
	if err := r.input(input); err != nil {
		t.Fatal(err)
	}
	if err := r.eof(Cursor{0, 1}); err != nil {
		t.Fatal(err)
	}

	// When checking completion without advancing wall time.
	if err := r.tips(); err != nil {
		t.Fatal(err)
	}

	// Then equality filters the record without stopping or sampling the log end.
	if query.calls != 0 || r.partitions[0].stopped || r.timestampStops != 0 || r.filtered != 1 || len(sink.writes) != 0 {
		t.Fatal("threshold equality stopped the partition", query.calls, r.partitions[0], sink.writes)
	}

	// When wall time passes the threshold and the already-consumed tip is sampled.
	clock.Sleep(time.Millisecond)
	if err := r.tips(); err != nil {
		t.Fatal(err)
	}
	if err := r.commit(r.ids()); err != nil {
		t.Fatal(err)
	}

	// Then the tip rule stops and saves the next position, not a timestamp trigger.
	if query.calls != 1 || !r.readyToExit() || r.tipStops != 1 || r.timestampStops != 0 || !reflect.DeepEqual(store.saves, [][]Cursor{{{0, 1}}}) {
		t.Fatal("incorrect tip completion", query.calls, r.tipStops, store.saves)
	}
}

func TestRunnerFixedTipWaitsForAcknowledgementAndDoesNotCopyLaterRecords(t *testing.T) {
	// Given a sampled end of 3, an unacknowledged write, and a later visible record.
	r, source, sink, store, _ := fixture(t)
	upper, stamp := int64(1000), int64(500)
	r.plan.Source.Kind = "TOPIC"
	source.high = 3
	query := &watermarkSource{testSource: source}
	r.source = query
	var logs bytes.Buffer
	r.log = slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	if err := r.Assign([]Assignment{{Cursor: Cursor{0, 0}, Window: plan.Window{Upper: &upper}}}); err != nil {
		t.Fatal(err)
	}
	if err := r.eof(Cursor{0, 0}); err != nil {
		t.Fatal(err)
	}
	if err := r.tips(); err != nil {
		t.Fatal(err)
	}
	first, later := incoming(0, 0, 1), incoming(0, 5, 1)
	first.Record.TimestampMS, later.Record.TimestampMS = &stamp, &stamp
	if err := r.input(first); err != nil {
		t.Fatal(err)
	}

	// When new data beyond the sampled end proves the intervening cursor gap.
	source.high = 100
	if err := r.input(later); err != nil {
		t.Fatal(err)
	}
	if err := r.tips(); err != nil {
		t.Fatal(err)
	}
	if err := r.commit(r.ids()); err != nil {
		t.Fatal(err)
	}

	// Then only the first record was submitted and unresolved work prevents saving.
	if query.calls != 1 || len(sink.writes) != 1 || r.readyToExit() || len(store.saves) != 0 || r.tracker.Confirmed(0).Position != 0 {
		t.Fatal("copied past the tip or saved unresolved work", query.calls, sink.writes, store.saves)
	}

	// When the first write is acknowledged.
	sink.reports <- Delivery{Token: sink.writes[0]}
	if err := r.deliveries(); err != nil {
		t.Fatal(err)
	}
	if err := r.commit(r.ids()); err != nil {
		t.Fatal(err)
	}

	// Then completion saves exactly the original end and preserves diagnostic order.
	if !r.readyToExit() || !reflect.DeepEqual(store.saves, [][]Cursor{{{0, 3}}}) {
		t.Fatal("sampled end changed", store.saves)
	}
	var messages []string
	for _, line := range bytes.Split(bytes.TrimSpace(logs.Bytes()), []byte{'\n'}) {
		var event struct {
			Message string `json:"msg"`
		}
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatal(err)
		}
		messages = append(messages, event.Message)
	}
	if !reflect.DeepEqual(messages, []string{"partition assigned", "tip watermark sampled", "partition stopped", "progress committed"}) {
		t.Fatal("completion diagnostics changed", messages)
	}
}
