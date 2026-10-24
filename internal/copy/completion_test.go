package copy

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, r.Assign([]Assignment{{Cursor: Cursor{Partition: 0, Position: 0}, Window: plan.Window{Upper: &upper}}}))
	input := incoming(0, 0, 1)
	input.Record.TimestampMS = &stamp
	require.NoError(t, r.input(input))
	require.NoError(t, r.eof(Cursor{Partition: 0, Position: 1}))

	// When checking completion without advancing wall time.
	require.NoError(t, r.tips())

	// Then equality filters the record without stopping or sampling the log end.
	if query.calls != 0 || r.partitions[0].stop != nil || r.timestampStops != 0 || r.filtered != 1 || len(sink.writes) != 0 {
		t.Fatal("threshold equality stopped the partition", query.calls, r.partitions[0], sink.writes)
	}

	// When wall time passes the threshold and the already-consumed tip is sampled.
	clock.Sleep(time.Millisecond)
	require.NoError(t, r.tips())
	require.NoError(t, r.commit(r.ids()))

	// Then the tip rule stops and saves the next position, not a timestamp trigger.
	if query.calls != 1 || !r.readyToExit() || r.tipStops != 1 || r.timestampStops != 0 || !reflect.DeepEqual(store.saves, [][]Cursor{{{Partition: 0, Position: 1, Stop: &Stop{Reason: "tip", At: 1}}}}) {
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
	require.NoError(t, r.Assign([]Assignment{{Cursor: Cursor{Partition: 0, Position: 0}, Window: plan.Window{Upper: &upper}}}))
	require.NoError(t, r.eof(Cursor{Partition: 0, Position: 0}))
	require.NoError(t, r.tips())
	first, later := incoming(0, 0, 1), incoming(0, 5, 1)
	first.Record.TimestampMS, later.Record.TimestampMS = &stamp, &stamp
	require.NoError(t, r.input(first))

	// When new data beyond the sampled end proves the intervening cursor gap.
	source.high = 100
	require.NoError(t, r.input(later))
	require.NoError(t, r.tips())
	require.NoError(t, r.commit(r.ids()))

	// Then only the first record was submitted and unresolved work prevents saving.
	if query.calls != 1 || len(sink.writes) != 1 || r.readyToExit() || len(store.saves) != 0 || r.tracker.Confirmed(0).Position != 0 {
		t.Fatal("copied past the tip or saved unresolved work", query.calls, sink.writes, store.saves)
	}

	// When the first write is acknowledged.
	sink.reports <- Delivery{Token: sink.writes[0]}
	require.NoError(t, r.deliveries())
	require.NoError(t, r.commit(r.ids()))

	// Then completion saves exactly the original end and preserves diagnostic order.
	if !r.readyToExit() || !reflect.DeepEqual(store.saves, [][]Cursor{{{Partition: 0, Position: 3, Stop: &Stop{Reason: "tip", At: 3}}}}) {
		t.Fatal("sampled end changed", store.saves)
	}
	var messages []string
	var details []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(logs.Bytes()), []byte{'\n'}) {
		var attributes map[string]any
		require.NoError(t, json.Unmarshal(line, &attributes))
		messages = append(messages, attributes["msg"].(string))
		details = append(details, attributes)
	}
	require.Equal(t, []string{"partition assigned", "tip watermark sampled", "partition stopped", "progress committed"}, messages, "completion diagnostics changed")
	for i, expected := range []map[string]any{
		{"partition": float64(0), "position": float64(0)},
		{"partition": float64(0), "position": float64(0), "stable": float64(0), "high_watermark": float64(3)},
		{"partition": float64(0), "position": float64(3), "reason": "tip", "high_watermark": float64(3)},
		{"partition": float64(0), "position": float64(3)},
	} {
		for key, want := range expected {
			if got := details[i][key]; got != want {
				t.Fatalf("event %d %s: got %v, want %v", i, key, got, want)
			}
		}
	}
}
