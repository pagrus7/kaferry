package observability

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/copy"
	sdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
)

func metricValues(t *testing.T, reader *sdk.ManualReader) map[string]metricdata.Metrics {
	t.Helper()
	var data metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &data); err != nil {
		t.Fatal(err)
	}
	out := map[string]metricdata.Metrics{}
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			out[m.Name] = m
		}
	}
	return out
}
func quietWarnings() *warnings { return &warnings{log: slog.New(slog.NewJSONHandler(io.Discard, nil))} }
func TestMetricsDescribeCopyWorkAndFullySuccessfulSaves(t *testing.T) {
	// Given a fresh provider with deterministic collection and two snapshots.
	reader := sdk.NewManualReader()
	r, err := newSession(reader, resource.Empty(), quietWarnings())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(context.Background())
	r.Publish(copy.Snapshot{Submitted: 8, Confirmed: 5, Filtered: 3, Skipped: 1, Iterations: 12, PendingEntries: 6, PendingEstimatedBytes: 500, EntryCap: 100, EstimatedByteCap: 1000, Partitions: 2, EOFStops: 1})
	at := time.Unix(123, 0)
	r.Saved(copy.SaveResult{Outcome: "success", At: at, Duration: 5 * time.Millisecond})
	r.Saved(copy.SaveResult{Outcome: "partial", At: at.Add(time.Second), Duration: 10 * time.Millisecond})
	r.Saved(copy.SaveResult{Outcome: "failure", At: at.Add(2 * time.Second), Duration: 20 * time.Millisecond})
	// When collecting metrics through the real SDK.
	values := metricValues(t, reader)
	// Then overlapping record states remain separate cumulative counters.
	for name, want := range map[string]int64{"records.submitted": 8, "records.confirmed": 5, "records.filtered": 3, "records.skipped": 1, "loop.iterations": 12} {
		m := values["kaferry."+name]
		sum, ok := m.Data.(metricdata.Sum[int64])
		if !ok || !sum.IsMonotonic || sum.Temporality != metricdata.CumulativeTemporality || len(sum.DataPoints) != 1 || sum.DataPoints[0].Value != want {
			t.Fatal(name, m)
		}
	}
	for name, want := range map[string]int64{"buffer.entries": 6, "buffer.estimated_size": 500, "buffer.entry_limit": 100, "buffer.estimated_size_limit": 1000, "partitions.held": 2} {
		g, ok := values["kaferry."+name].Data.(metricdata.Gauge[int64])
		if !ok || len(g.DataPoints) != 1 || g.DataPoints[0].Value != want {
			t.Fatal(name, g)
		}
	}
	if values["kaferry.buffer.estimated_size"].Unit != "By" {
		t.Fatal("incorrect bytes unit")
	}
	saves := values["kaferry.progress.saves"].Data.(metricdata.Sum[int64])
	if len(saves.DataPoints) != 3 {
		t.Fatal(saves)
	}
	for _, point := range saves.DataPoints {
		v, ok := point.Attributes.Value("outcome")
		if !ok || (v.AsString() != "success" && v.AsString() != "partial" && v.AsString() != "failure") || point.Value != 1 {
			t.Fatal(point)
		}
	}
	h := values["kaferry.progress.save.duration"].Data.(metricdata.Histogram[float64])
	if values["kaferry.progress.save.duration"].Unit != "s" || h.DataPoints[0].Count != 3 || h.DataPoints[0].Bounds[0] != .001 {
		t.Fatal(h)
	}
	last := values["kaferry.progress.last_success"].Data.(metricdata.Gauge[float64])
	if last.DataPoints[0].Value != 123 {
		t.Fatal(last)
	}
	// When ownership ends, then current gauges clear and cumulative counters retain their values.
	r.Publish(copy.Snapshot{Submitted: 8, Confirmed: 5, Iterations: 13})
	values = metricValues(t, reader)
	if values["kaferry.partitions.held"].Data.(metricdata.Gauge[int64]).DataPoints[0].Value != 0 {
		t.Fatal(values)
	}
	for _, p := range values["kaferry.partitions.stopped"].Data.(metricdata.Gauge[int64]).DataPoints {
		if p.Value != 0 {
			t.Fatal(p)
		}
	}
}

func TestNoSuccessfulSaveTimestampBeforeSuccess(t *testing.T) {
	// Given a worker with no fully successful save.
	reader := sdk.NewManualReader()
	r, err := newSession(reader, resource.Empty(), quietWarnings())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(context.Background())
	r.Saved(copy.SaveResult{Outcome: "partial", At: time.Now()})
	// When collecting, then no last-success timestamp is invented.
	if _, ok := metricValues(t, reader)["kaferry.progress.last_success"]; ok {
		t.Fatal("invented successful save")
	}
}
