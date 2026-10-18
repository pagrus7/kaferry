package observability

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/copy"
	"github.com/stretchr/testify/require"
	sdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
)

func metricValues(t *testing.T, reader *sdk.ManualReader) map[string]metricdata.Metrics {
	t.Helper()
	var data metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &data))
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
	require.NoError(t, err)
	defer r.Close(context.Background())
	r.Publish(copy.Snapshot{Submitted: 8, Confirmed: 5, Filtered: 3, Skipped: 1, Iterations: 12, PendingEntries: 6, PendingEstimatedBytes: 500, EntryCap: 100, EstimatedByteCap: 1000, Partitions: 9, TimestampStops: 2, TipStops: 3, EOFStops: 4})
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
		require.True(t, ok, "%s must be an integer sum: %v", name, m)
		require.True(t, sum.IsMonotonic, name)
		require.Equal(t, metricdata.CumulativeTemporality, sum.Temporality, name)
		require.Len(t, sum.DataPoints, 1, name)
		require.Equal(t, want, sum.DataPoints[0].Value, name)
	}
	for name, want := range map[string]int64{"buffer.entries": 6, "buffer.estimated_size": 500, "buffer.entry_limit": 100, "buffer.estimated_size_limit": 1000, "partitions.held": 9} {
		g, ok := values["kaferry."+name].Data.(metricdata.Gauge[int64])
		require.True(t, ok, "%s must be an integer gauge", name)
		require.Len(t, g.DataPoints, 1, name)
		require.Equal(t, want, g.DataPoints[0].Value, name)
	}
	stopped := map[string]int64{}
	stops := values["kaferry.partitions.stopped"].Data.(metricdata.Gauge[int64])
	require.Len(t, stops.DataPoints, 3)
	for _, point := range stops.DataPoints {
		reason, ok := point.Attributes.Value("reason")
		require.True(t, ok)
		stopped[reason.AsString()] = point.Value
	}
	require.Equal(t, map[string]int64{"timestamp": 2, "tip": 3, "eof": 4}, stopped)
	require.Equal(t, "By", values["kaferry.buffer.estimated_size"].Unit)
	saves := values["kaferry.progress.saves"].Data.(metricdata.Sum[int64])
	require.Len(t, saves.DataPoints, 3)
	for _, point := range saves.DataPoints {
		v, ok := point.Attributes.Value("outcome")
		require.True(t, ok)
		require.Contains(t, []string{"success", "partial", "failure"}, v.AsString())
		require.Equal(t, int64(1), point.Value)
	}
	h := values["kaferry.progress.save.duration"].Data.(metricdata.Histogram[float64])
	require.Equal(t, "s", values["kaferry.progress.save.duration"].Unit)
	require.Len(t, h.DataPoints, 1)
	point := h.DataPoints[0]
	require.Equal(t, uint64(3), point.Count)
	require.InDelta(t, .035, point.Sum, 1e-12)
	require.Equal(t, []float64{.001, .005, .01, .05, .1, .5, 1, 5, 10}, point.Bounds)
	require.Equal(t, []uint64{0, 1, 1, 1, 0, 0, 0, 0, 0, 0}, point.BucketCounts)
	last := values["kaferry.progress.last_success"].Data.(metricdata.Gauge[float64])
	require.Equal(t, float64(123), last.DataPoints[0].Value)
	// When ownership ends, then current gauges clear and cumulative counters retain their values.
	r.Publish(copy.Snapshot{Submitted: 8, Confirmed: 5, Iterations: 13})
	values = metricValues(t, reader)
	require.Zero(t, values["kaferry.partitions.held"].Data.(metricdata.Gauge[int64]).DataPoints[0].Value)
	for _, p := range values["kaferry.partitions.stopped"].Data.(metricdata.Gauge[int64]).DataPoints {
		require.Zero(t, p.Value)
	}
}

func TestNoSuccessfulSaveTimestampBeforeSuccess(t *testing.T) {
	// Given a worker with no fully successful save.
	reader := sdk.NewManualReader()
	r, err := newSession(reader, resource.Empty(), quietWarnings())
	require.NoError(t, err)
	defer r.Close(context.Background())
	r.Saved(copy.SaveResult{Outcome: "partial", At: time.Now()})
	// When collecting, then no last-success timestamp is invented.
	require.NotContains(t, metricValues(t, reader), "kaferry.progress.last_success")
}
