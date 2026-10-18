// Package observability exports copy observations without controlling copying.
package observability

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"github.com/pagrus7/kaferry/internal/copy"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	sdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

type Session interface {
	copy.Observer
	Close(context.Context)
}

type Factory struct {
	env     Environment
	log     *slog.Logger
	version string
}

func NewFactory(env Environment, log *slog.Logger, version string) *Factory {
	return &Factory{env: env, log: log, version: version}
}

// Open starts export only after the engine has explicitly enabled metrics.
func (f *Factory) Open(group string) (Session, error) {
	if err := validate(f.env); err != nil {
		return nil, err
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, fmt.Errorf("metrics: cannot generate instance identity")
	}
	warnings := &warnings{log: f.log}
	// OTel reports SDK/export failures through its process-wide error handler.
	// It never receives raw errors in the logger, which might contain credentials.
	otel.SetErrorHandler(warnings)
	otel.SetLogger(logr.New(warningSink{warnings}))
	res, err := resource.New(context.Background(), resource.WithFromEnv(), resource.WithTelemetrySDK(),
		resource.WithAttributes(attribute.String("service.name", "kaferry"), attribute.String("service.version", f.version),
			attribute.String("service.instance.id", hex.EncodeToString(id[:])), attribute.String("messaging.consumer.group.name", group)))
	if err != nil {
		return nil, fmt.Errorf("metrics: invalid OTEL_RESOURCE_ATTRIBUTES")
	}
	exporter, err := otlpmetrichttp.New(context.Background(),
		otlpmetrichttp.WithAggregationSelector(sdk.DefaultAggregationSelector),
		otlpmetrichttp.WithRetry(otlpmetrichttp.RetryConfig{Enabled: false}),
		otlpmetrichttp.WithTemporalitySelector(sdk.CumulativeTemporalitySelector))
	if err != nil {
		return nil, fmt.Errorf("metrics: cannot configure HTTP exporter")
	}
	return newSession(sdk.NewPeriodicReader(exporter), res, warnings)
}

type runtime struct {
	latest      atomic.Pointer[copy.Snapshot]
	lastSuccess atomic.Int64
	provider    *sdk.MeterProvider
	saves       metric.Int64Counter
	duration    metric.Float64Histogram
	warnings    *warnings
}

func newSession(reader sdk.Reader, res *resource.Resource, warnings *warnings) (*runtime, error) {
	r := &runtime{provider: sdk.NewMeterProvider(sdk.WithReader(reader), sdk.WithResource(res)), warnings: warnings}
	meter := r.provider.Meter("github.com/pagrus7/kaferry")
	var err error
	r.saves, err = meter.Int64Counter("kaferry.progress.saves", metric.WithUnit("{save}"))
	if err == nil {
		r.duration, err = meter.Float64Histogram("kaferry.progress.save.duration", metric.WithUnit("s"),
			metric.WithExplicitBucketBoundaries(.001, .005, .01, .05, .1, .5, 1, 5, 10))
	}
	if err == nil {
		err = r.register(meter)
	}
	if err != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = r.provider.Shutdown(ctx)
		return nil, fmt.Errorf("metrics: cannot register instruments")
	}
	return r, nil
}

func (r *runtime) register(meter metric.Meter) error {
	var instruments []metric.Observable
	counters := make([]metric.Int64ObservableCounter, 0, 5)
	for _, name := range []string{"records.submitted", "records.confirmed", "records.filtered", "records.skipped", "loop.iterations"} {
		unit := "{record}"
		if name == "loop.iterations" {
			unit = "{iteration}"
		}
		instrument, err := meter.Int64ObservableCounter("kaferry."+name, metric.WithUnit(unit))
		if err != nil {
			return err
		}
		counters = append(counters, instrument)
		instruments = append(instruments, instrument)
	}
	gauges := make([]metric.Int64ObservableGauge, 0, 6)
	for _, def := range []struct{ name, unit string }{
		{"buffer.entries", "{entry}"}, {"buffer.estimated_size", "By"},
		{"buffer.entry_limit", "{entry}"}, {"buffer.estimated_size_limit", "By"},
		{"partitions.held", "{partition}"}, {"partitions.stopped", "{partition}"},
	} {
		instrument, err := meter.Int64ObservableGauge("kaferry."+def.name, metric.WithUnit(def.unit))
		if err != nil {
			return err
		}
		gauges = append(gauges, instrument)
		instruments = append(instruments, instrument)
	}
	last, err := meter.Float64ObservableGauge("kaferry.progress.last_success", metric.WithUnit("s"))
	if err != nil {
		return err
	}
	instruments = append(instruments, last)
	_, err = meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		if s := r.latest.Load(); s != nil {
			values := [...]int64{int64(s.Submitted), int64(s.Confirmed), int64(s.Filtered), int64(s.Skipped), int64(s.Iterations)}
			for i, value := range values {
				observer.ObserveInt64(counters[i], value)
			}
			current := [...]int64{s.PendingEntries, s.PendingEstimatedBytes, s.EntryCap, s.EstimatedByteCap, s.Partitions}
			for i, value := range current {
				observer.ObserveInt64(gauges[i], value)
			}
			for i, reason := range []string{"timestamp", "tip", "eof"} {
				value := [...]int64{s.TimestampStops, s.TipStops, s.EOFStops}[i]
				observer.ObserveInt64(gauges[5], value, metric.WithAttributes(attribute.String("reason", reason)))
			}
		}
		if at := r.lastSuccess.Load(); at != 0 {
			observer.ObserveFloat64(last, float64(at)/1e9)
		}
		return nil
	}, instruments...)
	return err
}

func (r *runtime) Publish(s copy.Snapshot) { r.latest.Store(&s) }

func (r *runtime) Saved(result copy.SaveResult) {
	ctx := context.Background()
	r.saves.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", result.Outcome)))
	r.duration.Record(ctx, result.Duration.Seconds())
	if result.Outcome == "success" {
		r.lastSuccess.Store(result.At.UnixNano())
	}
}

func (r *runtime) Close(ctx context.Context) {
	// Do not let an exporter or a blocked diagnostic delay process cleanup.
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := r.provider.Shutdown(ctx); err != nil {
			r.warnings.Handle(err)
		}
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

type warnings struct {
	log  *slog.Logger
	next atomic.Int64
	busy atomic.Bool
}

func (w *warnings) Handle(error) {
	now := time.Now().UnixNano()
	previous := w.next.Load()
	if now < previous || !w.next.CompareAndSwap(previous, now+int64(30*time.Second)) || !w.busy.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer w.busy.Store(false)
		w.log.Warn("metrics export failed; copying continues; check collector settings")
	}()
}

// SDK diagnostics can include environment values; discard their text and fields.
type warningSink struct{ warnings *warnings }

func (warningSink) Init(logr.RuntimeInfo)                 {}
func (warningSink) Enabled(int) bool                      { return false }
func (warningSink) Info(int, string, ...any)              {}
func (s warningSink) Error(err error, _ string, _ ...any) { s.warnings.Handle(err) }
func (s warningSink) WithValues(...any) logr.LogSink      { return s }
func (s warningSink) WithName(string) logr.LogSink        { return s }
