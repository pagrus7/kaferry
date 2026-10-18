package observability

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	goruntime "runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/copy"
	sdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

type warningOutput struct {
	mu               sync.Mutex
	data             bytes.Buffer
	entered, release chan struct{}
	once             sync.Once
}

func (w *warningOutput) Write(data []byte) (int, error) {
	if w.entered != nil {
		w.once.Do(func() { close(w.entered) })
		<-w.release
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.data.Write(data)
}
func (w *warningOutput) text() string { w.mu.Lock(); defer w.mu.Unlock(); return w.data.String() }
func warningIdle(t *testing.T, w *warnings) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	for w.busy.Load() {
		select {
		case <-ctx.Done():
			t.Fatal("warning writer did not finish")
		default:
			goruntime.Gosched()
		}
	}
}
func TestWarningsAreRedactedAndRateLimited(t *testing.T) {
	// Given export failures containing credentials and a synchronized log capture.
	output := &warningOutput{}
	w := &warnings{log: slog.New(slog.NewJSONHandler(output, nil))}
	failure := errors.New("https://user:password@collector Authorization=secret-token")
	// When repeated failures occur after each preceding warning completes.
	for range 10 {
		w.Handle(failure)
		warningIdle(t, w)
	}
	// Then only one fixed warning appears, without the underlying error's contents.
	text := output.text()
	if strings.Count(text, "metrics reported an error") != 1 || strings.Contains(text, "password") || strings.Contains(text, "secret-token") || strings.Contains(text, "Authorization") {
		t.Fatal(text)
	}
}

func TestSDKDiagnosticsAreRedactedWithoutPreventingExport(t *testing.T) {
	// Given an invalid generic timeout overridden by a valid metrics timeout.
	requests := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/x-protobuf")
		select {
		case requests <- struct{}{}:
		default:
		}
	}))
	defer server.Close()
	metricEnv(t, server.URL+"/v1/metrics")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "fixture-secret-invalid-timeout")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_TIMEOUT", "1000")
	output := &warningOutput{}
	// When the real SDK logs the generic parse failure and exports using the override.
	session, err := NewFactory(systemEnvironment{}, slog.New(slog.NewJSONHandler(output, nil)), "test").Open("group")
	if err != nil {
		t.Fatal(err)
	}
	session.Publish(copy.Snapshot{Confirmed: 1})
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	session.Close(ctx)
	warningIdle(t, session.(*runtime).warnings)
	// Then export succeeds and the installed SDK logger emits only a fixed, redacted warning.
	select {
	case <-requests:
	case <-ctx.Done():
		t.Fatal("SDK diagnostic prevented export")
	}
	text := output.text()
	if strings.Count(text, "metrics reported an error") != 1 || strings.Contains(text, "fixture-secret") || strings.Contains(text, "parse duration") {
		t.Fatal(text)
	}
}

func TestBlockedWarningCannotHoldMetricsCleanup(t *testing.T) {
	// Given a diagnostic writer which blocks after it starts handling a failure.
	output := &warningOutput{entered: make(chan struct{}), release: make(chan struct{})}
	defer close(output.release)
	w := &warnings{log: slog.New(slog.NewJSONHandler(output, nil))}
	session, err := newSession(sdk.NewManualReader(), resource.Empty(), w)
	if err != nil {
		t.Fatal(err)
	}
	handled := make(chan struct{})
	go func() { w.Handle(errors.New("secret")); close(handled) }()
	select {
	case <-handled:
	case <-time.After(time.Second):
		t.Fatal("warning call blocked")
	}
	select {
	case <-output.entered:
	case <-time.After(time.Second):
		t.Fatal("writer was not reached")
	}
	// When another failure arrives and the exporter closes while that write is blocked.
	done := make(chan struct{})
	go func() {
		w.Handle(errors.New("secret"))
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		session.Close(ctx)
		close(done)
	}()
	// Then neither observation nor cleanup waits for the writer.
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("diagnostic blocked cleanup")
	}
}
