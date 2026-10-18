package observability

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	goruntime "runtime"
	"strings"
	"sync"
	"testing"
	"time"

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
	if strings.Count(text, "metrics export failed") != 1 || strings.Contains(text, "password") || strings.Contains(text, "secret-token") || strings.Contains(text, "Authorization") {
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
