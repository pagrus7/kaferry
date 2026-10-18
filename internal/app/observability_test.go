package app

import (
	"context"
	"errors"
	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/protobuf/proto"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/observability"
)

type metricsMock struct {
	fixture   *engineFixture
	opens     int
	group     string
	err       error
	closed    bool
	closeErr  error
	allowance time.Duration
}

func (m *metricsMock) Open(group string) (observability.Session, error) {
	m.opens++
	m.group = group
	if m.err != nil {
		return nil, m.err
	}
	return m, nil
}
func (*metricsMock) Publish(core.Snapshot) {}
func (*metricsMock) Saved(core.SaveResult) {}
func (m *metricsMock) Close(ctx context.Context) {
	m.closed = true
	m.closeErr = ctx.Err()
	if at, ok := ctx.Deadline(); ok {
		m.allowance = time.Until(at)
	}
	_ = m.fixture.stage("close-metrics")
}

func TestEngineOwnsMetricsOnlyDuringEnabledExecution(t *testing.T) {
	for _, mode := range []string{"disabled", "enabled", "invalid", "cancelled during run"} {
		t.Run(mode, func(t *testing.T) {
			// Given independent copy and observability factories.
			f := newEngineFixture()
			f.store = f.input
			m := &metricsMock{fixture: f}
			cfg := config.Defaults("")
			cfg.Metrics.Enabled = mode != "disabled"
			if mode == "invalid" {
				m.err = errors.New("invalid collector settings")
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if mode == "cancelled during run" {
				f.onStage = func(stage string) {
					if stage == "run" {
						cancel()
					}
				}
			}
			// When the engine executes and closes its resources.
			err := newEngine(f, f, f, engineLogger(), m).Run(ctx, enginePlan("TOPIC", "TOPIC"), cfg, noopShutdown{})
			// Then disabled metrics acquire nothing, invalid settings prevent endpoint I/O,
			// and enabled cleanup uses its own bounded context after copy resources close.
			if mode == "disabled" {
				if m.opens != 0 || m.closed || err != nil {
					t.Fatal(m, err)
				}
				return
			}
			if mode == "invalid" {
				var exit *core.Exit
				if !errors.As(err, &exit) || exit.Code != 2 || len(f.trace) != 0 || m.closed {
					t.Fatal(m, err, f.trace)
				}
				return
			}
			if err != nil || m.opens != 1 || m.group != "group" || !m.closed || m.closeErr != nil || m.allowance <= 0 || m.allowance > time.Second || f.trace[len(f.trace)-1] != "close-metrics" {
				t.Fatal(m, err, f.trace)
			}
		})
	}
}

func TestMetricsCleanupUsesRemainingProcessBudget(t *testing.T) {
	// Given a process whose shutdown allowance is already exhausted.
	f := newEngineFixture()
	m := &metricsMock{fixture: f}
	budget := newShutdownBudget(context.Background(), time.Millisecond, func(string) {})
	defer budget.Done()
	budget.Begin()
	// When telemetry is closed, then it receives an expired context rather than a new allowance.
	closeMetrics(m, budget)
	if m.closeErr == nil || m.allowance > 0 {
		t.Fatal(m)
	}
}

func TestExplainIgnoresMetricTransportAndRunValidatesBeforeOutput(t *testing.T) {
	for _, command := range []string{"run", "explain"} {
		t.Run(command, func(t *testing.T) {
			// Given enabled export with an invalid endpoint and a valid file query.
			fixture := filePreview(t)
			f, err := os.OpenFile(fixture.config, os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = f.WriteString("\n[metrics]\nenabled=true\n"); err != nil {
				t.Fatal(err)
			}
			_ = f.Close()
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "secret-invalid")
			t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "")
			// When the real CLI plans or runs it.
			var out strings.Builder
			code := Execute(t.Context(), []string{command, "-c", fixture.config, "-e", fixture.query}, nil, &out, &out)
			// Then explain starts no transport, while run fails before creating output.
			if command == "explain" {
				if code != 0 {
					t.Fatal(code, out.String())
				}
			} else if code != 2 || !strings.Contains(out.String(), "OTEL_EXPORTER_OTLP_ENDPOINT") || strings.Contains(out.String(), "secret-invalid") {
				t.Fatal(code, out.String())
			}
			if _, err := os.Stat(fixture.destination); !os.IsNotExist(err) {
				t.Fatal("output created", err)
			}
		})
	}
}

func TestFileCopyExportsInitialAndFinalSaves(t *testing.T) {
	// Given a real empty archive copy and an OTLP receiver.
	fixture := filePreview(t)
	received := make(chan *collectormetrics.ExportMetricsServiceRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		request := &collectormetrics.ExportMetricsServiceRequest{}
		if err := proto.Unmarshal(data, request); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		received <- request
		w.Header().Set("Content-Type", "application/x-protobuf")
	}))
	defer server.Close()
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", server.URL+"/v1/metrics")
	t.Setenv("OTEL_METRIC_EXPORT_INTERVAL", "60000")
	f, err := os.OpenFile(fixture.config, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteString("\n[metrics]\nenabled=true\n")
	_ = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	// When running through the composed CLI, sources, stores and exporter.
	code := Execute(t.Context(), []string{"run", "-c", fixture.config, "-e", fixture.query}, nil, io.Discard, io.Discard)
	// Then both initialization and final EOF saves are counted, with no copied records invented.
	if code != 0 {
		t.Fatal(code)
	}
	select {
	case request := <-received:
		var saves int64
		var eof bool
		for _, rm := range request.ResourceMetrics {
			for _, scope := range rm.ScopeMetrics {
				for _, m := range scope.Metrics {
					if m.Name == "kaferry.progress.saves" {
						for _, p := range m.GetSum().DataPoints {
							saves += p.GetAsInt()
						}
					}
					if m.Name == "kaferry.partitions.stopped" {
						for _, p := range m.GetGauge().DataPoints {
							for _, a := range p.Attributes {
								if a.Key == "reason" && a.Value.GetStringValue() == "eof" && p.GetAsInt() == 1 {
									eof = true
								}
							}
						}
					}
				}
			}
		}
		if saves != 2 || !eof {
			t.Fatal("missing startup or EOF save", saves, eof)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no metrics received")
	}
}
