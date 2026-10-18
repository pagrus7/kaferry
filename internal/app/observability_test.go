package app

import (
	"context"
	"errors"
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
	"github.com/stretchr/testify/require"
	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/protobuf/proto"
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
				require.Zero(t, m.opens)
				require.False(t, m.closed)
				require.NoError(t, err)
				return
			}
			if mode == "invalid" {
				var exit *core.Exit
				require.ErrorAs(t, err, &exit)
				require.EqualValues(t, 2, exit.Code)
				require.Len(t, f.trace, 0)
				require.False(t, m.closed)
				return
			}
			require.NoError(t, err)
			require.EqualValues(t, 1, m.opens)
			require.Equal(t, "group", m.group)
			require.True(t, m.closed)
			require.NoError(t, m.closeErr)
			require.Positive(t, m.allowance)
			require.LessOrEqual(t, m.allowance, time.Second)
			require.Equal(t, "close-metrics", f.trace[len(f.trace)-1])
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
	require.Error(t, m.closeErr)
	require.LessOrEqual(t, m.allowance, time.Duration(0))
}

func TestExplainIgnoresMetricTransportAndRunValidatesBeforeOutput(t *testing.T) {
	for _, command := range []string{"run", "explain"} {
		t.Run(command, func(t *testing.T) {
			// Given enabled export with an invalid endpoint and a valid file query.
			fixture := filePreview(t)
			f, err := os.OpenFile(fixture.config, os.O_APPEND|os.O_WRONLY, 0600)
			require.NoError(t, err)
			_, err = f.WriteString("\n[metrics]\nenabled=true\n")
			require.NoError(t, err)
			_ = f.Close()
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "secret-invalid")
			t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "")
			// When the real CLI plans or runs it.
			var out strings.Builder
			code := Execute(t.Context(), []string{command, "-c", fixture.config, "-e", fixture.query}, nil, &out, &out)
			// Then explain starts no transport, while run fails before creating output.
			if command == "explain" {
				require.Zero(t, code)
			} else {
				require.EqualValues(t, 2, code)
				require.Contains(t, out.String(), "OTEL_EXPORTER_OTLP_ENDPOINT")
				require.NotContains(t, out.String(), "secret-invalid")
			}
			{
				_, err := os.Stat(fixture.destination)
				require.True(t, os.IsNotExist(err))
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
			// The HTTP handler runs outside the test goroutine.
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
	require.NoError(t, err)
	_, err = f.WriteString("\n[metrics]\nenabled=true\n")
	_ = f.Close()
	require.NoError(t, err)
	// When running through the composed CLI, sources, stores and exporter.
	code := Execute(t.Context(), []string{"run", "-c", fixture.config, "-e", fixture.query}, nil, io.Discard, io.Discard)
	// Then both initialization and final EOF saves are counted, with no copied records invented.
	require.Zero(t, code)
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
		require.EqualValues(t, 2, saves)
		require.True(t, eof)
	case <-time.After(2 * time.Second):
		t.Fatal("no metrics received")
	}
}
