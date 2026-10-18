package observability

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/copy"
	"github.com/stretchr/testify/require"
	collector "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/protobuf/proto"
)

type systemEnvironment struct{}

func (systemEnvironment) LookupEnv(k string) (string, bool) { return os.LookupEnv(k) }
func (systemEnvironment) ReadFile(k string) ([]byte, error) { return os.ReadFile(k) }
func metricEnv(t *testing.T, endpoint string) {
	t.Helper()
	for _, key := range []string{"OTEL_EXPORTER_OTLP_PROTOCOL", "OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_HEADERS", "OTEL_EXPORTER_OTLP_CERTIFICATE", "OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE", "OTEL_EXPORTER_OTLP_CLIENT_KEY", "OTEL_EXPORTER_OTLP_INSECURE", "OTEL_EXPORTER_OTLP_TIMEOUT", "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "OTEL_EXPORTER_OTLP_METRICS_HEADERS", "OTEL_EXPORTER_OTLP_METRICS_CERTIFICATE", "OTEL_EXPORTER_OTLP_METRICS_CLIENT_CERTIFICATE", "OTEL_EXPORTER_OTLP_METRICS_CLIENT_KEY", "OTEL_EXPORTER_OTLP_METRICS_INSECURE", "OTEL_EXPORTER_OTLP_METRICS_TIMEOUT"} {
		t.Setenv(key, "")
	}
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", endpoint)
	t.Setenv("OTEL_METRIC_EXPORT_INTERVAL", "60000")
	t.Setenv("OTEL_METRIC_EXPORT_TIMEOUT", "1000")
}
func TestOTLPExportPreservesMetricAndResourceContract(t *testing.T) {
	seen := map[string]bool{}
	for _, specific := range []bool{false, true} {
		t.Run(map[bool]string{false: "base endpoint", true: "full metrics endpoint"}[specific], func(t *testing.T) {
			// Given an OTLP receiver and credentials supplied outside the SQL.
			requests := make(chan *collector.ExportMetricsServiceRequest, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/prefix/v1/metrics" || r.Header.Get("Authorization") != "Bearer token" {
					t.Error(r.URL.Path, "incorrect endpoint or credentials")
				}
				b, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
					return
				}
				var request collector.ExportMetricsServiceRequest
				if err := proto.Unmarshal(b, &request); err != nil {
					t.Error(err)
					return
				}
				requests <- &request
				w.Header().Set("Content-Type", "application/x-protobuf")
			}))
			defer server.Close()
			metricEnv(t, server.URL+"/prefix/v1/metrics")
			if !specific {
				t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "")
				t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", server.URL+"/prefix")
			}
			t.Setenv("OTEL_EXPORTER_OTLP_METRICS_HEADERS", "Authorization=Bearer%20token")
			t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "false")
			t.Setenv("OTEL_EXPORTER_OTLP_METRICS_INSECURE", "true")
			t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment.name=test,service.instance.id=not-unique")
			f := NewFactory(systemEnvironment{}, slog.New(slog.NewJSONHandler(io.Discard, nil)), "test-version")
			session, err := f.Open("test-group")
			require.NoError(t, err)
			session.Publish(copy.Snapshot{Submitted: 2, Confirmed: 1, Partitions: 1})
			// When a short run performs its final best-effort export.
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			session.Close(ctx)
			// Then the receiver sees typed metrics and a generated worker identity, without record contents.
			select {
			case request := <-requests:
				if len(request.ResourceMetrics) != 1 {
					t.Fatal(request)
				}
				attrs := map[string]string{}
				for _, a := range request.ResourceMetrics[0].Resource.Attributes {
					attrs[a.Key] = a.Value.GetStringValue()
				}
				if attrs["service.name"] != "kaferry" || attrs["service.version"] != "test-version" || attrs["messaging.consumer.group.name"] != "test-group" || len(attrs["service.instance.id"]) != 32 || attrs["deployment.environment.name"] != "test" {
					t.Fatal(attrs)
				}
				id := attrs["service.instance.id"]
				if seen[id] {
					t.Fatal("sessions for the same group reused an instance identity")
				}
				seen[id] = true
				found := false
				for _, s := range request.ResourceMetrics[0].ScopeMetrics {
					for _, m := range s.Metrics {
						if m.Name == "kaferry.records.confirmed" {
							found = true
							if m.GetSum().DataPoints[0].GetAsInt() != 1 {
								t.Fatal(m)
							}
						}
					}
				}
				if !found {
					t.Fatal("missing confirmed counter")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("no final export")
			}
		})
	}
}

func TestExporterRejectsHTTPSDowngradeBeforeSendingCredentials(t *testing.T) {
	for _, specific := range []string{"", " \t"} {
		t.Run(fmt.Sprintf("specific=%q", specific), func(t *testing.T) {
			// Given an HTTPS endpoint, credentials and an inherited plaintext transport flag.
			var requests atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "application/x-protobuf")
			}))
			defer server.Close()
			metricEnv(t, strings.Replace(server.URL, "http:", "https:", 1)+"/v1/metrics")
			t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "true")
			t.Setenv("OTEL_EXPORTER_OTLP_METRICS_INSECURE", specific)
			t.Setenv("OTEL_EXPORTER_OTLP_METRICS_HEADERS", "Authorization=Bearer%20fixture-token")
			// When opening the exporter, then it rejects the flag without making an HTTP request.
			session, err := NewFactory(systemEnvironment{}, slog.New(slog.NewJSONHandler(io.Discard, nil)), "test").Open("group")
			if session != nil {
				session.Publish(copy.Snapshot{Confirmed: 1})
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				session.Close(ctx)
			}
			if err == nil || err.Error() != "metrics: invalid or missing OTEL_EXPORTER_OTLP_INSECURE" || requests.Load() != 0 {
				t.Fatal("HTTPS downgrade was not rejected before export", err, requests.Load())
			}
		})
	}
}

func TestFactoryRejectsInvalidSDKConfiguration(t *testing.T) {
	cert, _ := certPair(t)
	dir := t.TempDir()
	certPath := filepath.Join(dir, "fixture-secret-certificate.pem")
	require.NoError(t, os.WriteFile(certPath, cert, 0600))
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()
	for _, tc := range []struct {
		name string
		env  map[string]string
		want string
	}{
		{"malformed resource", map[string]string{"OTEL_RESOURCE_ATTRIBUTES": "fixture-secret-attribute"}, "metrics: invalid OTEL_RESOURCE_ATTRIBUTES"},
		{"TLS configuration with HTTP", map[string]string{"OTEL_EXPORTER_OTLP_METRICS_CERTIFICATE": certPath}, "metrics: cannot configure HTTP exporter"},
		{"unreadable client key", map[string]string{
			"OTEL_EXPORTER_OTLP_METRICS_CLIENT_CERTIFICATE": certPath,
			"OTEL_EXPORTER_OTLP_METRICS_CLIENT_KEY":         filepath.Join(dir, "fixture-secret-missing-key.pem"),
		}, "metrics: invalid or missing OTEL_EXPORTER_OTLP_METRICS_CLIENT_KEY"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given a realistic SDK configuration error, including sensitive input or paths.
			metricEnv(t, server.URL+"/v1/metrics")
			t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "")
			for name, value := range tc.env {
				t.Setenv(name, value)
			}
			// When the real factory opens the exporter.
			session, err := NewFactory(systemEnvironment{}, slog.New(slog.NewJSONHandler(io.Discard, nil)), "test").Open("group")
			if session != nil {
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				session.Close(ctx)
			}
			// Then setup fails without a session or input values in the diagnostic.
			if session != nil || err == nil || err.Error() != tc.want {
				t.Fatal("incorrect configuration failure", err)
			}
		})
	}
}

func TestReceiverFailuresDoNotDelayShutdownOrRetry(t *testing.T) {
	for _, hang := range []bool{false, true} {
		t.Run(map[bool]string{false: "503", true: "no response"}[hang], func(t *testing.T) {
			// Given an unavailable receiver and a short cleanup deadline.
			var requests atomic.Int64
			release := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if hang {
					<-release
				}
				w.WriteHeader(503)
			}))
			defer server.Close()
			defer close(release)
			metricEnv(t, server.URL+"/v1/metrics")
			// Keep the 503 export alive longer than the SDK's first retry backoff.
			// A fast shutdown must come from disabled retries rather than a timeout.
			t.Setenv("OTEL_METRIC_EXPORT_TIMEOUT", "10000")
			t.Setenv("OTEL_EXPORTER_OTLP_METRICS_TIMEOUT", "10000")
			session, err := NewFactory(systemEnvironment{}, slog.New(slog.NewJSONHandler(io.Discard, nil)), "test").Open("group")
			require.NoError(t, err)
			session.Publish(copy.Snapshot{Confirmed: 3})
			budget := 10 * time.Second
			if hang {
				budget = 100 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(t.Context(), budget)
			defer cancel()
			// When the exporter closes, then a 503 returns promptly and a stalled request respects its context.
			done := make(chan struct{})
			go func() { session.Close(ctx); close(done) }()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("shutdown blocked")
			}
			require.Equal(t, int64(1), requests.Load(), "one best-effort export")
			if !hang {
				require.NoError(t, ctx.Err(), "503 shutdown must finish without waiting for a deadline")
			}
		})
	}
}
