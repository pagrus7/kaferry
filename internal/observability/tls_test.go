package observability

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/copy"
)

func TestExporterUsesConfiguredTrustAndClientIdentity(t *testing.T) {
	for _, clientAuth := range []bool{false, true} {
		t.Run(map[bool]string{false: "TLS", true: "mTLS"}[clientAuth], func(t *testing.T) {
			// Given a TLS receiver, requiring a client identity in the mTLS case.
			var requests atomic.Int64
			cert, key := certPair(t)
			roots := x509.NewCertPool()
			roots.AppendCertsFromPEM(cert)
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if clientAuth && (len(r.TLS.PeerCertificates) != 1 || r.TLS.PeerCertificates[0].Subject.CommonName != "test") {
					t.Error("client identity missing")
				}
				requests.Add(1)
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "application/x-protobuf")
			}))
			server.Config.ErrorLog = log.New(io.Discard, "", 0)
			if clientAuth {
				server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
			}
			server.StartTLS()
			defer server.Close()
			metricEnv(t, server.URL+"/v1/metrics")
			dir := t.TempDir()
			files := map[string][]byte{"ca": pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), "cert": cert, "key": key}
			for name, data := range files {
				if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			t.Setenv("OTEL_EXPORTER_OTLP_METRICS_CERTIFICATE", filepath.Join(dir, "ca"))
			if clientAuth {
				t.Setenv("OTEL_EXPORTER_OTLP_METRICS_CLIENT_CERTIFICATE", filepath.Join(dir, "cert"))
				t.Setenv("OTEL_EXPORTER_OTLP_METRICS_CLIENT_KEY", filepath.Join(dir, "key"))
			}
			session, err := NewFactory(systemEnvironment{}, slog.New(slog.NewJSONHandler(io.Discard, nil)), "test").Open("group")
			if err != nil {
				t.Fatal(err)
			}
			session.Publish(copy.Snapshot{Confirmed: 1})
			// When exporting through the composed SDK and environment configuration.
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			session.Close(ctx)
			// Then TLS verification and optional mutual authentication succeed.
			if requests.Load() != 1 {
				t.Fatal("TLS export did not reach the receiver")
			}
		})
	}
}
