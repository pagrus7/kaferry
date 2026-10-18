package observability

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"
)

type environment struct {
	values map[string]string
	files  map[string][]byte
}

func (e environment) LookupEnv(k string) (string, bool) { v, ok := e.values[k]; return v, ok }
func (e environment) ReadFile(k string) ([]byte, error) {
	if v, ok := e.files[k]; ok {
		return v, nil
	}
	return nil, errors.New("not found")
}
func TestInvalidSettingsAreSpecificAndRedacted(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{"OTEL_EXPORTER_OTLP_ENDPOINT", ""}, {"OTEL_EXPORTER_OTLP_ENDPOINT", "bad-secret"},
		{"OTEL_EXPORTER_OTLP_ENDPOINT", "http://user:secret@localhost"}, {"OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost?secret"},
		{"OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "grpc"}, {"OTEL_EXPORTER_OTLP_METRICS_TIMEOUT", "-1"},
		{"OTEL_METRIC_EXPORT_INTERVAL", "0"}, {"OTEL_METRIC_EXPORT_TIMEOUT", "100000000000000000000"},
		{"OTEL_EXPORTER_OTLP_HEADERS", "Authorization"}, {"OTEL_EXPORTER_OTLP_HEADERS", "Authorization=%0D%0Asecret"},
		{"OTEL_EXPORTER_OTLP_HEADERS", "bad name=secret"}, {"OTEL_EXPORTER_OTLP_INSECURE", "1"},
		{"OTEL_EXPORTER_OTLP_CERTIFICATE", "missing-secret"}, {"OTEL_EXPORTER_OTLP_CLIENT_KEY", "missing-secret"},
		{"OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE", "missing-secret"},
	} {
		t.Run(tc.key+"/"+tc.value, func(t *testing.T) {
			// Given an explicit malformed setting which may contain a secret.
			env := environment{values: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4318", tc.key: tc.value}}
			// When validating, then the setting is named without echoing its value.
			err := validate(env)
			if err == nil || !strings.Contains(err.Error(), map[bool]string{true: "OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE", false: tc.key}[tc.key == "OTEL_EXPORTER_OTLP_CLIENT_KEY"]) || strings.Contains(err.Error(), "secret") {
				t.Fatal(err)
			}
		})
	}
}
func TestMetricsSettingsUseSignalSpecificPrecedence(t *testing.T) {
	// Given overridden generic settings and valid metric-specific values.
	env := environment{values: map[string]string{
		"OTEL_EXPORTER_OTLP_ENDPOINT": "bad-generic", "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": "http://localhost:4318/v1/metrics",
		"OTEL_EXPORTER_OTLP_PROTOCOL": "grpc", "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL": "http/protobuf",
		"OTEL_EXPORTER_OTLP_TIMEOUT": "bad-generic", "OTEL_EXPORTER_OTLP_METRICS_TIMEOUT": "1234",
		"OTEL_EXPORTER_OTLP_METRICS_HEADERS": "Authorization=Bearer%20token",
	}}
	// When validating, then effective metric settings take precedence.
	if err := validate(env); err != nil {
		t.Fatal(err)
	}
}

func TestInsecureSettingMustMatchEndpointScheme(t *testing.T) {
	for _, tc := range []struct{ scheme, generic, specific, invalid string }{
		{"https", "true", "", "OTEL_EXPORTER_OTLP_INSECURE"},
		{"http", "false", "", "OTEL_EXPORTER_OTLP_INSECURE"},
		{"https", "", "true", "OTEL_EXPORTER_OTLP_METRICS_INSECURE"},
		{"http", "", "false", "OTEL_EXPORTER_OTLP_METRICS_INSECURE"},
		{"https", "true", "false", ""},
		{"http", "false", "true", ""},
	} {
		t.Run(tc.scheme+"/"+tc.generic+"/"+tc.specific, func(t *testing.T) {
			// Given an endpoint and generic or metric-specific transport flags.
			env := environment{values: map[string]string{
				"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": tc.scheme + "://localhost:4318/v1/metrics",
				"OTEL_EXPORTER_OTLP_INSECURE":         tc.generic, "OTEL_EXPORTER_OTLP_METRICS_INSECURE": tc.specific,
			}}
			// When validating, then conflicting flags fail and signal-specific matching flags pass.
			err := validate(env)
			if tc.invalid == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || err.Error() != "metrics: invalid or missing "+tc.invalid {
				t.Fatal("incorrect transport diagnostic", err)
			}
		})
	}
}

func certPair(t *testing.T) ([]byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, DNSNames: []string{"localhost"}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	priv, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: priv})
}
func TestTLSSettingsValidateTrustAndKeyPair(t *testing.T) {
	cert, key := certPair(t)
	for _, broken := range []string{"", "ca", "key", "cert"} {
		t.Run(broken, func(t *testing.T) {
			// Given a complete TLS trust and client-identity configuration.
			env := environment{values: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "https://localhost:4318", "OTEL_EXPORTER_OTLP_CERTIFICATE": "ca", "OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE": "cert", "OTEL_EXPORTER_OTLP_CLIENT_KEY": "key"}, files: map[string][]byte{"ca": cert, "cert": cert, "key": key}}
			if broken != "" {
				env.files[broken] = []byte("secret invalid PEM")
			}
			// When validating, then valid credentials pass and malformed credentials fail without their contents.
			err := validate(env)
			if (broken == "") != (err == nil) || err != nil && strings.Contains(err.Error(), "secret") {
				t.Fatal(err)
			}
		})
	}
}
