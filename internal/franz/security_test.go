package franz

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
)

func testCA(t *testing.T) (*x509.Certificate, ed25519.PrivateKey, []byte) {
	t.Helper()
	public, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "fixture CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, public, key)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return parsed, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}
func testCertificate(t *testing.T, ca *x509.Certificate, issuer ed25519.PrivateKey, client bool) (tls.Certificate, []byte, []byte) {
	t.Helper()
	public, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	usage := x509.ExtKeyUsageServerAuth
	if client {
		usage = x509.ExtKeyUsageClientAuth
	}
	certificate := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "fixture"}, DNSNames: []string{"localhost"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage}}
	der, err := x509.CreateCertificate(rand.Reader, certificate, ca, public, issuer)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM, keyPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encoded})
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return pair, certPEM, keyPEM
}
func writeFixture(t *testing.T, directory, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
func tlsFixture(t *testing.T, certificate tls.Certificate, roots *x509.CertPool) string {
	t.Helper()
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{certificate}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS12})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var workers sync.WaitGroup
	go func() {
		defer close(done)
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			workers.Add(1)
			go func(connection net.Conn) {
				defer workers.Done()
				defer connection.Close()
				_ = connection.SetDeadline(time.Now().Add(2 * time.Second))
				if connection.(*tls.Conn).Handshake() == nil {
					_, _ = connection.Write([]byte{1})
				}
			}(connection)
		}
	}()
	t.Cleanup(func() { listener.Close(); <-done; workers.Wait() })
	return listener.Addr().String()
}
func TestTLSHostnameOverrideKeepsTrustAndClientCertificateChecks(t *testing.T) {
	// Given a local mTLS server and separate trusted/untrusted CAs.
	ca, key, caPEM := testCA(t)
	_, _, wrongCA := testCA(t)
	server, _, _ := testCertificate(t, ca, key, false)
	_, clientPEM, clientKey := testCertificate(t, ca, key, true)
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	address := tlsFixture(t, server, roots)
	directory := t.TempDir()
	caPath := writeFixture(t, directory, "ca.pem", caPEM)
	wrongPath := writeFixture(t, directory, "wrong-ca.pem", wrongCA)
	certPath := writeFixture(t, directory, "client.pem", clientPEM)
	keyPath := writeFixture(t, directory, "client.key", clientKey)
	for _, tc := range []struct {
		name, host, algorithm   string
		wrongCA, noClient, want bool
	}{
		{"valid mTLS", "localhost", "https", false, false, true},
		{"wrong hostname rejected", "wrong.invalid", "https", false, false, false},
		{"only hostname disabled", "wrong.invalid", "none", false, false, true},
		{"untrusted CA remains rejected", "wrong.invalid", "none", true, false, false},
		{"missing client certificate rejected", "localhost", "none", false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			settings := settings{ca: caPath, certificate: certPath, key: keyPath, hostname: tc.algorithm}
			if tc.wrongCA {
				settings.ca = wrongPath
			}
			if tc.noClient {
				settings.certificate, settings.key = "", ""
			}
			cfg, err := settings.tlsConfig()
			if err != nil {
				t.Fatal(err)
			}
			cfg.ServerName = tc.host
			// When performing a real handshake and authenticated application read.
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			connection, err := (&tls.Dialer{Config: cfg}).DialContext(ctx, "tcp", address)
			if err == nil {
				defer connection.Close()
				_ = connection.SetReadDeadline(time.Now().Add(time.Second))
				var b [1]byte
				_, err = connection.Read(b[:])
			}
			// Then disabling hostname matching never disables CA or client-certificate verification.
			if (err == nil) != tc.want {
				t.Fatal("incorrect TLS protection", err)
			}
		})
	}
}

func TestConnectionOptionsApplyTLSAndSelectedSASLMechanism(t *testing.T) {
	ca, _, caPEM := testCA(t)
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	caPath := writeFixture(t, t.TempDir(), "ca.pem", caPEM)
	for _, tc := range []struct{ protocol, mechanism string }{
		{"PLAINTEXT", ""}, {"SSL", ""}, {"SASL_SSL", "PLAIN"},
		{"SASL_SSL", "SCRAM-SHA-256"}, {"SASL_SSL", "SCRAM-SHA-512"},
		{"SASL_PLAINTEXT", "SCRAM-SHA-256"},
	} {
		t.Run(tc.protocol+"/"+tc.mechanism, func(t *testing.T) {
			// Given valid connection properties for the selected transport and authentication.
			properties := map[string]string{"security.protocol": tc.protocol}
			if strings.HasSuffix(tc.protocol, "SSL") {
				properties["ssl.ca.location"] = caPath
			}
			if tc.mechanism != "" {
				properties["sasl.mechanism"], properties["sasl.username"], properties["sasl.password"] = tc.mechanism, "user", "password"
			}
			s, err := parseSettings(testPlan(t, properties).Source.Properties)
			if err != nil {
				t.Fatal(err)
			}
			options, err := s.commonOptions()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			// When applying the production options to the real client without connecting.
			client, err := kgo.NewClient(append(options, kgo.WithContext(ctx))...)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			// Then TLS trust and the chosen authentication mechanism reach the client.
			tlsConfig := client.OptValue(kgo.DialTLSConfig).(*tls.Config)
			if strings.HasSuffix(tc.protocol, "SSL") {
				if tlsConfig == nil || tlsConfig.MinVersion != tls.VersionTLS12 || tlsConfig.RootCAs == nil || !tlsConfig.RootCAs.Equal(roots) || tlsConfig.InsecureSkipVerify {
					t.Fatal("TLS trust settings were not applied")
				}
			} else if tlsConfig != nil {
				t.Fatal("unexpected TLS transport")
			}
			mechanisms := client.OptValue(kgo.SASL).([]sasl.Mechanism)
			if tc.mechanism == "" {
				if len(mechanisms) != 0 {
					t.Fatal("unexpected SASL authentication")
				}
			} else if len(mechanisms) != 1 || mechanisms[0].Name() != tc.mechanism {
				t.Fatal("wrong SASL mechanism applied")
			} else {
				_, first, err := mechanisms[0].Authenticate(t.Context(), "host")
				if err != nil || (tc.mechanism == "PLAIN" && string(first) != "\x00user\x00password") ||
					(tc.mechanism != "PLAIN" && !strings.Contains(string(first), "n=user,")) {
					t.Fatal("SASL credentials were not applied correctly")
				}
			}
		})
	}
}

func TestTLSFileErrorsFailValidationWithoutExposingPathsOrContents(t *testing.T) {
	directory := t.TempDir()
	ca, issuer, _ := testCA(t)
	_, certPEM, _ := testCertificate(t, ca, issuer, true)
	invalid := writeFixture(t, directory, "sensitive-fixture.pem", []byte("sensitive-fixture-contents"))
	certificate := writeFixture(t, directory, "client.pem", certPEM)
	for _, tc := range []struct {
		name, key  string
		properties map[string]string
	}{
		{"missing CA file", "ssl.ca.location", map[string]string{"ssl.ca.location": filepath.Join(directory, "sensitive-fixture-missing.pem")}},
		{"invalid CA file", "ssl.ca.location", map[string]string{"ssl.ca.location": invalid}},
		{"invalid client key", "ssl.certificate.location/ssl.key.location", map[string]string{"ssl.certificate.location": certificate, "ssl.key.location": invalid}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given unreadable or malformed TLS files named by the user's connection.
			tc.properties["security.protocol"] = "SSL"
			p := testPlan(t, tc.properties)
			// When validating before any Kafka or output handles are opened.
			err := componentFactory().Validate(p, config.Defaults("").Worker)
			// Then the error identifies the setting without exposing its path or contents.
			if err == nil || !strings.Contains(err.Error(), tc.key) || strings.Contains(err.Error(), directory) || strings.Contains(err.Error(), "sensitive-fixture") {
				t.Fatal("unsafe or missing TLS diagnostic", err)
			}
		})
	}
}
