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
	"sync"
	"testing"
	"time"
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
