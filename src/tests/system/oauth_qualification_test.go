//go:build integration && qualification

package compat

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func fixtureOAuthKey(t *testing.T, identity int) *rsa.PrivateKey {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(os.Getenv("KAFERRY_QUAL_CERT_DIR"), fmt.Sprintf("oauth-%d.key", identity)))
	require.NoError(t, err)
	block, _ := pem.Decode(data)
	require.NotNil(t, block)
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	require.NoError(t, err)
	return key.(*rsa.PrivateKey)
}

func signedFixtureToken(key *rsa.PrivateKey, identity int, issuer, audience string, expiry time.Time) (string, error) {
	header, _ := json.Marshal(map[string]any{"alg": "RS256", "kid": fmt.Sprintf("qualification-%d", identity)})
	claims, _ := json.Marshal(map[string]any{"sub": "copier", "iss": issuer, "aud": audience, "iat": time.Now().Add(-time.Minute).Unix(), "exp": expiry.Unix()})
	message := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(claims)
	digest := sha256.Sum256([]byte(message))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	return message + "." + base64.RawURLEncoding.EncodeToString(signature), err
}

func oauthProperties(t *testing.T) map[string]string {
	t.Helper()
	cfg := secureProperties(t, "copier", "fixture-copy-secret")
	delete(cfg, "sasl.username")
	delete(cfg, "sasl.password")
	cfg["sasl.mechanism"] = "OAUTHBEARER"
	return cfg
}

func TestSaslTlsOAuthTokenFile(t *testing.T) {
	// Given a broker that validates signatures, issuer, audience and expiration.
	_, producer, src, dst, group := secureFixture(t)
	key := fixtureOAuthKey(t, 1)
	cfg := oauthProperties(t)
	path := filepath.Join(t.TempDir(), "token")
	cfg["sasl.oauthbearer.token.file"] = path
	statement := fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' WITH (GROUP='%s') OPTIONS(WORKER_STOP_AFTER_RECORDS=1)", dst, src, group)
	for round := range 2 {
		token, err := signedFixtureToken(key, 1, "https://kaferry.test", "kaferry-qualification", time.Now().Add(time.Minute))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, []byte(token), 0600))
		publish(t, producer, &kgo.Record{Topic: src, Value: []byte{byte(round)}, Timestamp: time.Now().Truncate(time.Millisecond)})
		// When a fresh invocation reads the replaced token file and resumes.
		ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
		result := binaryWorker(t, ctx, statement, secureConfig(t, cfg))
		cancel()
		// Then only the remaining record is copied and the token is redacted.
		require.Zero(t, result.code, result.detail)
		require.NotContains(t, result.detail, token)
	}
	reader := secureReader(t, dst)
	for i := range 2 {
		require.Equal(t, []byte{byte(i)}, read(t, reader).Value)
	}
	for _, failure := range []string{"issuer", "audience", "expired", "signature"} {
		t.Run(failure, func(t *testing.T) {
			issuer, audience, expiry, signing := "https://kaferry.test", "kaferry-qualification", time.Now().Add(time.Minute), key
			switch failure {
			case "issuer":
				issuer = "https://wrong.test"
			case "audience":
				audience = "wrong"
			case "expired":
				expiry = time.Now().Add(-time.Minute)
			case "signature":
				signing = fixtureOAuthKey(t, 2)
			}
			token, err := signedFixtureToken(signing, 1, issuer, audience, expiry)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(path, []byte(token), 0600))
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			result := binaryWorker(t, ctx, statement, secureConfig(t, cfg))
			require.Equal(t, 1, result.code, result.detail)
			require.NotContains(t, result.detail, token)
		})
	}
}

func TestSaslTlsOAuthRenewalAndKeyRotation(t *testing.T) {
	// Given a live copy, short-lived signed tokens and a broker's HTTP JWKS cache.
	a, producer, src, dst, group := secureFixture(t)
	keys := [2]*rsa.PrivateKey{fixtureOAuthKey(t, 1), fixtureOAuthKey(t, 2)}
	var rotated atomic.Bool
	var renewed atomic.Int64
	issuer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		if !ok || user != "copier-client" || password != "private-oauth-secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		identity := 1
		if rotated.Load() {
			identity = 2
			renewed.Add(1)
		}
		token, err := signedFixtureToken(keys[identity-1], identity, "https://kaferry.test", "kaferry-qualification", time.Now().Add(20*time.Second))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": token, "token_type": "Bearer", "expires_in": 20})
	}))
	defer issuer.Close()
	ca := filepath.Join(t.TempDir(), "issuer.pem")
	require.NoError(t, os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: issuer.Certificate().Raw}), 0600))
	cfg := oauthProperties(t)
	cfg["sasl.oauthbearer.token.endpoint.url"], cfg["sasl.oauthbearer.ssl.ca.location"] = issuer.URL, ca
	cfg["sasl.oauthbearer.client.id"], cfg["sasl.oauthbearer.client.secret"] = "copier-client", "private-oauth-secret"
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	publish(t, producer, &kgo.Record{Topic: src, Value: []byte("before"), Timestamp: time.Now().Truncate(time.Millisecond)})
	statement := fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' WITH (GROUP='%s') OPTIONS(WORKER_STOP_AFTER_RECORDS=2)", dst, src, group)
	process := startCLI(t, ctx, testBinary(t), "", "run", "-e", statement, "-c", secureConfig(t, cfg))
	waitEnd(t, a, dst, 1)
	// When the issuer rotates keys while the same process reauthenticates.
	root := os.Getenv("KAFERRY_QUAL_CERT_DIR")
	jwks, err := os.ReadFile(filepath.Join(root, "jwks-rotated.json"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "jwks", "keys.json"), jwks, 0644))
	rotated.Store(true)
	require.Eventually(t, func() bool { return renewed.Load() > 0 }, 45*time.Second, 100*time.Millisecond)
	publish(t, producer, &kgo.Record{Topic: src, Value: []byte("after"), Timestamp: time.Now().Truncate(time.Millisecond)})
	// Then broker-side signature validation accepts the new key and copying finishes.
	require.NoError(t, process.wait(), process.logs())
	require.NotContains(t, process.logs(), "private-oauth-secret")
	reader := secureReader(t, dst)
	require.Equal(t, "before", string(read(t, reader).Value))
	require.Equal(t, "after", string(read(t, reader).Value))
}

func TestMutualTlsCopy(t *testing.T) { testTLSCopy(t, true) }
func TestServerTlsCopy(t *testing.T) { testTLSCopy(t, false) }

func testTLSCopy(t *testing.T, mutual bool) {
	t.Helper()
	// Given a TLS listener and, for mutual TLS, a client certificate mapped to copier.
	_, producer, src, dst, group := secureFixture(t)
	port := 94
	if mutual {
		port = 93
	}
	cfg := map[string]string{"bootstrap.servers": fmt.Sprintf("127.0.0.1:410%d,127.0.0.1:420%d,127.0.0.1:430%d", port, port, port),
		"security.protocol": "SSL", "ssl.ca.location": os.Getenv("KAFERRY_QUAL_CA")}
	if mutual {
		cfg["ssl.certificate.location"] = filepath.Join(os.Getenv("KAFERRY_QUAL_CERT_DIR"), "client.pem")
		cfg["ssl.key.location"] = filepath.Join(os.Getenv("KAFERRY_QUAL_CERT_DIR"), "client.key")
	}
	publish(t, producer, &kgo.Record{Topic: src, Value: []byte("tls"), Timestamp: time.Now().Truncate(time.Millisecond)})
	statement := fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' WITH (GROUP='%s') OPTIONS(WORKER_STOP_AFTER_RECORDS=1)", dst, src, group)
	// When copying through the CLI, then TLS authentication preserves the record.
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	defer cancel()
	result := binaryWorker(t, ctx, statement, secureConfig(t, cfg))
	require.Zero(t, result.code, result.detail)
	require.Equal(t, "tls", string(read(t, secureReader(t, dst)).Value))
	if mutual {
		delete(cfg, "ssl.certificate.location")
		delete(cfg, "ssl.key.location")
		result = binaryWorker(t, ctx, statement, secureConfig(t, cfg))
		require.Equal(t, 1, result.code, "missing client certificate must fail: %s", result.detail)
	}
}
