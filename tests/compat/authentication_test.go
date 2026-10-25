//go:build integration && qualification

package compat

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestSaslTlsExternalAuthentication(t *testing.T) {
	for _, mechanism := range []string{"OAUTHBEARER", "GSSAPI"} {
		t.Run(mechanism, func(t *testing.T) {
			// Given a record and a restricted OAuth or Kerberos principal in the disposable Kafka cluster.
			_, producer, src, dst, group := secureFixture(t)
			stamp := time.Now().Add(-time.Minute).Truncate(time.Millisecond)
			publish(t, producer, &kgo.Record{Topic: src, Value: []byte(mechanism), Timestamp: stamp})
			cfg := secureProperties(t, "copier", "fixture-copy-secret")
			delete(cfg, "sasl.username")
			delete(cfg, "sasl.password")
			cfg["sasl.mechanism"] = mechanism
			if mechanism == "GSSAPI" {
				require.NotEmpty(t, os.Getenv("KAFERRY_QUAL_KEYTAB"))
				cfg["sasl.kerberos.principal"] = "copier@KAFERRY.TEST"
				cfg["sasl.kerberos.keytab"] = os.Getenv("KAFERRY_QUAL_KEYTAB")
				cfg["sasl.kerberos.config"] = os.Getenv("KAFERRY_QUAL_KRB_CONFIG")
			} else {
				issuer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					user, password, ok := r.BasicAuth()
					if !ok || user != "copier-client" || password != "private-oauth-secret" {
						w.WriteHeader(http.StatusUnauthorized)
						return
					}
					// Kafka's disposable fixture validates the principal and expiration of this unsigned JWT.
					claims, _ := json.Marshal(map[string]any{"sub": "copier", "iat": time.Now().Unix(), "exp": time.Now().Add(time.Hour).Unix()})
					token := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none"}`)) + "." + base64.RawURLEncoding.EncodeToString(claims) + "."
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(map[string]any{"access_token": token, "token_type": "Bearer", "expires_in": 3600})
				}))
				defer issuer.Close()
				ca := filepath.Join(t.TempDir(), "issuer.pem")
				require.NoError(t, os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: issuer.Certificate().Raw}), 0600))
				cfg["sasl.oauthbearer.token.endpoint.url"], cfg["sasl.oauthbearer.ssl.ca.location"] = issuer.URL, ca
				cfg["sasl.oauthbearer.client.id"], cfg["sasl.oauthbearer.client.secret"] = "copier-client", "private-oauth-secret"
			}
			statement := fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' WITH (GROUP='%s') OPTIONS(WORKER_STOP_AFTER_RECORDS=1)", dst, src, group)
			// When separate CLI invocations copy and resume with that authentication method.
			for round := range 2 {
				if round == 1 {
					publish(t, producer, &kgo.Record{Topic: src, Value: []byte("resumed"), Timestamp: stamp})
				}
				ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
				result := binaryWorker(t, ctx, statement, secureConfig(t, cfg))
				cancel()
				// Then authentication succeeds, credentials stay private, and each run adds exactly one record.
				require.Equal(t, 0, result.code, "%s", result.detail)
				require.NotContains(t, result.detail, "private-oauth-secret")
			}
			reader := secureReader(t, dst)
			records := []*kgo.Record{read(t, reader), read(t, reader)}
			require.Equal(t, mechanism, string(records[0].Value))
			require.Equal(t, "resumed", string(records[1].Value))
			require.Equal(t, stamp, records[0].Timestamp)
		})
	}
}
