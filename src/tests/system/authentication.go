//go:build integration && qualification

package compat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func RunSaslTlsExternalAuthentication(t *testing.T) {
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
				key := fixtureOAuthKey(t, 1)
				oauthTokenEndpoint(t, cfg, 3600, func() (string, error) {
					return signedFixtureToken(key, 1, "https://kaferry.test", "kaferry-qualification", time.Now().Add(time.Hour))
				})
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
