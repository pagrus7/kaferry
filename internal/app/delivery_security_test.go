package app

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/pagrus7/kaferry/internal/sql"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

func TestRealSecuredTransactionsAndCaptureCoordinator(t *testing.T) {
	brokers, ca := os.Getenv("KAFERRY_QUAL_BROKERS"), os.Getenv("KAFERRY_QUAL_CA")
	if brokers == "" || ca == "" {
		t.Skip("requires disposable tests/qualification secured broker fixture")
	}
	replicas := int16(3)
	if text := os.Getenv("KAFERRY_LAB_REPLICAS"); text != "" {
		n, err := strconv.Atoi(text)
		require.NoError(t, err)
		require.Positive(t, n)
		require.LessOrEqual(t, n, 3)
		replicas = int16(n)
	}
	roots := x509.NewCertPool()
	certificate, err := os.ReadFile(ca)
	require.NoError(t, err)
	require.True(t, roots.AppendCertsFromPEM(certificate))
	adminClient := client(t, strings.Split(brokers, ","), kgo.DialTLSConfig(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}), kgo.SASL(plain.Auth{User: "admin", Pass: "fixture-admin-secret"}.AsMechanism()))
	admin := kadm.NewClient(adminClient)
	groupPrefix := fmt.Sprintf("lab-security-%d", time.Now().UnixNano())
	for _, mechanism := range []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER", "GSSAPI"} {
		t.Run(mechanism, func(t *testing.T) {
			// Given restricted principals, replicated topics and a separate progress-topic permission.
			t.Log("preparing topics")
			created, err := admin.CreateTopics(testContext(t), 1, replicas, map[string]*string{"min.insync.replicas": kadm.StringPtr(fmt.Sprint(min(replicas, 2)))}, "input", "output")
			require.NoError(t, err)
			require.NoError(t, created.Error())
			created, err = admin.CreateTopics(testContext(t), 1, replicas, map[string]*string{"cleanup.policy": kadm.StringPtr("compact"), "min.insync.replicas": kadm.StringPtr(fmt.Sprint(min(replicas, 2)))}, "progress")
			require.NoError(t, err)
			require.NoError(t, created.Error())
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_, _ = admin.DeleteTopics(ctx, "input", "output", "progress")
			})
			t.Log("preparing principal permissions")
			for _, acl := range []*kadm.ACLBuilder{
				kadm.NewACLs().Topics("input", "output", "progress").Operations(kadm.OpRead, kadm.OpWrite, kadm.OpDescribeConfigs, kadm.OpAlter),
				kadm.NewACLs().Groups("lab-security-").ResourcePatternType(kadm.ACLPatternPrefixed).Operations(kadm.OpRead),
				kadm.NewACLs().TransactionalIDs("kaferry-", "kaferry-publication-").ResourcePatternType(kadm.ACLPatternPrefixed).Operations(kadm.OpWrite),
			} {
				results, err := admin.CreateACLs(testContext(t), acl.Allow("User:copier").AllowHosts("*"))
				require.NoError(t, err)
				for _, result := range results {
					require.NoError(t, result.Err)
				}
			}
			t.Log("preparing SCRAM credentials")
			for _, method := range []kadm.ScramMechanism{kadm.ScramSha256, kadm.ScramSha512} {
				results, err := admin.AlterUserSCRAMs(testContext(t), nil, []kadm.UpsertSCRAM{{User: "copier", Mechanism: method, Iterations: 4096, Password: "fixture-copy-secret"}})
				require.NoError(t, err)
				require.NoError(t, results.Error())
			}
			cfg := map[string]string{"bootstrap.servers": brokers, "security.protocol": "SASL_SSL", "sasl.mechanism": mechanism, "sasl.username": "copier", "sasl.password": "fixture-copy-secret", "ssl.ca.location": ca}
			if mechanism == "GSSAPI" || mechanism == "OAUTHBEARER" {
				delete(cfg, "sasl.username")
				delete(cfg, "sasl.password")
			}
			if mechanism == "GSSAPI" {
				cfg["sasl.kerberos.principal"] = "copier@KAFERRY.TEST"
				cfg["sasl.kerberos.keytab"] = os.Getenv("KAFERRY_QUAL_KEYTAB")
				cfg["sasl.kerberos.config"] = os.Getenv("KAFERRY_QUAL_KRB_CONFIG")
			}
			if mechanism == "OAUTHBEARER" {
				issuer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					user, password, ok := r.BasicAuth()
					if !ok || user != "client" || password != "private-secret" {
						w.WriteHeader(http.StatusUnauthorized)
						return
					}
					w.Header().Set("Content-Type", "application/json")
					claims, _ := json.Marshal(map[string]any{"sub": "copier", "iat": time.Now().Unix(), "exp": time.Now().Add(time.Hour).Unix()})
					token := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none"}`)) + "." + base64.RawURLEncoding.EncodeToString(claims) + "."
					_ = json.NewEncoder(w).Encode(map[string]any{"access_token": token, "token_type": "Bearer", "expires_in": 3600})
				}))
				defer issuer.Close()
				path := filepath.Join(t.TempDir(), "issuer.pem")
				require.NoError(t, os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: issuer.Certificate().Raw}), 0600))
				cfg["sasl.oauthbearer.token.endpoint.url"] = issuer.URL
				cfg["sasl.oauthbearer.ssl.ca.location"] = path
				cfg["sasl.oauthbearer.client.id"] = "client"
				cfg["sasl.oauthbearer.client.secret"] = "private-secret"
			}
			t.Log("seeding input")
			seedClient := client(t, strings.Split(brokers, ","), kgo.DialTLSConfig(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}), kgo.SASL(plain.Auth{User: "admin", Pass: "fixture-admin-secret"}.AsMechanism()))
			for offset := range 3 {
				require.NoError(t, seedClient.ProduceSync(testContext(t), recordFor("input", 0, fmt.Sprintf("secure:%d", offset))).FirstErr())
			}
			profiles := plan.Profiles{"src": cfg, "dst": cfg}
			for _, direction := range []string{"TOPIC", "ARCHIVE"} {
				destination := "TOPIC 'output@dst'"
				root := ""
				if direction == "ARCHIVE" {
					root = t.TempDir()
					destination = "ARCHIVE '" + root + "' WITH(ROTATE_AFTER_SIZE=1000)"
				}
				ast, err := sql.NewParser().Parse(fmt.Sprintf("INSERT INTO %s SELECT * FROM TOPIC 'input@src' WITH(GROUP='%s-%s-%s') WHERE RECORD_OFFSET < LATEST", destination, groupPrefix, mechanism, direction))
				require.NoError(t, err)
				p, err := plan.NewPlanner(environment{}).Build(ast, profiles)
				require.NoError(t, err)
				options := Options{ProgressTopic: "progress"}
				// When transactional output or isolated-disk coordination copies and resumes with each mechanism.
				t.Logf("copying to %s", direction)
				require.NoError(t, Run(testContext(t), p, config.Defaults(""), options))
				t.Logf("resuming %s", direction)
				require.NoError(t, Run(testContext(t), p, config.Defaults(""), options))
				t.Logf("inspecting %s", direction)
				status, err := (Inspection{Options: options}).Status(testContext(t), p, config.Defaults("").Worker, core.NoopStopObserver{})
				require.NoError(t, err)
				require.Equal(t, "stopped", status.Verdict)
				// Then the archive contains each occurrence once; broker output is checked below.
				if direction == "ARCHIVE" {
					require.Equal(t, map[string]int{"secure:0": 1, "secure:1": 1, "secure:2": 1}, archiveValues(t, root))
				}
			}
			t.Log("checking committed output")
			got := recordValues(committedRecords(t, strings.Split(brokers, ","), 1, kgo.DialTLSConfig(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}), kgo.SASL(plain.Auth{User: "admin", Pass: "fixture-admin-secret"}.AsMechanism())))
			require.Equal(t, map[string]int{"secure:0": 1, "secure:1": 1, "secure:2": 1}, got)
		})
	}
}
