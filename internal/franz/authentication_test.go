package franz

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/pagrus7/kaferry/internal/config"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
)

func configuredMechanism(t *testing.T, values map[string]string) sasl.Mechanism {
	t.Helper()
	values["security.protocol"] = "SASL_SSL"
	s, err := parseSettings(testPlan(t, values).Source.Properties)
	require.NoError(t, err)
	options, err := s.commonOptions()
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	client, err := kgo.NewClient(append(options, kgo.WithContext(ctx))...)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	mechanisms := client.OptValue(kgo.SASL).([]sasl.Mechanism)
	require.Len(t, mechanisms, 1)
	require.Equal(t, values["sasl.mechanism"], mechanisms[0].Name())
	return mechanisms[0]
}

func TestOAuthClientCredentialsReuseAndRenewTokens(t *testing.T) {
	// Given an HTTPS issuer and OAuth client credentials, with a first token near expiration.
	var calls atomic.Int32
	requests := make(chan string, 4)
	issuer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, _ := r.BasicAuth()
		_ = r.ParseForm()
		requests <- user + ":" + password + ":" + r.Form.Get("grant_type") + ":" + r.Form.Get("scope")
		n := calls.Add(1)
		expiry := 3600
		if n == 1 {
			expiry = 5
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"access_token":"token-%d","token_type":"Bearer","expires_in":%d}`, n, expiry)
	}))
	t.Cleanup(issuer.Close)
	ca := writeFixture(t, t.TempDir(), "issuer.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: issuer.Certificate().Raw}))
	mechanism := configuredMechanism(t, map[string]string{
		"sasl.mechanism": "OAUTHBEARER", "sasl.oauthbearer.token.endpoint.url": issuer.URL,
		"sasl.oauthbearer.client.id": "private-client", "sasl.oauthbearer.client.secret": "private-secret",
		"sasl.oauthbearer.scope": "kafka.read kafka.write", "sasl.oauthbearer.ssl.ca.location": ca,
	})
	// When new broker sessions authenticate, then renew the token near expiry and reuse its replacement.
	for _, want := range []string{"token-1", "token-2", "token-2"} {
		session, first, err := mechanism.Authenticate(t.Context(), "broker:9093")
		require.NoError(t, err)
		require.Contains(t, string(first), "auth=Bearer "+want+"\x01")
		done, _, err := session.Challenge(nil)
		require.NoError(t, err)
		require.True(t, done)
	}
	require.EqualValues(t, 2, calls.Load())
	for range 2 {
		require.Equal(t, "private-client:private-secret:client_credentials:kafka.read kafka.write", <-requests)
	}
}

func TestOAuthIssuerFailuresAndCancellationStayRedacted(t *testing.T) {
	for _, response := range []string{
		`{"error":"invalid_client","error_description":"private-secret"}`,
		`{"access_token":"private-token","token_type":"MAC"}`,
		`{"access_token":"private-token\u0001injected","token_type":"Bearer"}`,
		`{"token_type":"Bearer"}`,
	} {
		t.Run(response, func(t *testing.T) {
			// Given an issuer response containing secrets or an invalid bearer credential.
			issuer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprint(w, response)
			}))
			defer issuer.Close()
			provider := &oauthCredentials{client: issuer.Client()}
			provider.config.ClientID, provider.config.ClientSecret, provider.config.TokenURL = "private-client", "private-secret", issuer.URL
			mechanism := safeMechanism{Mechanism: oauth.Oauth(provider.authenticate)}
			// When authenticating, then report only the mechanism and preserve cancellation matching.
			_, first, err := mechanism.Authenticate(t.Context(), "host:9093")
			require.EqualError(t, err, "Kafka OAUTHBEARER authentication failed")
			require.Nil(t, first)
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			_, _, err = mechanism.Authenticate(ctx, "host:9093")
			require.ErrorIs(t, err, context.Canceled)
		})
	}
}

func TestOAuthTokenFileReloadAndChallengeRedaction(t *testing.T) {
	// Given a mounted bearer token file that an external credential agent can replace.
	path := writeFixture(t, t.TempDir(), "private-token", []byte("first-token\n"))
	mechanism := configuredMechanism(t, map[string]string{"sasl.mechanism": "OAUTHBEARER", "sasl.oauthbearer.token.file": path})
	for _, token := range []string{"first-token", "replacement-token"} {
		require.NoError(t, os.WriteFile(path, []byte(token+"\n"), 0600))
		// When a new broker session authenticates, then read the current file instead of a cached startup token.
		session, first, err := mechanism.Authenticate(t.Context(), "host:9093")
		require.NoError(t, err)
		require.Contains(t, string(first), "auth=Bearer "+token+"\x01")
		_, _, err = session.Challenge([]byte("private-token-echo"))
		require.EqualError(t, err, "Kafka OAUTHBEARER authentication failed")
	}
	// When rotation leaves an empty, malformed or missing file, then fail without exposing paths or tokens.
	for _, contents := range []string{"", "private-token\x01injected", "private token"} {
		require.NoError(t, os.WriteFile(path, []byte(contents), 0600))
		_, _, err := mechanism.Authenticate(t.Context(), "host:9093")
		require.EqualError(t, err, "Kafka OAUTHBEARER authentication failed")
	}
	require.NoError(t, os.Remove(path))
	_, _, err := mechanism.Authenticate(t.Context(), "host:9093")
	require.EqualError(t, err, "Kafka OAUTHBEARER authentication failed")
}

func TestAWSIAMUsesDefaultCredentialsAndSignsBrokerSessions(t *testing.T) {
	// Given isolated AWS environment credentials, without an AWS account or network calls.
	t.Setenv("AWS_ACCESS_KEY_ID", "fixture-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "private-secret-key")
	t.Setenv("AWS_SESSION_TOKEN", "private-session-token")
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(t.TempDir(), "absent"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "absent"))
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	mechanism := configuredMechanism(t, map[string]string{"sasl.mechanism": "AWS_MSK_IAM"})
	// When authenticating to an MSK broker, then sign with the selected temporary credentials and broker region.
	session, first, err := mechanism.Authenticate(t.Context(), "b-1.fixture.kafka.us-east-1.amazonaws.com:9098")
	require.NoError(t, err)
	var signed map[string]string
	require.NoError(t, json.Unmarshal(first, &signed))
	require.Equal(t, "b-1.fixture.kafka.us-east-1.amazonaws.com", signed["host"])
	require.Contains(t, signed["x-amz-credential"], "fixture-access-key/")
	require.Contains(t, signed["x-amz-credential"], "/us-east-1/kafka-cluster/aws4_request")
	require.Equal(t, "private-session-token", signed["x-amz-security-token"])
	require.NotEmpty(t, signed["x-amz-signature"])
	require.NotContains(t, string(first), "private-secret-key")
	done, _, err := session.Challenge([]byte(`{"version":"2020_10_22"}`))
	require.NoError(t, err)
	require.True(t, done)
	_, _, err = mechanism.Authenticate(t.Context(), "b-2.fixture.kafka.us-east-1.amazonaws.com:9098")
	require.NoError(t, err, "reuse the discovered SDK credential provider for another broker")
}

func TestAWSIAMRetrievesRotatingCredentialsAndRedactsProviderErrors(t *testing.T) {
	// Given a credential provider that rotates keys between broker sessions.
	call := 0
	mechanism := awsMechanism(aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
		call++
		if call == 3 {
			return aws.Credentials{}, fmt.Errorf("private-credential: %w", context.DeadlineExceeded)
		}
		if call > 3 {
			return aws.Credentials{}, errors.New("private-credential")
		}
		return aws.Credentials{AccessKeyID: fmt.Sprintf("key-%d", call), SecretAccessKey: "private-key"}, nil
	}))
	// When brokers authenticate independently, then each signature uses current credentials.
	for index := 1; index <= 2; index++ {
		_, first, err := mechanism.Authenticate(t.Context(), "b-1.fixture.kafka.us-east-1.amazonaws.com:9098")
		require.NoError(t, err)
		require.Contains(t, string(first), fmt.Sprintf("key-%d/", index))
	}
	_, _, err := mechanism.Authenticate(t.Context(), "broker:9098")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NotContains(t, err.Error(), "private")
	_, _, err = mechanism.Authenticate(t.Context(), "broker:9098")
	require.EqualError(t, err, "Kafka AWS_MSK_IAM authentication failed")
}

func TestAWSIAMConfigurationErrorsStayRedacted(t *testing.T) {
	// Given an invalid shared AWS configuration containing a credential-like value.
	path := writeFixture(t, t.TempDir(), "private-config", []byte("[private-invalid\n"))
	t.Setenv("AWS_CONFIG_FILE", path)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "absent"))
	mechanism := configuredMechanism(t, map[string]string{"sasl.mechanism": "AWS_MSK_IAM"})
	// When the first broker authenticates, then SDK configuration errors do not expose file paths or contents.
	_, _, err := mechanism.Authenticate(t.Context(), "b-1.fixture.kafka.us-east-1.amazonaws.com:9098")
	require.EqualError(t, err, "Kafka AWS_MSK_IAM authentication failed")
}

func TestExternalAuthenticationRejectsInconsistentConfiguration(t *testing.T) {
	for name, values := range map[string]map[string]string{
		"OAuth settings with PLAIN":    {"sasl.oauthbearer.client.id": "private-client"},
		"Kerberos settings with PLAIN": {"sasl.kerberos.principal": "private-user@REALM"},
		"username with IAM":            {"sasl.mechanism": "AWS_MSK_IAM", "sasl.username": "private-user"},
		"OAuth HTTP endpoint":          {"sasl.mechanism": "OAUTHBEARER", "sasl.oauthbearer.token.endpoint.url": "http://private-issuer/token"},
		"OAuth URL credentials":        {"sasl.mechanism": "OAUTHBEARER", "sasl.oauthbearer.token.endpoint.url": "https://private-user:private-secret@issuer/token"},
		"OAuth missing credentials":    {"sasl.mechanism": "OAUTHBEARER", "sasl.oauthbearer.token.endpoint.url": "https://issuer/token"},
		"OAuth mixed providers":        {"sasl.mechanism": "OAUTHBEARER", "sasl.oauthbearer.token.file": "private-token", "sasl.oauthbearer.client.id": "private-client"},
		"Kerberos missing realm":       {"sasl.mechanism": "GSSAPI", "sasl.kerberos.principal": "private-user"},
		"Kerberos missing keytab":      {"sasl.mechanism": "GSSAPI", "sasl.kerberos.principal": "private-user@REALM"},
	} {
		t.Run(name, func(t *testing.T) {
			// Given incompatible or incomplete authentication properties containing private values.
			values["security.protocol"] = "SASL_SSL"
			// When validating, then reject before opening output and never print the values.
			err := componentFactory().Validate(testPlan(t, values), config.Defaults("").Worker)
			require.Error(t, err)
			require.NotContains(t, err.Error(), "private")
		})
	}
}

func TestOAuthTokenRequestHonorsCancellation(t *testing.T) {
	// Given an issuer that has accepted a request but has not returned a token.
	entered := make(chan struct{})
	release := make(chan struct{})
	issuer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		close(entered)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer issuer.Close()
	defer close(release)
	provider := &oauthCredentials{client: issuer.Client()}
	provider.config.ClientID, provider.config.ClientSecret, provider.config.TokenURL = "private-client", "private-secret", issuer.URL
	mechanism := safeMechanism{Mechanism: oauth.Oauth(provider.authenticate)}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, _, err := mechanism.Authenticate(ctx, "broker:9093")
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("token request did not reach issuer")
	}
	// When the request is cancelled, then authentication exits without waiting for an HTTP timeout.
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
		require.NotContains(t, err.Error(), "private")
	case <-time.After(3 * time.Second):
		t.Fatal("authentication ignored cancellation")
	}
}

func TestAuthenticationFilesAreCheckedBeforeOpeningOutput(t *testing.T) {
	directory := t.TempDir()
	invalid := writeFixture(t, directory, "private-file", []byte("private-file-contents"))
	invalidConfig := writeFixture(t, directory, "private-krb5.conf", []byte("[libdefaults]\n udp_preference_limit = private-number\n"))
	noKDC := writeFixture(t, directory, "no-kdc.conf", []byte("[libdefaults]\n dns_lookup_kdc = false\n[realms]\n KAFERRY.TEST = {\n }\n"))
	krbConfig := writeFixture(t, directory, "krb5.conf", []byte("[libdefaults]\n default_realm = KAFERRY.TEST\n dns_lookup_kdc = false\n[realms]\n KAFERRY.TEST = {\n  kdc = 127.0.0.1:1\n }\n"))
	keys := keytab.New()
	require.NoError(t, keys.AddEntry("copier", "KAFERRY.TEST", "private-password", time.Now(), 1, 18))
	keyBytes, err := keys.Marshal()
	require.NoError(t, err)
	keyPath := writeFixture(t, directory, "client.keytab", keyBytes)
	for _, tc := range []struct {
		name, mechanism, property, path string
	}{
		{"missing token", "OAUTHBEARER", "sasl.oauthbearer.token.file", filepath.Join(directory, "private-missing")},
		{"empty token", "OAUTHBEARER", "sasl.oauthbearer.token.file", writeFixture(t, directory, "empty-token", nil)},
		{"missing issuer CA", "OAUTHBEARER", "sasl.oauthbearer.ssl.ca.location", filepath.Join(directory, "private-missing-ca")},
		{"invalid issuer CA", "OAUTHBEARER", "sasl.oauthbearer.ssl.ca.location", invalid},
		{"invalid Kerberos config", "GSSAPI", "sasl.kerberos.config", invalidConfig},
		{"missing realm KDC", "GSSAPI", "sasl.kerberos.config", noKDC},
		{"invalid keytab", "GSSAPI", "sasl.kerberos.keytab", invalid},
		{"missing keytab", "GSSAPI", "sasl.kerberos.keytab", filepath.Join(directory, "private-missing-keytab")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given malformed or unreadable local authentication files with private paths and contents.
			values := map[string]string{"security.protocol": "SASL_SSL", "sasl.mechanism": tc.mechanism}
			if tc.mechanism == "GSSAPI" {
				values["sasl.kerberos.config"], values["sasl.kerberos.keytab"], values["sasl.kerberos.principal"] = krbConfig, keyPath, "copier@KAFERRY.TEST"
			} else if tc.property != "sasl.oauthbearer.token.file" {
				values["sasl.oauthbearer.token.endpoint.url"], values["sasl.oauthbearer.client.id"], values["sasl.oauthbearer.client.secret"] = "https://issuer/token", "private-client", "private-secret"
			}
			values[tc.property] = tc.path
			// When validating before resource acquisition, then identify the setting without revealing its value.
			err := componentFactory().Validate(testPlan(t, values), config.Defaults("").Worker)
			require.ErrorContains(t, err, tc.property)
			require.NotContains(t, err.Error(), "private")
			require.NotContains(t, err.Error(), directory)
		})
	}
	// Given usable local Kerberos files, when a KDC is unavailable, then redact the principal and network details.
	mechanism := configuredMechanism(t, map[string]string{"sasl.mechanism": "GSSAPI", "sasl.kerberos.principal": "copier@KAFERRY.TEST", "sasl.kerberos.keytab": keyPath, "sasl.kerberos.config": krbConfig})
	_, _, err = mechanism.Authenticate(t.Context(), "broker:9093")
	require.EqualError(t, err, "Kafka GSSAPI authentication failed")
}
