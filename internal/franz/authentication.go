package franz

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/twmb/franz-go/pkg/sasl"
)

type oauthSettings struct{ endpoint, clientID, secret, scope, ca, tokenFile string }
type kerberosSettings struct{ principal, keytab, config, service string }

func (s settings) validateAuthentication(keys []string) error {
	for _, key := range keys {
		switch {
		case strings.HasPrefix(key, "sasl.oauthbearer.") && s.mechanism != "OAUTHBEARER":
			return invalidProperty(key, "requires sasl.mechanism=OAUTHBEARER")
		case strings.HasPrefix(key, "sasl.kerberos.") && s.mechanism != "GSSAPI":
			return invalidProperty(key, "requires sasl.mechanism=GSSAPI")
		case (key == "sasl.username" || key == "sasl.password") && s.mechanism != "PLAIN" && !strings.HasPrefix(s.mechanism, "SCRAM-"):
			return invalidProperty(key, "requires PLAIN or SCRAM authentication")
		}
	}
	switch s.mechanism {
	case "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
		if s.username == "" || s.password == "" {
			return invalidProperty("sasl.username/sasl.password", "both are required for PLAIN or SCRAM")
		}
	case "OAUTHBEARER":
		if s.oauth.tokenFile != "" {
			if s.oauth.endpoint != "" || s.oauth.clientID != "" || s.oauth.secret != "" || s.oauth.scope != "" || s.oauth.ca != "" {
				return invalidProperty("sasl.oauthbearer.token.file", "cannot be combined with OAuth client credentials settings")
			}
			return nil
		}
		endpoint, err := url.Parse(s.oauth.endpoint)
		if err != nil || endpoint.Scheme != "https" || endpoint.Hostname() == "" || endpoint.User != nil || endpoint.Fragment != "" {
			return invalidProperty("sasl.oauthbearer.token.endpoint.url", "requires an HTTPS URL without embedded credentials or a fragment")
		}
		if s.oauth.clientID == "" || s.oauth.secret == "" {
			return invalidProperty("sasl.oauthbearer.client.id/sasl.oauthbearer.client.secret", "both are required for OAuth client credentials")
		}
	case "GSSAPI":
		user, realm, ok := strings.Cut(s.kerberos.principal, "@")
		if !ok || user == "" || realm == "" || strings.Contains(realm, "@") {
			return invalidProperty("sasl.kerberos.principal", "requires a principal with a realm, such as user@REALM")
		}
		if s.kerberos.keytab == "" || s.kerberos.config == "" || s.kerberos.service == "" {
			return invalidProperty("sasl.kerberos.keytab/config/service.name", "keytab, configuration file and service name must not be empty")
		}
	}
	return nil
}

// Credential providers and SASL peers can include secrets in their errors.
// Keep only the mechanism name and standard cancellation causes in diagnostics.
type safeMechanism struct {
	sasl.Mechanism
	cleanup func()
	mu      sync.Mutex
	closed  bool
}

func (m *safeMechanism) Authenticate(ctx context.Context, host string) (sasl.Session, []byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	// A Kafka client can close while authentication is still running.
	// Close takes this lock before destroying credentials used by that call.
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, nil, authenticationError(m.Name(), context.Canceled)
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	session, first, err := m.Mechanism.Authenticate(ctx, host)
	if err != nil {
		return nil, nil, authenticationError(m.Name(), err)
	}
	return safeSession{session, m.Name()}, first, nil
}

func (m *safeMechanism) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	m.closed = true
	if closer, ok := m.Mechanism.(sasl.ClosingMechanism); ok {
		closer.Close()
	}
	if m.cleanup != nil {
		m.cleanup()
	}
}

type safeSession struct {
	sasl.Session
	mechanism string
}

func (s safeSession) Challenge(response []byte) (bool, []byte, error) {
	done, next, err := s.Session.Challenge(response)
	if err != nil {
		return false, nil, authenticationError(s.mechanism, err)
	}
	return done, next, nil
}

func authenticationError(mechanism string, cause error) error {
	for _, err := range []error{context.Canceled, context.DeadlineExceeded} {
		if errors.Is(cause, err) {
			return fmt.Errorf("kafka %s authentication failed: %w", mechanism, err)
		}
	}
	return fmt.Errorf("kafka %s authentication failed", mechanism)
}
